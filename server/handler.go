package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mdzio/go-logging"
	"github.com/mdzio/go-veap"
	"github.com/mdzio/go-veap/encoding"
)

const (
	// default max. size of a valid request: 1 MB
	defaultRequestSizeLimit = 1 * 1024 * 1024

	// default max. number of entries in a history
	defaultHistorySizeLimit = 10000

	// query parameters
	writePVQueryParam = "writepv"
	formatQueryParam  = "format"
	formatSimple      = "simple"

	// content types
	contentTypeJSON = "application/json"
	contentTypeText = "text/plain; charset=utf-8"
)

var handlerLog = logging.Get("veap-handler")

// HandlerStats collects statistics about the requests and responses. To access
// the counters atomic.LoadInt64 must be used.
type HandlerStats struct {
	Requests       uint64
	RequestBytes   uint64
	ResponseBytes  uint64
	ErrorResponses uint64
}

// Handler transforms HTTP requests to VEAP service requests.
type Handler struct {
	// Service is VEAP service provider for processing the requests.
	veap.Service

	// URLPrefix must be set, if the VEAP tree starts not at root.
	URLPrefix string

	// RequestSizeLimit is the maximum size of a valid request. If not set, the
	// limit is 1 MB.
	RequestSizeLimit int64

	// HistorySizeLimit is the maximum number of entries in a history. If not
	// set, the limit is 10000 entries.
	HistorySizeLimit int64

	// Statistics collects statistics about the requests and responses.
	Stats HandlerStats
}

func (h *Handler) ServeHTTP(respWriter http.ResponseWriter, request *http.Request) {
	handlerLog.Debugf("Request from %s, method %s, URL %v", request.RemoteAddr, request.Method, request.URL)
	// update statistics
	atomic.AddUint64(&h.Stats.Requests, 1)

	// remove prefix
	fullPath := request.URL.EscapedPath()
	if !strings.HasPrefix(fullPath, h.URLPrefix) {
		h.errorResponse(respWriter, request, veap.StatusNotFound, "URL prefix does not match: %s", request.URL.Path)
		return
	}
	fullPath = strings.TrimPrefix(fullPath, h.URLPrefix)

	// receive request
	reqLimitReader := http.MaxBytesReader(respWriter, request.Body, h.requestSizeLimit())
	reqBytes, err := ioutil.ReadAll(reqLimitReader)
	if err != nil {
		h.errorResponse(respWriter, request, veap.StatusBadRequest, "Receiving of request failed: %v", err)
		return
	}

	// update statistics
	atomic.AddUint64(&h.Stats.RequestBytes, uint64(len(reqBytes)))

	// log request
	if handlerLog.TraceEnabled() && len(reqBytes) > 0 {
		handlerLog.Tracef("Request body: %s", string(reqBytes))
	}

	// dispatch VEAP service
	respCode := http.StatusOK
	var respBytes []byte
	var contentType = contentTypeJSON
	base := path.Base(fullPath)
	switch base {

	case veap.PVMarker:
		switch request.Method {
		case http.MethodGet:
			qvs := request.URL.Query()
			wpv := qvs.Get(writePVQueryParam)
			if wpv != "" {
				// VEAP protocol extension: HTTP-GET request for writing PV with
				// query parameter 'writepv'
				err = h.serveSetPV(path.Dir(fullPath), []byte(wpv), true /* fuzzy parsing */)
			} else {
				// VEAP protocol extension: returning PV in specific format with
				// query parameter 'format', contentType may be changed
				respBytes, contentType, err = h.servePV(path.Dir(fullPath), qvs.Get(formatQueryParam))
			}
		case http.MethodPut:
			err = h.serveSetPV(path.Dir(fullPath), reqBytes, false /* no fuzzy parsing */)
		default:
			h.errorResponse(respWriter, request, veap.StatusMethodNotAllowed,
				"Method %s not allowed for PV %s", request.Method, fullPath)
			return
		}

	case veap.HistMarker:
		switch request.Method {
		case http.MethodGet:
			respBytes, err = h.serveHistory(path.Dir(fullPath), request.URL.Query())
		case http.MethodPut:
			err = h.serveSetHistory(path.Dir(fullPath), reqBytes)
		default:
			h.errorResponse(respWriter, request, veap.StatusMethodNotAllowed,
				"Method %s not allowed for history %s", request.Method, fullPath)
			return
		}

	case veap.ExgDataMarker:
		if request.Method != http.MethodPut {
			h.errorResponse(respWriter, request, veap.StatusMethodNotAllowed,
				"Invalid method for ExgData service: %s", request.Method)
			return
		}
		if fullPath != "/"+veap.ExgDataMarker {
			h.errorResponse(respWriter, request, veap.StatusNotFound,
				"Invalid path for ExgData service: %s", fullPath)
			return
		}
		respBytes, err = h.serveExgData(reqBytes)

	case veap.QueryMarker:
		if request.Method != http.MethodGet {
			h.errorResponse(respWriter, request, veap.StatusMethodNotAllowed,
				"Invalid method for Query service: %s", request.Method)
			return
		}
		if fullPath != "/"+veap.QueryMarker {
			h.errorResponse(respWriter, request, veap.StatusNotFound,
				"Invalid path for Query service: %s", fullPath)
			return
		}
		respBytes, err = h.serveQuery(request.URL.Query())

	default:
		switch request.Method {
		case http.MethodGet:
			respBytes, err = h.serveProperties(fullPath)
		case http.MethodPut:
			var created bool
			created, err = h.serveSetProperties(fullPath, reqBytes)
			if created {
				respCode = http.StatusCreated
			}
		case http.MethodDelete:
			err = h.serveDelete(fullPath)
		default:
			h.errorResponse(respWriter, request, veap.StatusMethodNotAllowed,
				"Method %s not allowed for %s", request.Method, fullPath)
			return
		}
	}

	// send error response
	if err != nil {
		if svcErr, ok := err.(veap.Error); ok {
			respCode = svcErr.Code()
		} else {
			respCode = http.StatusInternalServerError
		}
		h.errorResponse(respWriter, request, respCode, "%v", err)
		return
	}

	// update statistics
	atomic.AddUint64(&h.Stats.ResponseBytes, uint64(len(respBytes)))

	// send OK response
	if handlerLog.TraceEnabled() {
		if len(respBytes) > 0 {
			handlerLog.Tracef("Response body: %s", string(respBytes))
		}
		handlerLog.Tracef("Response code: %d", respCode)
	}
	respWriter.Header().Set("Content-Type", contentType)
	respWriter.Header().Set("X-Content-Type-Options", "nosniff")
	respWriter.Header().Set("Content-Length", strconv.Itoa(len(respBytes)))
	respWriter.WriteHeader(respCode)
	if _, err = respWriter.Write(respBytes); err != nil {
		handlerLog.Warningf("Sending response to %s failed: %v", request.RemoteAddr, err)
		return
	}
}

type wireServiceError struct {
	Message string `json:"message"`
}

func (h *Handler) errorResponse(respWriter http.ResponseWriter, request *http.Request, code int, format string, args ...interface{}) {
	// create error object
	w := wireServiceError{Message: fmt.Sprintf(format, args...)}

	// log error
	handlerLog.Warningf("Request from %s: %s; code %d", request.RemoteAddr, w.Message, code)

	// marshal error as JSON
	b, err := json.Marshal(w)
	if err != nil {
		handlerLog.Warningf("Conversion of error to JSON failed: %v", err)
		return
	}

	// send error
	respWriter.Header().Set("Content-Type", "application/json")
	respWriter.Header().Set("X-Content-Type-Options", "nosniff")
	respWriter.Header().Set("Content-Length", strconv.Itoa(len(b)))
	respWriter.WriteHeader(code)
	if _, err = respWriter.Write(b); err != nil {
		handlerLog.Warningf("Sending error response to %s failed: %v", request.RemoteAddr, err)
		return
	}

	// update statistics
	atomic.AddUint64(&h.Stats.ErrorResponses, 1)
	atomic.AddUint64(&h.Stats.ResponseBytes, uint64(len(b)))
}

func (h *Handler) servePV(path string, format string) ([]byte, string, error) {
	// invoke service
	pv, svcErr := h.Service.ReadPV(path)
	if svcErr != nil {
		return nil, "", svcErr
	}

	// format PV
	if format == formatSimple {
		return []byte(fmt.Sprint(pv.Value)), contentTypeText, nil
	}

	// default format: convert PV to JSON
	b, err := json.Marshal(encoding.PVToWire(pv))
	if err != nil {
		return nil, "", fmt.Errorf("Conversion of PV to JSON failed: %v", err)
	}
	return b, contentTypeJSON, nil
}

func (h *Handler) serveSetPV(path string, b []byte, fuzzy bool) error {
	// convert JSON to PV
	pv, err := encoding.BytesToPV(b, fuzzy)
	if err != nil {
		return veap.NewErrorf(veap.StatusBadRequest, "Conversion of JSON to PV failed: %v", err)
	}

	// invoke service
	return h.Service.WritePV(path, pv)
}

func (h *Handler) serveHistory(path string, params url.Values) ([]byte, error) {
	// parse params
	begin, err := parseTimeParam(params, "begin")
	if err != nil {
		return nil, err
	}
	end, err := parseTimeParam(params, "end")
	if err != nil {
		return nil, err
	}
	switch {
	case begin != nil && end != nil:
		// both parameters found
	case begin == nil && end == nil:
		// no parameters found
		e := time.Now()
		end = &e
		b := e.Add(-24 * time.Hour)
		begin = &b
	default:
		// one parameter is missing
		var p string
		if begin != nil {
			p = "end"
		} else {
			p = "begin"
		}
		return nil, veap.NewErrorf(veap.StatusBadRequest, "Missing request parameter: %s", p)
	}
	limit, err := parseIntParam(params, "limit")
	if err != nil {
		return nil, err
	}
	maxLimit := h.historySizeLimit()
	if limit != nil {
		if *limit > maxLimit {
			handlerLog.Warningf("History size limit exceeded: %d", *limit)
			limit = &maxLimit
		}
	} else {
		// no limit provided
		limit = &maxLimit
	}

	// invoke service
	hist, err := h.Service.ReadHistory(path, *begin, *end, *limit)
	if err != nil {
		return nil, err
	}

	// convert history to JSON
	b, err := json.Marshal(encoding.HistToWire(hist))
	if err != nil {
		return nil, fmt.Errorf("Conversion of history to JSON failed: %v", err)
	}
	return b, nil
}

func (h *Handler) serveSetHistory(path string, reqBytes []byte) error {
	// convert JSON to history
	var w encoding.WireHist
	err := json.Unmarshal(reqBytes, &w)
	if err != nil {
		return veap.NewErrorf(veap.StatusBadRequest, "Conversion of JSON to history failed: %v", err)
	}

	// invoke service
	hist, err := encoding.WireToHist(w)
	if err != nil {
		return err
	}
	return h.Service.WriteHistory(path, hist)
}

func (h *Handler) serveProperties(objPath string) ([]byte, error) {
	// invoke service
	attr, links, svrErr := h.Service.ReadProperties(objPath)
	if svrErr != nil {
		return nil, svrErr
	}

	// copy attributes
	wireAttr := make(map[string]interface{})
	for k, v := range attr {
		wireAttr[k] = v
	}

	// add ~links property
	if len(links) > 0 {
		wireLinks := make([]encoding.WireLink, len(links))
		for i, l := range links {
			// modify absolute paths
			p := l.Target
			if path.IsAbs(p) {
				p = h.URLPrefix + p
			}
			wireLinks[i] = encoding.WireLink{
				Role:   l.Role,
				Target: p,
				Title:  l.Title,
			}
		}
		wireAttr[veap.LinksMarker] = wireLinks
	}

	// convert properties to JSON
	b, err := json.Marshal(wireAttr)
	if err != nil {
		return nil, fmt.Errorf("Conversion of properties to JSON failed: %v", err)
	}
	return b, nil
}

func (h *Handler) serveSetProperties(path string, reqBytes []byte) (bool, error) {
	// convert JSON to attributes
	var attr map[string]interface{}
	err := json.Unmarshal(reqBytes, &attr)
	if err != nil {
		return false, veap.NewErrorf(veap.StatusBadRequest, "Conversion of JSON to attributes failed: %v", err)
	}

	// invoke service
	return h.Service.WriteProperties(path, attr)
}

func (h *Handler) serveDelete(path string) error {
	// invoke service
	return h.Service.Delete(path)
}

func (h *Handler) serveExgData(reqBytes []byte) (respBytes []byte, serviceErr error) {
	// service provided?
	ms, ok := h.Service.(veap.MetaService)
	if !ok {
		serviceErr = veap.NewErrorf(veap.StatusBadRequest, "ExgData service not implemented")
		return
	}

	// decode params
	var wireParams encoding.WireExgDataParams
	err := json.Unmarshal(reqBytes, &wireParams)
	if err != nil {
		serviceErr = veap.NewErrorf(veap.StatusBadRequest, "Invalid JSON for ExgData parameters: %v", err)
		return
	}
	writePVs, readPaths := encoding.WireToExgDataParams(&wireParams)

	// call service
	writeErrors, readResults, serviceErr := ms.ExgData(writePVs, readPaths)
	if serviceErr != nil {
		return
	}

	// encode results
	wireResult := encoding.ExgDataResultsToWire(writeErrors, readResults)
	respBytes, err = json.Marshal(wireResult)
	if err != nil {
		serviceErr = veap.NewErrorf(veap.StatusInternalServerError, "Conversion of ExgData results to JSON failed: %v", err)
		return
	}
	return
}

// The ~path URL parameter specifies a path mask (e.g. ~path=/device/*/*). This
// parameter must be specified at least once.
func (h *Handler) serveQuery(parameters url.Values) (respBytes []byte, serviceErr error) {
	// service provided?
	ms, ok := h.Service.(veap.MetaService)
	if !ok {
		serviceErr = veap.NewErrorf(veap.StatusBadRequest, "Query service not implemented")
		return
	}

	// extract paths and remove URL prefix
	paths := make([]string, 0)
	for _, fullPath := range parameters[veap.PathMarker] {
		if !strings.HasPrefix(fullPath, h.URLPrefix) {
			return nil, veap.NewErrorf(veap.StatusNotFound, "Path prefix does not match: %s", fullPath)
		}
		paths = append(paths, strings.TrimPrefix(fullPath, h.URLPrefix))
	}

	// call service
	queryResults, serviceErr := ms.Query(paths)
	if serviceErr != nil {
		return
	}

	// encode results
	wireResult := make([]map[string]interface{}, len(queryResults))
	for idx := range queryResults {

		// copy attributes
		wireAttr := make(map[string]interface{})
		for k, v := range queryResults[idx].Attributes {
			wireAttr[k] = v
		}

		// add ~links property
		if len(queryResults[idx].Links) > 0 {
			wireLinks := make([]encoding.WireLink, len(queryResults[idx].Links))
			for i, l := range queryResults[idx].Links {
				// modify absolute paths
				p := l.Target
				if path.IsAbs(p) {
					p = h.URLPrefix + p
				}
				wireLinks[i] = encoding.WireLink{
					Role:   l.Role,
					Target: p,
					Title:  l.Title,
				}
			}
			wireAttr[veap.LinksMarker] = wireLinks
		}

		// add ~path property
		p := queryResults[idx].Path
		// modify absolute paths
		if path.IsAbs(p) {
			p = h.URLPrefix + p
		}
		wireAttr[veap.PathMarker] = p

		// add to result
		wireResult[idx] = wireAttr
	}
	respBytes, err := json.Marshal(wireResult)
	if err != nil {
		serviceErr = veap.NewErrorf(veap.StatusInternalServerError, "Conversion of Query results to JSON failed: %v", err)
		return
	}
	return
}

func (h *Handler) requestSizeLimit() int64 {
	if h.RequestSizeLimit == 0 {
		return defaultRequestSizeLimit
	}
	return h.RequestSizeLimit
}

func (h *Handler) historySizeLimit() int64 {
	if h.HistorySizeLimit == 0 {
		return defaultHistorySizeLimit
	}
	return h.HistorySizeLimit
}

func parseIntParam(params url.Values, name string) (*int64, error) {
	values, ok := params[name]
	if !ok {
		return nil, nil
	}
	if len(values) != 1 {
		return nil, veap.NewErrorf(veap.StatusBadRequest, "Invalid request parameter: %s", name)
	}
	txt := values[0]
	i, err := strconv.ParseInt(txt, 10, 64)
	if err != nil {
		return nil, veap.NewErrorf(veap.StatusBadRequest, "Invalid request parameter %s: %v", name, err)
	}
	return &i, nil
}

func parseTimeParam(params url.Values, name string) (*time.Time, error) {
	i, err := parseIntParam(params, name)
	if err != nil {
		return nil, err
	}
	if i == nil {
		return nil, nil
	}
	t := time.Unix(0, (*i)*1000000)
	return &t, nil
}
