package veap

import (
	"bytes"
	"encoding/json"
	"errors"
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
)

const (
	// service markers
	pvMarker      = "~pv"
	histMarker    = "~hist"
	exgDataMarker = "~exgdata"

	// property markers
	linksMarker = "~links"

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
	Service

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
		h.errorResponse(respWriter, request, StatusNotFound, "URL prefix does not match: %s", request.URL.Path)
		return
	}
	fullPath = strings.TrimPrefix(fullPath, h.URLPrefix)

	// receive request
	reqLimitReader := http.MaxBytesReader(respWriter, request.Body, h.requestSizeLimit())
	reqBytes, err := ioutil.ReadAll(reqLimitReader)
	if err != nil {
		h.errorResponse(respWriter, request, StatusBadRequest, "Receiving of request failed: %v", err)
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

	case pvMarker:
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
			h.errorResponse(respWriter, request, StatusMethodNotAllowed,
				"Method %s not allowed for PV %s", request.Method, fullPath)
			return
		}

	case histMarker:
		switch request.Method {
		case http.MethodGet:
			respBytes, err = h.serveHistory(path.Dir(fullPath), request.URL.Query())
		case http.MethodPut:
			err = h.serveSetHistory(path.Dir(fullPath), reqBytes)
		default:
			h.errorResponse(respWriter, request, StatusMethodNotAllowed,
				"Method %s not allowed for history %s", request.Method, fullPath)
			return
		}

	case exgDataMarker:
		if request.Method != http.MethodPut {
			h.errorResponse(respWriter, request, StatusMethodNotAllowed,
				"Invalid method for ExgData service: %s", request.Method)
			return
		}
		if fullPath != "/"+exgDataMarker {
			h.errorResponse(respWriter, request, StatusNotFound,
				"Invalid path for ExgData service: %s", fullPath)
			return
		}
		respBytes, err = h.serveExgData(reqBytes)

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
			h.errorResponse(respWriter, request, StatusMethodNotAllowed,
				"Method %s not allowed for %s", request.Method, fullPath)
			return
		}
	}

	// send error response
	if err != nil {
		if svcErr, ok := err.(Error); ok {
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
	b, err := json.Marshal(pvToWire(pv))
	if err != nil {
		return nil, "", fmt.Errorf("Conversion of PV to JSON failed: %v", err)
	}
	return b, contentTypeJSON, nil
}

func (h *Handler) serveSetPV(path string, b []byte, fuzzy bool) error {
	// convert JSON to PV
	pv, err := bytesToPV(b, fuzzy)
	if err != nil {
		return NewErrorf(StatusBadRequest, "Conversion of JSON to PV failed: %v", err)
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
		return nil, NewErrorf(StatusBadRequest, "Missing request parameter: %s", p)
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
	b, err := json.Marshal(histToWire(hist))
	if err != nil {
		return nil, fmt.Errorf("Conversion of history to JSON failed: %v", err)
	}
	return b, nil
}

func (h *Handler) serveSetHistory(path string, reqBytes []byte) error {
	// convert JSON to history
	var w wireHist
	err := json.Unmarshal(reqBytes, &w)
	if err != nil {
		return NewErrorf(StatusBadRequest, "Conversion of JSON to history failed: %v", err)
	}

	// invoke service
	hist, err := wireToHist(w)
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
		wireLinks := make([]wireLink, len(links))
		for i, l := range links {
			// modify absolute paths
			p := l.Target
			if path.IsAbs(p) {
				p = h.URLPrefix + p
			}
			wireLinks[i] = wireLink{
				l.Role,
				p,
				l.Title,
			}
		}
		wireAttr[linksMarker] = wireLinks
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
		return false, NewErrorf(StatusBadRequest, "Conversion of JSON to attributes failed: %v", err)
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
	ms, ok := h.Service.(MetaService)
	if !ok {
		serviceErr = NewErrorf(StatusBadRequest, "ExgData service not implemented")
		return
	}

	// decode params
	var wireParams wireExgDataParams
	err := json.Unmarshal(reqBytes, &wireParams)
	if err != nil {
		serviceErr = NewErrorf(StatusBadRequest, "Invalid JSON for ExgData parameters: %v", err)
		return
	}
	writePVs, readPaths := wireToExgDataParams(&wireParams)

	// call service
	writeErrors, readResults, serviceErr := ms.ExgData(writePVs, readPaths)
	if serviceErr != nil {
		return
	}

	// encode results
	wireResult := exgDataResultsToWire(writeErrors, readResults)
	respBytes, err = json.Marshal(wireResult)
	if err != nil {
		serviceErr = NewErrorf(StatusInternalServerError, "Conversion of ExgData results to JSON failed: %v", err)
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
		return nil, NewErrorf(StatusBadRequest, "Invalid request parameter: %s", name)
	}
	txt := values[0]
	i, err := strconv.ParseInt(txt, 10, 64)
	if err != nil {
		return nil, NewErrorf(StatusBadRequest, "Invalid request parameter %s: %v", name, err)
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

type wirePV struct {
	Time  int64       `json:"ts"`
	Value interface{} `json:"v"`
	State State       `json:"s"`
}

var errUnexpectetContent = errors.New("Unexpectet content")

func bytesToPV(payload []byte, fuzzy bool) (PV, error) {
	// try to convert JSON to wirePV
	var w wirePV
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	err := dec.Decode(&w)
	if err == nil {
		// check for unexpected content
		c, err2 := ioutil.ReadAll(dec.Buffered())
		if err2 != nil {
			return PV{}, fmt.Errorf("ReadAll failed: %w", err2)
		}
		// allow only white space
		cs := strings.TrimSpace(string(c))
		if len(cs) != 0 {
			err = errUnexpectetContent
		}
	}

	// parsing failed?
	if err != nil {
		// if not fuzzy mode, return error
		if !fuzzy {
			return PV{}, err
		}
		// take whole payload as JSON value
		var v interface{}
		err = json.Unmarshal(payload, &v)
		if err == nil {
			w = wirePV{Value: v}
		} else {
			// if no valid JSON content is found, use the whole payload as string
			w = wirePV{Value: string(payload)}
		}
	}

	// if no timestamp is provided, use current time
	var ts time.Time
	if w.Time == 0 {
		ts = time.Now()
	} else {
		ts = time.Unix(0, w.Time*1000000)
	}

	// if no state is provided, state is implicit GOOD
	return PV{
		Time:  ts,
		Value: w.Value,
		State: w.State,
	}, nil
}

func wireToPV(wirePV wirePV) PV {
	// if no timestamp is provided, use current time
	var ts time.Time
	if wirePV.Time == 0 {
		ts = time.Now()
	} else {
		ts = time.Unix(0, wirePV.Time*1000000)
	}

	// if no state is provided, state is implicit GOOD
	return PV{
		Time:  ts,
		Value: wirePV.Value,
		State: wirePV.State,
	}
}

func pvToWire(pv PV) wirePV {
	return wirePV{
		Time:  pv.Time.UnixNano() / 1000000,
		Value: pv.Value,
		State: pv.State,
	}
}

type wireHist struct {
	Times  []int64       `json:"ts"`
	Values []interface{} `json:"v"`
	States []State       `json:"s"`
}

func histToWire(hist []PV) wireHist {
	w := wireHist{}
	w.Times = make([]int64, len(hist))
	w.Values = make([]interface{}, len(hist))
	w.States = make([]State, len(hist))
	for i, e := range hist {
		w.Times[i] = e.Time.UnixNano() / 1000000
		w.Values[i] = e.Value
		w.States[i] = e.State
	}
	return w
}

func wireToHist(w wireHist) ([]PV, error) {
	l := len(w.Times)
	if len(w.Values) != l || len(w.States) != l {
		return nil, NewErrorf(StatusBadRequest, "History arrays must have same length")
	}
	hist := make([]PV, l)
	for i := 0; i < l; i++ {
		hist[i] = PV{
			time.Unix(0, w.Times[i]*1000000),
			w.Values[i],
			w.States[i],
		}
	}
	return hist, nil
}

type wireLink struct {
	Role   string `json:"rel"`
	Target string `json:"href"`
	Title  string `json:"title,omitempty"`
}

type wireError struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

func errorToWire(err Error) *wireError {
	if err == nil {
		return nil
	}
	return &wireError{err.Code(), err.Error()}
}

type wireWritePVParam struct {
	Path string `json:"path"`
	PV   wirePV `json:"pv"`
}

type wireExgDataParams struct {
	WritePVs  []wireWritePVParam `json:"writePVs"`
	ReadPaths []string           `json:"readPaths"`
}

type wireReadPVResult struct {
	PV    *wirePV    `json:"pv,omitempty"`
	Error *wireError `json:"error,omitempty"`
}

type wireExgDataResults struct {
	WriteErrors []*wireError       `json:"writeErrors"`
	ReadResults []wireReadPVResult `json:"readResults"`
}

func wireToExgDataParams(w *wireExgDataParams) (writePVs []WritePVParam, readPaths []string) {
	writePVs = make([]WritePVParam, len(w.WritePVs))
	for i := range w.WritePVs {
		writePVs[i].Path = w.WritePVs[i].Path
		writePVs[i].PV = wireToPV(w.WritePVs[i].PV)
	}
	readPaths = w.ReadPaths
	return
}

func exgDataResultsToWire(writeErrors []Error, readResults []ReadPVResult) *wireExgDataResults {
	w := &wireExgDataResults{}
	w.WriteErrors = make([]*wireError, len(writeErrors))
	for i := range writeErrors {
		w.WriteErrors[i] = errorToWire(writeErrors[i])
	}
	w.ReadResults = make([]wireReadPVResult, len(readResults))
	for i := range readResults {
		if err := readResults[i].Error; err != nil {
			w.ReadResults[i].Error = errorToWire(err)
		} else {
			wpv := pvToWire(readResults[i].PV)
			w.ReadResults[i].PV = &wpv
		}
	}
	return w
}
