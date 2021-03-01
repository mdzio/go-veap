package veap

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
)

const (
	// service markers
	pvMarker   = "~pv"
	histMarker = "~hist"

	// property markers
	linksMarker = "~links"

	// default max. size of a valid request: 1 MB
	defaultRequestSizeLimit = 1 * 1024 * 1024

	// default max. number of entries in a history
	defaultHistorySizeLimit = 10000
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
	base := path.Base(fullPath)
	switch base {
	case pvMarker:
		switch request.Method {
		case http.MethodGet:
			respBytes, err = h.servePV(path.Dir(fullPath))
		case http.MethodPut:
			err = h.serveSetPV(path.Dir(fullPath), reqBytes)
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
	respWriter.Header().Set("Content-Type", "application/json")
	respWriter.Header().Set("X-Content-Type-Options", "nosniff")
	respWriter.Header().Set("Content-Length", strconv.Itoa(len(respBytes)))
	respWriter.WriteHeader(respCode)
	if _, err = respWriter.Write(respBytes); err != nil {
		handlerLog.Warningf("Sending response to %s failed: %v", request.RemoteAddr, err)
		return
	}
}

type wireError struct {
	Message string `json:"message"`
}

func (h *Handler) errorResponse(respWriter http.ResponseWriter, request *http.Request, code int, format string, args ...interface{}) {
	// create error object
	w := wireError{Message: fmt.Sprintf(format, args...)}

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

func (h *Handler) servePV(path string) ([]byte, error) {
	// invoke service
	pv, svcErr := h.Service.ReadPV(path)
	if svcErr != nil {
		return nil, svcErr
	}

	// convert PV to JSON
	b, err := json.Marshal(pvToWire(pv))
	if err != nil {
		return nil, fmt.Errorf("Conversion of PV to JSON failed: %v", err)
	}
	return b, nil
}

func (h *Handler) serveSetPV(path string, b []byte) error {
	// convert JSON to PV
	var w wirePV
	err := json.Unmarshal(b, &w)
	if err != nil {
		return NewErrorf(StatusBadRequest, "Conversion of JSON to PV failed: %v", err)
	}

	// invoke service
	return h.Service.WritePV(path, wireToPV(w))
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

func wireToPV(w wirePV) PV {
	// if no timestamp is provided, use current time
	var ts time.Time
	if w.Time == 0 {
		ts = time.Now()
	} else {
		ts = time.Unix(0, w.Time*1000000)
	}
	// if no state is provided, state is implicit GOOD
	return PV{
		ts,
		w.Value,
		w.State,
	}
}

func pvToWire(pv PV) wirePV {
	var w wirePV
	w.Time = pv.Time.UnixNano() / 1000000
	w.Value = pv.Value
	w.State = pv.State
	return w
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
