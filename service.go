package veap

import (
	"fmt"
	"reflect"
	"time"
)

// State is the current state of a process value.
type State int

// Base state values for a process value.
const (
	StateGood      State = 0
	StateUncertain State = 100
	StateBad       State = 200
)

// Good returns true, if the state is good.
func (s State) Good() bool {
	return s >= StateGood && s < StateUncertain
}

// Uncertain returns true, if the state is uncertain.
func (s State) Uncertain() bool {
	return s >= StateUncertain && s < StateBad
}

// Bad returns true, if the state is bad or invalid.
func (s State) Bad() bool {
	return s >= StateBad || s < StateGood
}

// PV is a process value.
type PV struct {
	Time time.Time
	// Value must be supported by package json.
	Value interface{}
	State State
}

// Equal checks two PVs for equality.
func (pv PV) Equal(o PV) bool {
	if pv.Time != o.Time {
		return false
	}
	if pv.State != o.State {
		return false
	}
	if !reflect.DeepEqual(pv.Value, o.Value) {
		return false
	}
	return true
}

// VEAP service error codes. They are based on the HTTP status codes.
const (
	StatusOK                  int = 200
	StatusCreated             int = 201
	StatusBadRequest          int = 400
	StatusUnauthorized        int = 401
	StatusForbidden           int = 403
	StatusNotFound            int = 404
	StatusMethodNotAllowed    int = 405
	StatusInternalServerError int = 500

	// Signals an error in VEAP client code (e.g. no connection to VEAP server,
	// deserialization failed).
	StatusClientError = 900
)

// Error has an additional VEAP service error code.
type Error interface {
	Code() int
	error
}

type extendedError struct {
	error
	code int
}

func (e extendedError) Code() int {
	return e.code
}

// NewError creates an Error based on a standard error.
func NewError(code int, err error) Error {
	return extendedError{err, code}
}

type simpleError struct {
	msg  string
	code int
}

func (e simpleError) Code() int {
	return e.code
}

func (e simpleError) Error() string {
	return e.msg
}

// NewErrorf creates an Error with a code and a formatted message.
func NewErrorf(code int, format string, values ...interface{}) Error {
	return simpleError{fmt.Sprintf(format, values...), code}
}

// AttrValues is a container for named values.
type AttrValues map[string]interface{}

// Link describes a relationship of one VEAP object to another.
type Link struct {
	// Role describes the role (or type) of the target (e.g. device, channel,
	// room).
	Role string

	// Target is an absolute or relative path to the target object. The path
	// segments must be escaped with url.PathEscape.
	Target string

	// Title describes this link (e.g. name of the target object).
	Title string
}

// Service provides the VEAP base services. The path parameter is always
// escaped, use url.PathUnescape to unescape path segments.
type Service interface {
	// ReadPV reads the process value of a data point. VEAP-Protocol: HTTP-GET
	// on PV (.../~pv)
	ReadPV(path string) (PV, Error)

	// WritePV sets the process value of a data point. VEAP-Protocol: HTTP-PUT
	// on PV (.../~pv)
	WritePV(path string, pv PV) Error

	// ReadHistory retrieves the history of a data point. The times of the
	// returned entries must be in ascending order. VEAP-Protocol: HTTP-GET on
	// history (.../~hist)
	ReadHistory(path string, begin time.Time, end time.Time, limit int64) ([]PV, Error)

	// WriteHistory replaces the history of a data point. The replaced time
	// range goes from the minimum timestamp to the maximum timestamp.
	// VEAP-Protocol: HTTP-PUT on history (.../~hist)
	WriteHistory(path string, timeSeries []PV) Error

	// ReadProperties returns the attributes and links of a VEAP object.
	// Attribute values must be supported by package json. VEAP-Protocol:
	// HTTP-GET on object
	ReadProperties(path string) (attributes AttrValues, links []Link, err Error)

	// WriteProperties updates properties of an existing VEAP object. If no
	// object exists at the specified path, a new object is created. Links are
	// intentionally not handled. (A concept is still pending.) Attributes were
	// unmarshalled with package json. VEAP-Protocol: HTTP-PUT on object
	WriteProperties(path string, attributes AttrValues) (created bool, err Error)

	// Delete destroys a VEAP object. VEAP-Protocol: HTTP-DELETE on object
	Delete(path string) Error
}

// FuncService delegates service calls to a set of functions.
type FuncService struct {
	ReadPVFunc          func(path string) (PV, Error)
	WritePVFunc         func(path string, pv PV) Error
	ReadHistoryFunc     func(path string, begin time.Time, end time.Time, limit int64) ([]PV, Error)
	WriteHistoryFunc    func(path string, timeSeries []PV) Error
	ReadPropertiesFunc  func(path string) (attributes AttrValues, links []Link, err Error)
	WritePropertiesFunc func(path string, attributes AttrValues) (created bool, err Error)
	DeleteFunc          func(path string) Error
}

// ReadPV implements Service.
func (s *FuncService) ReadPV(path string) (PV, Error) {
	if s.ReadPVFunc == nil {
		return PV{}, NewErrorf(StatusInternalServerError, "PVFunc not provided")
	}
	return s.ReadPVFunc(path)
}

// WritePV implements Service.
func (s *FuncService) WritePV(path string, pv PV) Error {
	if s.WritePVFunc == nil {
		return NewErrorf(StatusInternalServerError, "SetPVFunc not provided")
	}
	return s.WritePVFunc(path, pv)
}

// ReadHistory implements Service.
func (s *FuncService) ReadHistory(path string, begin time.Time, end time.Time, limit int64) ([]PV, Error) {
	if s.ReadHistoryFunc == nil {
		return []PV{}, NewErrorf(StatusInternalServerError, "HistoryFunc not provided")
	}
	return s.ReadHistoryFunc(path, begin, end, limit)
}

// WriteHistory implements Service.
func (s *FuncService) WriteHistory(path string, timeSeries []PV) Error {
	if s.WriteHistoryFunc == nil {
		return NewErrorf(StatusInternalServerError, "SetHistoryFunc not provided")
	}
	return s.WriteHistoryFunc(path, timeSeries)
}

// ReadProperties implements Service.
func (s *FuncService) ReadProperties(path string) (attributes AttrValues, links []Link, err Error) {
	if s.ReadPropertiesFunc == nil {
		return nil, nil, NewErrorf(StatusInternalServerError, "PropertiesFunc not provided")
	}
	return s.ReadPropertiesFunc(path)
}

// WriteProperties implements Service.
func (s *FuncService) WriteProperties(path string, attributes AttrValues) (bool, Error) {
	if s.WritePropertiesFunc == nil {
		return false, NewErrorf(StatusInternalServerError, "SetPropertiesFunc not provided")
	}
	return s.WritePropertiesFunc(path, attributes)
}

// Delete implements Service.
func (s *FuncService) Delete(path string) Error {
	if s.DeleteFunc == nil {
		return NewErrorf(StatusInternalServerError, "DeleteFunc not provided")
	}
	return s.DeleteFunc(path)
}
