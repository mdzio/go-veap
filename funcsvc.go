package veap

import "time"

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
