package encoding

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/mdzio/go-veap"
)

type WirePV struct {
	Time  int64       `json:"ts"`
	Value interface{} `json:"v"`
	State veap.State  `json:"s"`
}

var errUnexpectetContent = errors.New("Unexpectet content")

func BytesToPV(payload []byte, fuzzy bool) (veap.PV, error) {
	// try to convert JSON to WirePV
	var w WirePV
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	err := dec.Decode(&w)
	if err == nil {
		// check for unexpected content
		c, err2 := ioutil.ReadAll(dec.Buffered())
		if err2 != nil {
			return veap.PV{}, fmt.Errorf("ReadAll failed: %w", err2)
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
			return veap.PV{}, err
		}
		// take whole payload as JSON value
		var v interface{}
		err = json.Unmarshal(payload, &v)
		if err == nil {
			w = WirePV{Value: v}
		} else {
			// if no valid JSON content is found, use the whole payload as string
			w = WirePV{Value: string(payload)}
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
	return veap.PV{
		Time:  ts,
		Value: w.Value,
		State: w.State,
	}, nil
}

func WireToPV(WirePV WirePV) veap.PV {
	// if no timestamp is provided, use current time
	var ts time.Time
	if WirePV.Time == 0 {
		ts = time.Now()
	} else {
		ts = time.Unix(0, WirePV.Time*1000000)
	}

	// if no state is provided, state is implicit GOOD
	return veap.PV{
		Time:  ts,
		Value: WirePV.Value,
		State: WirePV.State,
	}
}

func PVToWire(pv veap.PV) WirePV {
	return WirePV{
		Time:  pv.Time.UnixNano() / 1000000,
		Value: pv.Value,
		State: pv.State,
	}
}

type WireHist struct {
	Times  []int64       `json:"ts"`
	Values []interface{} `json:"v"`
	States []veap.State  `json:"s"`
}

func HistToWire(hist []veap.PV) WireHist {
	w := WireHist{}
	w.Times = make([]int64, len(hist))
	w.Values = make([]interface{}, len(hist))
	w.States = make([]veap.State, len(hist))
	for i, e := range hist {
		w.Times[i] = e.Time.UnixNano() / 1000000
		w.Values[i] = e.Value
		w.States[i] = e.State
	}
	return w
}

func WireToHist(w WireHist) ([]veap.PV, error) {
	l := len(w.Times)
	if len(w.Values) != l || len(w.States) != l {
		return nil, veap.NewErrorf(veap.StatusBadRequest, "History arrays must have same length")
	}
	hist := make([]veap.PV, l)
	for i := 0; i < l; i++ {
		hist[i] = veap.PV{
			Time:  time.Unix(0, w.Times[i]*1000000),
			Value: w.Values[i],
			State: w.States[i],
		}
	}
	return hist, nil
}

type WireLink struct {
	Role   string `json:"rel"`
	Target string `json:"href"`
	Title  string `json:"title,omitempty"`
}

type WireError struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

func ErrorToWire(err veap.Error) *WireError {
	if err == nil {
		return nil
	}
	return &WireError{err.Code(), err.Error()}
}

type WireWritePVParam struct {
	Path string `json:"path"`
	PV   WirePV `json:"pv"`
}

type WireExgDataParams struct {
	WritePVs  []WireWritePVParam `json:"writePVs"`
	ReadPaths []string           `json:"readPaths"`
}

type WireReadPVResult struct {
	PV    *WirePV    `json:"pv,omitempty"`
	Error *WireError `json:"error,omitempty"`
}

type WireExgDataResults struct {
	WriteErrors []*WireError       `json:"writeErrors"`
	ReadResults []WireReadPVResult `json:"readResults"`
}

func WireToExgDataParams(w *WireExgDataParams) (writePVs []veap.WritePVParam, readPaths []string) {
	writePVs = make([]veap.WritePVParam, len(w.WritePVs))
	for i := range w.WritePVs {
		writePVs[i].Path = w.WritePVs[i].Path
		writePVs[i].PV = WireToPV(w.WritePVs[i].PV)
	}
	readPaths = w.ReadPaths
	return
}

func ExgDataResultsToWire(writeErrors []veap.Error, readResults []veap.ReadPVResult) *WireExgDataResults {
	w := &WireExgDataResults{}
	w.WriteErrors = make([]*WireError, len(writeErrors))
	for i := range writeErrors {
		w.WriteErrors[i] = ErrorToWire(writeErrors[i])
	}
	w.ReadResults = make([]WireReadPVResult, len(readResults))
	for i := range readResults {
		if err := readResults[i].Error; err != nil {
			w.ReadResults[i].Error = ErrorToWire(err)
		} else {
			wpv := PVToWire(readResults[i].PV)
			w.ReadResults[i].PV = &wpv
		}
	}
	return w
}
