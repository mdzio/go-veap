package server

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/mdzio/go-logging"
	"github.com/mdzio/go-veap"
)

func init() {
	logging.SetLevel(logging.TraceLevel)
}
func TestHandlerPV(t *testing.T) {
	cases := []struct {
		pvIn       veap.PV
		svcErrIn   veap.Error
		typeWanted string
		textWanted string
		codeWanted int
	}{
		{
			veap.PV{},
			veap.NewErrorf(veap.StatusForbidden, "error message 1"),
			"application/json",
			`{"message":"error message 1"}`,
			veap.StatusForbidden,
		},
		{
			veap.PV{
				Time:  time.Unix(1, 234567891),
				Value: 123.456,
				State: 42,
			},
			nil,
			"application/json",
			`{"ts":1234,"v":123.456,"s":42}`,
			veap.StatusOK,
		},
		{
			veap.PV{
				Time:  time.Unix(3, 0),
				Value: "Hello World!",
				State: 21,
			},
			nil,
			"application/json",
			`{"ts":3000,"v":"Hello World!","s":21}`,
			veap.StatusOK,
		},
		{
			veap.PV{
				Time:  time.Unix(123, 0),
				Value: []int{1, 2, 3},
				State: 200,
			},
			nil,
			"application/json",
			`{"ts":123000,"v":[1,2,3],"s":200}`,
			veap.StatusOK,
		},
	}

	var pvIn veap.PV
	var svcErrIn veap.Error
	svc := veap.FuncService{
		ReadPVFunc: func(path string) (veap.PV, veap.Error) { return pvIn, svcErrIn },
	}
	h := &Handler{Service: &svc}
	srv := httptest.NewServer(h)
	defer srv.Close()

	for _, c := range cases {
		pvIn = c.pvIn
		svcErrIn = c.svcErrIn

		resp, err := http.Get(srv.URL + "/~pv")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != c.codeWanted {
			t.Error(resp.StatusCode)
		}
		ct := resp.Header.Get("Content-Type")
		if ct != c.typeWanted {
			t.Error(ct)
		}
		b, _ := ioutil.ReadAll(resp.Body)
		if string(b) != c.textWanted {
			t.Error(string(b))
		}
	}
}

func TestHandlerSetPV(t *testing.T) {
	cases := []struct {
		pvIn       string
		svcErrIn   veap.Error
		pvWanted   veap.PV
		typeWanted string
		textWanted string
		codeWanted int
	}{
		{
			`{"ts":1234,"v":`,
			nil,
			veap.PV{},
			"application/json",
			`{"message":"Conversion of JSON to PV failed: unexpected EOF"}`,
			veap.StatusBadRequest,
		},
		{
			`{"ts":1234,"v":123.456,"s":42}`,
			nil,
			veap.PV{
				Time:  time.Unix(1, 234000000),
				Value: 123.456,
				State: 42,
			},
			"application/json",
			"",
			veap.StatusOK,
		},
		{
			`{"ts":1234,"v":["a","b","c"],"s":21}`,
			nil,
			veap.PV{
				Time:  time.Unix(1, 234000000),
				Value: []interface{}{"a", "b", "c"},
				State: 21,
			},
			"application/json",
			"",
			veap.StatusOK,
		},
		{
			`{"ts":1,"v":true,"s":0}`,
			veap.NewErrorf(veap.StatusForbidden, "no access"),
			veap.PV{
				Time:  time.Unix(0, 1000000),
				Value: true,
				State: 0,
			},
			"application/json",
			`{"message":"no access"}`,
			veap.StatusForbidden,
		},
	}

	var pvOut veap.PV
	var svcErrIn veap.Error
	svc := veap.FuncService{
		WritePVFunc: func(path string, pv veap.PV) veap.Error {
			pvOut = pv
			return svcErrIn
		},
	}
	h := &Handler{Service: &svc}
	srv := httptest.NewServer(h)
	defer srv.Close()

	client := &http.Client{}

	for _, c := range cases {
		svcErrIn = c.svcErrIn

		pvIn := bytes.NewBufferString(c.pvIn)
		req, err := http.NewRequest(http.MethodPut, srv.URL+"/~pv", pvIn)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != c.codeWanted {
			t.Error(resp.StatusCode)
		}
		ct := resp.Header.Get("Content-Type")
		if ct != c.typeWanted {
			t.Error(ct)
		}
		b, _ := ioutil.ReadAll(resp.Body)
		s := string(b)
		if s != c.textWanted {
			t.Error(s)
		}
		if !reflect.DeepEqual(pvOut, c.pvWanted) {
			t.Error(pvOut)
		}
	}
}

func TestHandlerHistory(t *testing.T) {
	cases := []struct {
		histIn     []veap.PV
		histWanted string
	}{
		{
			nil,
			`{"ts":[],"v":[],"s":[]}`,
		},
		{
			[]veap.PV{
				{Time: time.Unix(0, 1000000), Value: 3.0, State: 5},
				{Time: time.Unix(0, 2000000), Value: 4.0, State: 6},
			},
			`{"ts":[1,2],"v":[3,4],"s":[5,6]}`,
		},
	}

	var histIn []veap.PV
	var pathOut string
	var beginOut, endOut time.Time
	var limitOut int64
	svc := veap.FuncService{
		ReadHistoryFunc: func(path string, begin time.Time, end time.Time, limit int64) ([]veap.PV, veap.Error) {
			pathOut = path
			beginOut = begin
			endOut = end
			limitOut = limit
			return histIn, nil
		},
	}
	h := &Handler{Service: &svc}
	srv := httptest.NewServer(h)
	defer srv.Close()

	for _, c := range cases {
		histIn = c.histIn
		resp, err := http.Get(srv.URL + "/abc/~hist?begin=1&end=2&limit=3")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if pathOut != "/abc" {
			t.Error(pathOut)
		}
		if beginOut.UnixNano() != 1000000 {
			t.Error(beginOut)
		}
		if endOut.UnixNano() != 2000000 {
			t.Error(beginOut)
		}
		if limitOut != 3 {
			t.Error(limitOut)
		}
		if resp.StatusCode != veap.StatusOK {
			t.Error(resp.StatusCode)
		}
		b, _ := ioutil.ReadAll(resp.Body)
		s := string(b)
		if s != c.histWanted {
			t.Error(s)
		}
	}
}

func TestHandlerSetHistory(t *testing.T) {
	cases := []struct {
		histIn     string
		histWanted []veap.PV
	}{
		{
			`{"ts":[1,2],"v":[3,4],"s":[5,6]}`,
			[]veap.PV{
				{Time: time.Unix(0, 1000000), Value: 3.0, State: 5},
				{Time: time.Unix(0, 2000000), Value: 4.0, State: 6},
			},
		},
	}

	var histOut []veap.PV
	svc := veap.FuncService{
		WriteHistoryFunc: func(path string, hist []veap.PV) veap.Error {
			histOut = hist
			return nil
		},
	}
	h := &Handler{Service: &svc}
	srv := httptest.NewServer(h)
	defer srv.Close()

	client := &http.Client{}

	for _, c := range cases {
		histIn := bytes.NewBufferString(c.histIn)
		req, err := http.NewRequest(http.MethodPut, srv.URL+"/~hist", histIn)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != veap.StatusOK {
			t.Error(resp.StatusCode)
		}
		b, _ := ioutil.ReadAll(resp.Body)
		s := string(b)
		if len(s) > 0 {
			t.Error(s)
		}
		if !reflect.DeepEqual(histOut, c.histWanted) {
			t.Error(histOut)
		}
	}
}

func TestHandlerProperties(t *testing.T) {
	cases := []struct {
		propsIn    veap.AttrValues
		linksIn    []veap.Link
		jsonWanted string
	}{
		{
			veap.AttrValues{},
			[]veap.Link{},
			`{}`,
		},
		{
			veap.AttrValues{
				"a": 3, "b.c": "str",
			},
			[]veap.Link{
				{Role: "itf", Target: "..", Title: "Itf"},
				{Role: "itf", Target: "/a/b", Title: "B"},
			},
			`{"a":3,"b.c":"str","~links":[{"rel":"itf","href":"..","title":"Itf"},{"rel":"itf","href":"/veap/a/b","title":"B"}]}`,
		},
		{
			veap.AttrValues{
				"b": false,
			},
			[]veap.Link{
				{Role: "dp", Target: "c", Title: ""},
			},
			`{"b":false,"~links":[{"rel":"dp","href":"c"}]}`,
		},
	}

	var propsIn veap.AttrValues
	var linksIn []veap.Link
	svc := veap.FuncService{
		ReadPropertiesFunc: func(path string) (attributes veap.AttrValues, links []veap.Link, err veap.Error) {
			return propsIn, linksIn, nil
		},
	}
	h := &Handler{Service: &svc, URLPrefix: "/veap"}
	srv := httptest.NewServer(h)
	defer srv.Close()

	for _, c := range cases {
		propsIn = c.propsIn
		linksIn = c.linksIn

		resp, err := http.Get(srv.URL + "/veap/a")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != veap.StatusOK {
			t.Error(resp.StatusCode)
		}
		ct := resp.Header.Get("Content-Type")
		if ct != "application/json" {
			t.Error(ct)
		}
		b, _ := ioutil.ReadAll(resp.Body)
		if string(b) != c.jsonWanted {
			t.Error(string(b))
		}
	}
}

func TestHandlerSetProperties(t *testing.T) {
	cases := []struct {
		attrIn     string
		createdIn  bool
		attrWanted veap.AttrValues
		codeWanted int
	}{
		{
			`{}`,
			true,
			veap.AttrValues{},
			veap.StatusCreated,
		},
		{
			`{"active":false}`,
			false,
			veap.AttrValues{"active": false},
			veap.StatusOK,
		},
	}

	var attrOut veap.AttrValues
	var createdIn bool
	svc := veap.FuncService{
		WritePropertiesFunc: func(path string, attributes veap.AttrValues) (created bool, err veap.Error) {
			attrOut = attributes
			return createdIn, nil
		},
	}
	h := &Handler{Service: &svc}
	srv := httptest.NewServer(h)
	defer srv.Close()

	client := &http.Client{}

	for _, c := range cases {
		attrIn := bytes.NewBufferString(c.attrIn)
		createdIn = c.createdIn
		req, err := http.NewRequest(http.MethodPut, srv.URL+"/a", attrIn)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != c.codeWanted {
			t.Error(resp.StatusCode)
		}
		b, _ := ioutil.ReadAll(resp.Body)
		s := string(b)
		if len(s) > 0 {
			t.Error(s)
		}
		if !reflect.DeepEqual(attrOut, c.attrWanted) {
			t.Error(attrOut)
		}
	}
}

func TestHandlerDelete(t *testing.T) {
	cases := []struct {
		pathIn     string
		errIn      veap.Error
		codeWanted int
		textWanted string
	}{
		{
			`/a/b/c`,
			nil,
			veap.StatusOK,
			"",
		},
		{
			`/a`,
			veap.NewErrorf(veap.StatusNotFound, "not found"),
			veap.StatusNotFound,
			`{"message":"not found"}`,
		},
		{
			`/%2F`,
			nil,
			veap.StatusOK,
			"",
		},
	}

	var pathOut string
	var errIn veap.Error
	svc := veap.FuncService{
		DeleteFunc: func(path string) veap.Error {
			pathOut = path
			return errIn
		},
	}
	h := &Handler{Service: &svc}
	srv := httptest.NewServer(h)
	defer srv.Close()

	client := &http.Client{}

	for _, c := range cases {
		errIn = c.errIn
		req, err := http.NewRequest(http.MethodDelete, srv.URL+c.pathIn, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != c.codeWanted {
			t.Error(resp.StatusCode)
		}
		b, _ := ioutil.ReadAll(resp.Body)
		s := string(b)
		if s != c.textWanted {
			t.Error(s)
		}
		if c.pathIn != pathOut {
			t.Error(pathOut)
		}
	}
}

func TestHandlerStatistics(t *testing.T) {
	svc := veap.FuncService{
		ReadPVFunc: func(path string) (veap.PV, veap.Error) {
			return veap.PV{Time: time.Unix(1, 2), Value: 3, State: 4}, nil
		},
	}
	h := &Handler{Service: &svc}
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/~pv")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if h.Stats.Requests != 1 {
		t.Error(h.Stats.Requests)
	}
	if h.Stats.RequestBytes != 0 {
		t.Error(h.Stats.RequestBytes)
	}
	if h.Stats.ErrorResponses != 0 {
		t.Error(h.Stats.ErrorResponses)
	}
	if h.Stats.ResponseBytes != 23 {
		t.Error(h.Stats.ResponseBytes)
	}

	resp, err = http.Post(srv.URL+"/~pv", "", bytes.NewBufferString("0123456789"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if h.Stats.Requests != 2 {
		t.Error(h.Stats.Requests)
	}
	if h.Stats.RequestBytes != 10 {
		t.Error(h.Stats.RequestBytes)
	}
	if h.Stats.ErrorResponses != 1 {
		t.Error(h.Stats.ErrorResponses)
	}
	if h.Stats.ResponseBytes != 72 {
		t.Error(h.Stats.ResponseBytes)
	}
}

func TestHandlerRequestLimit(t *testing.T) {
	h := &Handler{
		RequestSizeLimit: 10,
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "", bytes.NewBufferString("01234567890"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Error(resp.StatusCode)
	}
	b, _ := ioutil.ReadAll(resp.Body)
	if string(b) != `{"message":"Receiving of request failed: http: request body too large"}` {
		t.Error(string(b))
	}
}

func httpPUT(url, body string) (string, error) {
	reqBody := bytes.NewBufferString(body)
	req, err := http.NewRequest(http.MethodPut, url, reqBody)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != veap.StatusOK {
		return "", fmt.Errorf("Status: %d", resp.StatusCode)
	}
	respBody, _ := ioutil.ReadAll(resp.Body)
	return string(respBody), nil
}

func TestExgData(t *testing.T) {
	svc := veap.FuncService{
		ReadPVFunc: func(path string) (veap.PV, veap.Error) {
			switch path {
			case "/a":
				return veap.PV{Time: time.Unix(0, 0), Value: 1.0}, nil
			case "/b":
				return veap.PV{Time: time.Unix(0, 0), Value: "bbb"}, nil
			default:
				return veap.PV{}, veap.NewErrorf(veap.StatusNotFound, "Not found: %s", path)
			}
		},
		WritePVFunc: func(path string, pv veap.PV) veap.Error {
			switch path {
			case "/a":
				if !reflect.DeepEqual(pv.Value, 2.0) {
					return veap.NewErrorf(veap.StatusBadRequest, "Invalid value for /a")
				}
				return nil
			case "/b":
				if !reflect.DeepEqual(pv.Value, "aaa") {
					return veap.NewErrorf(veap.StatusBadRequest, "Invalid value for /b")
				}
				return nil
			default:
				return veap.NewErrorf(veap.StatusNotFound, "Not found: %s", path)
			}
		},
	}
	msvc := &veap.BasicMetaService{Service: &svc}
	h := &Handler{Service: msvc}
	srv := httptest.NewServer(h)
	defer srv.Close()

	_, err := httpPUT(srv.URL+"/~exgdata", "")
	if err == nil {
		t.Fatalf("Expected error")
	}

	body := `{
		"writePVs":[
			{"path":"/a","pv":{"v":2.0}},
			{"path":"/b","pv":{"v":"aaa"}},
			{"path":"/c"}
		],
		"readPaths":[
			"/a",
			"/b",
			"/c"
		]
	}`
	resp, err := httpPUT(srv.URL+"/~exgdata", body)
	if err != nil {
		t.Fatal(err)
	}
	if resp != `{"writeErrors":[null,null,{"code":404,"message":"Not found: /c"}],`+
		`"readResults":[{"pv":{"ts":0,"v":1,"s":0}},{"pv":{"ts":0,"v":"bbb","s":0}},{"error":{"code":404,"message":"Not found: /c"}}]}` {
		t.Fatalf("Unexpected response: %s", resp)
	}

	body = `{
		"writePVs":[
			{"path":"/a","pv":{"v":1.0}},
			{"path":"/b","pv":{"v":"bbb"}}
		],
		"readPaths":[
			"/d"
		]
	}`
	resp, err = httpPUT(srv.URL+"/~exgdata", body)
	if err != nil {
		t.Fatal(err)
	}
	if resp != `{"writeErrors":[{"code":400,"message":"Invalid value for /a"},{"code":400,"message":"Invalid value for /b"}],`+
		`"readResults":[{"error":{"code":404,"message":"Not found: /d"}}]}` {
		t.Fatalf("Unexpected response: %s", resp)
	}
}
