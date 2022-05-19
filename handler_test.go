package veap

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
)

func init() {
	logging.SetLevel(logging.TraceLevel)
}
func TestHandlerPV(t *testing.T) {
	cases := []struct {
		pvIn       PV
		svcErrIn   Error
		typeWanted string
		textWanted string
		codeWanted int
	}{
		{
			PV{},
			NewErrorf(StatusForbidden, "error message 1"),
			"application/json",
			`{"message":"error message 1"}`,
			StatusForbidden,
		},
		{
			PV{
				time.Unix(1, 234567891),
				123.456,
				42,
			},
			nil,
			"application/json",
			`{"ts":1234,"v":123.456,"s":42}`,
			StatusOK,
		},
		{
			PV{
				time.Unix(3, 0),
				"Hello World!",
				21,
			},
			nil,
			"application/json",
			`{"ts":3000,"v":"Hello World!","s":21}`,
			StatusOK,
		},
		{
			PV{
				time.Unix(123, 0),
				[]int{1, 2, 3},
				200,
			},
			nil,
			"application/json",
			`{"ts":123000,"v":[1,2,3],"s":200}`,
			StatusOK,
		},
	}

	var pvIn PV
	var svcErrIn Error
	svc := FuncService{
		ReadPVFunc: func(path string) (PV, Error) { return pvIn, svcErrIn },
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
		svcErrIn   Error
		pvWanted   PV
		typeWanted string
		textWanted string
		codeWanted int
	}{
		{
			`{"ts":1234,"v":`,
			nil,
			PV{},
			"application/json",
			`{"message":"Conversion of JSON to PV failed: unexpected end of JSON input"}`,
			StatusBadRequest,
		},
		{
			`{"ts":1234,"v":123.456,"s":42}`,
			nil,
			PV{
				time.Unix(1, 234000000),
				123.456,
				42,
			},
			"application/json",
			"",
			StatusOK,
		},
		{
			`{"ts":1234,"v":["a","b","c"],"s":21}`,
			nil,
			PV{
				time.Unix(1, 234000000),
				[]interface{}{"a", "b", "c"},
				21,
			},
			"application/json",
			"",
			StatusOK,
		},
		{
			`{"ts":1,"v":true,"s":0}`,
			NewErrorf(StatusForbidden, "no access"),
			PV{
				time.Unix(0, 1000000),
				true,
				0,
			},
			"application/json",
			`{"message":"no access"}`,
			StatusForbidden,
		},
	}

	var pvOut PV
	var svcErrIn Error
	svc := FuncService{
		WritePVFunc: func(path string, pv PV) Error {
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
		histIn     []PV
		histWanted string
	}{
		{
			nil,
			`{"ts":[],"v":[],"s":[]}`,
		},
		{
			[]PV{
				{time.Unix(0, 1000000), 3.0, 5},
				{time.Unix(0, 2000000), 4.0, 6},
			},
			`{"ts":[1,2],"v":[3,4],"s":[5,6]}`,
		},
	}

	var histIn []PV
	var pathOut string
	var beginOut, endOut time.Time
	var limitOut int64
	svc := FuncService{
		ReadHistoryFunc: func(path string, begin time.Time, end time.Time, limit int64) ([]PV, Error) {
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
		if resp.StatusCode != StatusOK {
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
		histWanted []PV
	}{
		{
			`{"ts":[1,2],"v":[3,4],"s":[5,6]}`,
			[]PV{
				{time.Unix(0, 1000000), 3.0, 5},
				{time.Unix(0, 2000000), 4.0, 6},
			},
		},
	}

	var histOut []PV
	svc := FuncService{
		WriteHistoryFunc: func(path string, hist []PV) Error {
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

		if resp.StatusCode != StatusOK {
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
		propsIn    AttrValues
		linksIn    []Link
		jsonWanted string
	}{
		{
			AttrValues{},
			[]Link{},
			`{}`,
		},
		{
			AttrValues{
				"a": 3, "b.c": "str",
			},
			[]Link{
				{"itf", "..", "Itf"},
				{"itf", "/a/b", "B"},
			},
			`{"a":3,"b.c":"str","~links":[{"rel":"itf","href":"..","title":"Itf"},{"rel":"itf","href":"/veap/a/b","title":"B"}]}`,
		},
		{
			AttrValues{
				"b": false,
			},
			[]Link{
				{"dp", "c", ""},
			},
			`{"b":false,"~links":[{"rel":"dp","href":"c"}]}`,
		},
	}

	var propsIn AttrValues
	var linksIn []Link
	svc := FuncService{
		ReadPropertiesFunc: func(path string) (attributes AttrValues, links []Link, err Error) {
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

		if resp.StatusCode != StatusOK {
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
		attrWanted AttrValues
		codeWanted int
	}{
		{
			`{}`,
			true,
			AttrValues{},
			StatusCreated,
		},
		{
			`{"active":false}`,
			false,
			AttrValues{"active": false},
			StatusOK,
		},
	}

	var attrOut AttrValues
	var createdIn bool
	svc := FuncService{
		WritePropertiesFunc: func(path string, attributes AttrValues) (created bool, err Error) {
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
		errIn      Error
		codeWanted int
		textWanted string
	}{
		{
			`/a/b/c`,
			nil,
			StatusOK,
			"",
		},
		{
			`/a`,
			NewErrorf(StatusNotFound, "not found"),
			StatusNotFound,
			`{"message":"not found"}`,
		},
		{
			`/%2F`,
			nil,
			StatusOK,
			"",
		},
	}

	var pathOut string
	var errIn Error
	svc := FuncService{
		DeleteFunc: func(path string) Error {
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
	svc := FuncService{
		ReadPVFunc: func(path string) (PV, Error) { return PV{time.Unix(1, 2), 3, 4}, nil },
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
	if resp.StatusCode != StatusOK {
		return "", fmt.Errorf("Status: %d", resp.StatusCode)
	}
	respBody, _ := ioutil.ReadAll(resp.Body)
	return string(respBody), nil
}

func TestExgData(t *testing.T) {
	svc := FuncService{
		ReadPVFunc: func(path string) (PV, Error) {
			switch path {
			case "/a":
				return PV{Time: time.Unix(0, 0), Value: 1.0}, nil
			case "/b":
				return PV{Time: time.Unix(0, 0), Value: "bbb"}, nil
			default:
				return PV{}, NewErrorf(StatusNotFound, "Not found: %s", path)
			}
		},
		WritePVFunc: func(path string, pv PV) Error {
			switch path {
			case "/a":
				if !reflect.DeepEqual(pv.Value, 2.0) {
					return NewErrorf(StatusBadRequest, "Invalid value for /a")
				}
				return nil
			case "/b":
				if !reflect.DeepEqual(pv.Value, "aaa") {
					return NewErrorf(StatusBadRequest, "Invalid value for /b")
				}
				return nil
			default:
				return NewErrorf(StatusNotFound, "Not found: %s", path)
			}
		},
	}
	msvc := &BasicMetaService{Service: &svc}
	h := &Handler{Service: msvc}
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := httpPUT(srv.URL+"/~exgdata", "")
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
	resp, err = httpPUT(srv.URL+"/~exgdata", body)
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
