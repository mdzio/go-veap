package model

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mdzio/go-veap"

	"github.com/mdzio/go-lib/jsonutil"
)

func TestVendorAndStatistics(t *testing.T) {
	// build model
	root := NewRoot(&RootCfg{})
	service := &Service{Root: root}
	handler := &veap.Handler{Service: service}
	vendor := NewVendor(&VendorCfg{
		ServerName:        "VEAP Demonstration Server",
		ServerVersion:     "0.1.0",
		ServerDescription: "Reference implementation of the VEAP protocol",
		VendorName:        "VEAP",
		Collection:        root,
	})
	NewHandlerStats(vendor, &handler.Stats)

	// start test server
	server := httptest.NewServer(handler)
	defer server.Close()

	// request ~vendor
	resp, err := http.Get(server.URL + "/~vendor")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := ioutil.ReadAll(resp.Body)
	expected := []byte(`{
		"description":"Information about the server and the vendor",
		"identifier":"~vendor",
		"serverDescription":"Reference implementation of the VEAP protocol",
		"serverName":"VEAP Demonstration Server",
		"serverVersion":"0.1.0",
		"title":"Vendor Information",
		"veapVersion":"1",
		"vendorName":"VEAP",
		"~links":[
			{"rel":"item","href":"statistics","title":"HTTP(S) Handler Statistics"},
			{"rel":"collection","href":".."}
		]
	}`)
	if !jsonutil.Equal(b, expected) {
		t.Error(string(b))
	}

	// request ~vendor/statistics/requests/~pv
	cases := []string{`,"v":2,`, `,"v":3,`, `,"v":4,`}
	for _, c := range cases {
		resp, err := http.Get(server.URL + "/~vendor/statistics/requests/~pv")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := ioutil.ReadAll(resp.Body)
		str := string(b)
		if !strings.Contains(str, c) {
			t.Error(str)
		}
	}
}
