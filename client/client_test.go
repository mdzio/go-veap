package client

import (
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"
	"time"

	_ "github.com/mdzio/go-lib/testutil"
	"github.com/mdzio/go-veap"
	"github.com/mdzio/go-veap/model"
	"github.com/mdzio/go-veap/server"
)

func TestExgData(t *testing.T) {
	// create simple test server
	vars := map[string]veap.PV{
		"/a": {Time: time.Unix(1, 0), Value: 1.0},
		"/b": {Time: time.Unix(1, 0), Value: "b"},
	}
	svc := veap.FuncService{
		ReadPVFunc: func(path string) (veap.PV, veap.Error) {
			pv, ok := vars[path]
			if ok {
				return pv, nil
			} else {
				return veap.PV{}, veap.NewErrorf(veap.StatusNotFound, "Not found: %s", path)
			}
		},
		WritePVFunc: func(path string, pv veap.PV) veap.Error {
			if _, ok := vars[path]; !ok {
				return veap.NewErrorf(veap.StatusNotFound, "Not found: %s", path)
			}
			vars[path] = pv
			return nil
		},
	}
	msvc := &veap.BasicMetaService{Service: &svc}
	h := &server.Handler{Service: msvc}
	srv := httptest.NewServer(h)
	defer srv.Close()

	// create client
	cln := &Client{URL: srv.URL}
	cln.Init()

	// no op tests
	writeErrors, readResults, err := cln.ExgData(nil, nil)
	if err != nil || len(writeErrors) != 0 || len(readResults) != 0 {
		t.Fatal()
	}

	// read test
	writeErrors, readResults, err = cln.ExgData(nil, []string{"/a", "/b"})
	if err != nil || len(writeErrors) != 0 || len(readResults) != 2 {
		t.Fatal()
	}
	if readResults[0].Error != nil || readResults[1].Error != nil {
		t.Fatal()
	}
	if !readResults[0].PV.Equal(vars["/a"]) || !readResults[1].PV.Equal(vars["/b"]) {
		t.Fatal()
	}

	// write/read test
	writeErrors, readResults, err = cln.ExgData(
		[]veap.WritePVParam{
			{Path: "/a", PV: veap.PV{Time: time.Unix(2, 0), State: veap.StateUncertain, Value: 42}},
		},
		[]string{"/a"},
	)
	if err != nil || len(writeErrors) != 1 || len(readResults) != 1 {
		t.Fatal()
	}
	if writeErrors[0] != nil || readResults[0].Error != nil {
		t.Fatal()
	}
	if readResults[0].PV.Equal(veap.PV{Time: time.Unix(2, 0), State: veap.StateUncertain, Value: 42}) {
		t.Fatal()
	}

	// error test
	writeErrors, readResults, err = cln.ExgData(
		[]veap.WritePVParam{{Path: "/x", PV: veap.PV{}}},
		[]string{"/y"},
	)
	if err != nil || len(writeErrors) != 1 || len(readResults) != 1 {
		t.Fatal()
	}
	if writeErrors[0] == nil || writeErrors[0].Code() != 404 || writeErrors[0].Error() != "Not found: /x" {
		t.Fatal()
	}
	if readResults[0].Error == nil || readResults[0].Error.Code() != 404 || readResults[0].Error.Error() != "Not found: /y" {
		t.Fatal()
	}
}

func buildTree(parent model.ChangeableCollection, depth int) {
	if depth == 0 {
		return
	}
	for i := 'a'; i <= 'c'; i++ {
		ident := string(i) + strconv.Itoa(int(i))
		domain := model.NewDomain(&model.DomainCfg{
			Identifier: ident,
			Title:      ident,
			Collection: parent,
		})
		buildTree(domain, depth-1)
	}
}

func TestQuery(t *testing.T) {
	// create simple test server
	root := model.NewRoot(&model.RootCfg{})
	buildTree(root, 2)
	// non ASCII character
	model.NewDomain(&model.DomainCfg{
		Identifier: "ä",
		Title:      "ä",
		Collection: root,
	})
	msvc := &veap.BasicMetaService{Service: &model.Service{Root: root}}
	h := &server.Handler{Service: msvc}
	srv := httptest.NewServer(h)
	defer srv.Close()

	// create client
	cln := &Client{URL: srv.URL}
	cln.Init()

	// no op test
	res, err := cln.Query(nil)
	if err != nil || len(res) != 0 {
		t.Fatal()
	}

	// single domain test
	res, err = cln.Query([]string{"/a97/b98"})
	if err != nil {
		t.Fatal()
	}
	if len(res) != 1 {
		t.Fatal()
	}
	exp := veap.QueryResult{
		Path:       "/a97/b98",
		Attributes: veap.AttrValues{"identifier": "b98", "title": "b98"},
		Links:      []veap.Link{{Role: "collection", Target: "..", Title: "a97"}},
	}
	if !reflect.DeepEqual(res[0], exp) {
		t.Fatal()
	}

	// multiple domain test
	res, err = cln.Query([]string{"/a97/a*", "/b98/*"})
	if err != nil {
		t.Fatal()
	}
	if len(res) != 4 {
		t.Fatal(res)
	}

	// non ASCII character test
	res, err = cln.Query([]string{"/ä"})
	if err != nil {
		t.Fatal()
	}
	if len(res) != 1 {
		t.Fatal()
	}
	exp = veap.QueryResult{
		Path:       "/%C3%A4",
		Attributes: veap.AttrValues{"identifier": "ä", "title": "ä"},
		Links:      []veap.Link{{Role: "collection", Target: ".."}},
	}
	if !reflect.DeepEqual(res[0], exp) {
		t.Fatal()
	}
}
