package model

import (
	"reflect"
	"testing"
	"time"

	"github.com/mdzio/go-veap"
)

func TestRootAndDomain(t *testing.T) {
	// create root
	r := NewRoot(&RootCfg{
		Identifier:  "root",
		Title:       "My Root",
		Description: "Description",
		AdditionalAttr: veap.AttrValues{
			"myprop": 2,
		},
		ItemRole: "children",
	})
	s := &Service{Root: r}

	// test root
	attr, _, err := s.ReadProperties("/")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(attr, veap.AttrValues{
		"identifier":  "root",
		"title":       "My Root",
		"description": "Description",
		"myprop":      2,
	}) {
		t.Error(attr)
	}

	// add domain
	NewDomain(&DomainCfg{
		Identifier:     "mydomain",
		Title:          "My Domain",
		CollectionRole: "parent",
		ItemRole:       "children",
		Collection:     r,
	})

	// test domain
	attr, links, err := s.ReadProperties("/mydomain")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(attr, veap.AttrValues{
		"identifier": "mydomain",
		"title":      "My Domain",
	}) {
		t.Error(attr)
	}
	if !reflect.DeepEqual(links, []veap.Link{
		veap.Link{Target: "..", Title: "My Root", Role: "parent"},
	}) {
		t.Error(links)
	}

	// test root
	attr, links, err = s.ReadProperties("/")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(attr, veap.AttrValues{
		"identifier":  "root",
		"title":       "My Root",
		"description": "Description",
		"myprop":      2,
	}) {
		t.Error(attr)
	}
	if !reflect.DeepEqual(links, []veap.Link{
		veap.Link{Target: "mydomain", Title: "My Domain", Role: "children"},
	}) {
		t.Errorf("%++v", links)
	}

	// remove invalid domain
	pobj := r.RemoveItem("not found")
	if pobj != nil {
		t.Error("unexpected item")
	}

	// remove domain
	pobj = r.RemoveItem("mydomain")
	if pobj == nil {
		t.Error("expected item")
	}
	if pobj.GetTitle() != "My Domain" {
		t.Errorf("%v", pobj)
	}

	// test root
	_, links, err = s.ReadProperties("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 0 {
		t.Error("unexpected links")
	}
}

func TestVendor(t *testing.T) {
	r := NewRoot(&RootCfg{Title: "Root"})
	s := &Service{Root: r}
	NewVendor(&VendorCfg{
		Collection:        r,
		ServerName:        "TestName",
		ServerDescription: "TestDescription",
		ServerVersion:     "1.0.0",
		VendorName:        "VEAP",
	})
	attr, links, err := s.ReadProperties("/~vendor")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(attr, veap.AttrValues{
		"serverName":        "TestName",
		"serverVersion":     "1.0.0",
		"serverDescription": "TestDescription",
		"vendorName":        "VEAP",
		"identifier":        "~vendor",
		"title":             "Vendor Information",
		"veapVersion":       "1",
		"description":       "Information about the server and the vendor",
	}) {
		t.Error(attr)
	}
	if !reflect.DeepEqual(links, []veap.Link{
		veap.Link{Target: "..", Title: "Root", Role: "collection"},
	}) {
		t.Errorf("%++v", links)
	}
}

func TestROVariable(t *testing.T) {
	r := NewRoot(&RootCfg{})
	s := &Service{Root: r}
	NewROVariable(&ROVariableCfg{
		Identifier: "var",
		Collection: r,
		ReadPVFunc: func() (veap.PV, veap.Error) {
			return veap.PV{
				Time:  time.Unix(1, 234567891),
				Value: 123.456,
				State: 42,
			}, nil
		},
	})

	attr, links, err := s.ReadProperties("/var")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(attr, veap.AttrValues{
		"identifier": "var",
	}) {
		t.Error(attr)
	}
	if !reflect.DeepEqual(links, []veap.Link{
		veap.Link{Target: "..", Title: "", Role: "collection"},
		veap.Link{Target: "~pv", Title: "PV Service", Role: "service"},
	}) {
		t.Errorf("%++v", links)
	}

	pv, err := s.ReadPV("/var")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pv, veap.PV{
		Time:  time.Unix(1, 234567891),
		Value: 123.456,
		State: 42,
	}) {
		t.Error(pv)
	}

	pv, err = s.ReadPV("/unknownVar")
	var expectedPV veap.PV
	if pv != expectedPV {
		t.Error(pv)
	}
	if err == nil {
		t.Error("expected error")
	}
	if err.Code() != veap.StatusNotFound || err.Error() != "Item not found at /: unknownVar" {
		t.Error(err)
	}

	err = s.WritePV("/var", veap.PV{})
	if err == nil {
		t.Error("expected error")
	}
	if err.Code() != veap.StatusMethodNotAllowed {
		t.Error(err.Code())
	}
	if err.Error() != "Writing PV not supported: /var" {
		t.Error(err.Error())
	}
}

func TestVariable(t *testing.T) {
	r := NewRoot(&RootCfg{})
	s := &Service{Root: r}
	initpv := veap.PV{Time: time.Unix(2, 0), Value: 3.7, State: 100}
	pv := initpv
	NewVariable(&VariableCfg{
		Identifier: "testvar",
		Collection: r,
		ReadPVFunc: func() (veap.PV, veap.Error) {
			return pv, nil
		},
		WritePVFunc: func(newpv veap.PV) veap.Error {
			pv = newpv
			return nil
		},
	})

	pv, err := s.ReadPV("/testvar")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pv, initpv) {
		t.Error(pv)
	}

	spv := veap.PV{Time: time.Unix(2, 0), Value: 3.7, State: 100}
	err = s.WritePV("/testvar", spv)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pv, spv) {
		t.Error(pv)
	}

	pv, err = s.ReadPV("/testvar")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pv, spv) {
		t.Error(pv)
	}
}

func TestModifiableDomain(t *testing.T) {
	r := NewRoot(&RootCfg{Title: "Root title"})
	s := &Service{Root: r}

	NewModifiableDomain(&ModifiableDomainCfg{
		Identifier: "domain",
		Title:      "Domain title",
		CreateItem: func(c ChangeableCollection, id string, attr veap.AttrValues) veap.Error {
			NewDomain(&DomainCfg{
				Identifier:     id,
				Title:          id + " title",
				AdditionalAttr: attr,
				Collection:     c,
				CollectionRole: "parent",
			})
			return nil
		},
		Collection: r,
	})

	created, err := s.WriteProperties("/a", veap.AttrValues{})
	if err.Error() != "Create not supported: /a" || created {
		t.Error("expected error")
	}

	_, links, err := s.ReadProperties("/domain")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(links, []veap.Link{
		veap.Link{Target: "..", Title: "Root title", Role: "collection"},
	}) {
		t.Error(links)
	}

	created, err = s.WriteProperties("/domain/comp1", veap.AttrValues{"attr1": "value of attr1"})
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Error(created)
	}

	_, links, err = s.ReadProperties("/domain")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(links, []veap.Link{
		veap.Link{Target: "comp1", Title: "comp1 title", Role: "item"},
		veap.Link{Target: "..", Title: "Root title", Role: "collection"},
	}) {
		t.Error(links)
	}

	attr, links, err := s.ReadProperties("/domain/comp1")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(links, []veap.Link{
		veap.Link{Target: "..", Title: "Domain title", Role: "parent"},
	}) {
		t.Errorf("%+v", links)
	}
	if !reflect.DeepEqual(attr, veap.AttrValues{
		"identifier": "comp1",
		"title":      "comp1 title",
		"attr1":      "value of attr1",
	}) {
		t.Error(attr)
	}

	err = s.Delete("/domain/comp1")
	if err != nil {
		t.Error(err)
	}

	_, links, err = s.ReadProperties("/domain")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(links, []veap.Link{
		veap.Link{Target: "..", Title: "Root title", Role: "collection"},
	}) {
		t.Errorf("%+v", links)
	}
}

type dom struct {
	*Domain
	BasicLinks
}

func TestLinks(t *testing.T) {
	r := NewRoot(&RootCfg{})
	s := &Service{Root: r}

	d1 := &dom{
		Domain: NewDomain(&DomainCfg{
			Identifier: "d1",
		}),
	}
	r.PutItem(d1)
	d1.Collection = r
	d2 := NewDomain(&DomainCfg{
		Identifier: "d2",
		Collection: r,
	})

	_, links, err := s.ReadProperties("/d1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(links, []veap.Link{
		veap.Link{Target: "..", Title: "", Role: "collection"},
	}) {
		t.Errorf("%+v", links)
	}

	b := d1.PutLink(d2, "rel1")
	if !b {
		t.Error(b)
	}
	b = d1.PutLink(d2, "rel1")
	if b {
		t.Error(b)
	}
	b = d1.PutLink(d2, "rel2")
	if !b {
		t.Error(b)
	}
	b = d1.PutLink(d2, "rel2")
	if b {
		t.Error(b)
	}

	b = d1.RemoveLink(d2, "rel2")
	if !b {
		t.Error(b)
	}

	_, links, err = s.ReadProperties("/d1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(links, []veap.Link{
		veap.Link{Target: "..", Title: "", Role: "collection"},
		veap.Link{Target: "/d2", Title: "", Role: "rel1"},
	}) {
		t.Errorf("%+v", links)
	}

	b = d1.RemoveLink(d2, "rel1")
	if !b {
		t.Error(b)
	}
	b = d1.RemoveLink(d2, "rel1")
	if b {
		t.Error(b)
	}

	_, links, err = s.ReadProperties("/d1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(links, []veap.Link{
		veap.Link{Target: "..", Title: "", Role: "collection"},
	}) {
		t.Errorf("%+v", links)
	}
}

func TestLinkedDomain(t *testing.T) {
	r := NewRoot(&RootCfg{})
	s := &Service{Root: r}

	d1 := NewLinkedDomain(&DomainCfg{
		Identifier: "d1",
		Collection: r,
	})
	d1.PutLink(r, "2nd root link")

	_, links, err := s.ReadProperties("/d1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(links, []veap.Link{
		veap.Link{Target: "..", Title: "", Role: "collection"},
		veap.Link{Target: "/", Title: "", Role: "2nd root link"},
	}) {
		t.Errorf("%+v", links)
	}
}
