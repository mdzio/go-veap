package model

import (
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/mdzio/go-veap"
)

func buildTree(parent ChangeableCollection, depth int) {
	if depth > 0 {
		for i := 'a'; i <= 'c'; i++ {
			ident := string(i) + strconv.Itoa(int(i))
			domain := NewDomain(&DomainCfg{
				Identifier: ident,
				Title:      ident,
				Collection: parent,
			})
			buildTree(domain, depth-1)
		}
	}
}

func TestQuery(t *testing.T) {
	root := NewRoot(&RootCfg{})
	msvc := veap.BasicMetaService{Service: &Service{Root: root}}

	// build test model
	buildTree(root, 4)
	NewDomain(&DomainCfg{
		Identifier: "ä",
		Title:      "ä",
		Collection: root,
	})

	// define test cases
	cases := []struct {
		pathPatterns []string
		resultPaths  []string
		errText      string
	}{
		{
			pathPatterns: []string{"/"},
			resultPaths:  []string{"/"},
			errText:      "",
		},
		{
			pathPatterns: []string{"/a97"},
			resultPaths:  []string{"/a97"},
			errText:      "",
		},
		{
			pathPatterns: []string{"/notExists/notExists"},
			resultPaths:  []string{},
			errText:      "",
		},
		{
			pathPatterns: []string{"/*"},
			resultPaths:  []string{"/%C3%A4", "/a97", "/b98", "/c99"},
			errText:      "",
		},
		{
			pathPatterns: []string{"["},
			resultPaths:  nil,
			errText:      "Invalid content '[' in URL parameter",
		},
		{
			pathPatterns: []string{"%"},
			resultPaths:  nil,
			errText:      "Invalid content '%' in URL parameter",
		},
		{
			pathPatterns: []string{"/a97/??[8-9]"},
			resultPaths:  []string{"/a97/b98", "/a97/c99"},
			errText:      "",
		},
		{
			pathPatterns: []string{"/*98/??[8-9]"},
			resultPaths:  []string{"/b98/b98", "/b98/c99"},
			errText:      "",
		},
		{
			pathPatterns: []string{"/a97/??[^7]/*"},
			resultPaths: []string{"/a97/b98/a97", "/a97/b98/b98", "/a97/b98/c99",
				"/a97/c99/a97", "/a97/c99/b98", "/a97/c99/c99"},
			errText: "",
		},
	}

	// execute tests
	for idx, c := range cases {
		rs, err := msvc.Query(c.pathPatterns)
		if err != nil {
			if c.errText == "" {
				t.Errorf("Unexpected error in test case %d: %s", idx, err)
			} else {
				if !strings.Contains(err.Error(), c.errText) {
					t.Errorf("Wrong error in test case %d: %s", idx, err)
				}
			}
			continue
		}
		ps := make([]string, 0)
		for _, r := range rs {
			ps = append(ps, r.Path)
		}
		sort.Strings(ps)
		if !reflect.DeepEqual(c.resultPaths, ps) {
			t.Errorf("Wrong paths in test case %d: %s", idx, ps)
		}
	}
}
