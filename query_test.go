package veap

import (
	"reflect"
	"testing"
)

func TestChildPaths(t *testing.T) {
	results := childPaths("/a/b", []Link{
		{Target: "."},
		{Target: ".."},
		{Target: "c"},
		{Target: "c/d"},
		{Target: "/a/b/c2"},
		{Target: "../b/c3"},
	})
	if !reflect.DeepEqual(results, []string{
		"/a/b/c",
		"/a/b/c2",
		"/a/b/c3",
	}) {
		t.Error(results)
	}
}

func TestGlob(t *testing.T) {

}
