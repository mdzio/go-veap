package veap

import (
	"errors"
	"testing"
)

func TestNewErrorf(t *testing.T) {
	err := NewErrorf(StatusNotFound, "abc%s", "def")
	if err.Code() != StatusNotFound || err.Error() != "abcdef" {
		t.Fail()
	}
}

func TestNewError(t *testing.T) {
	err := NewError(StatusForbidden, errors.New("ghi"))
	if err.Code() != StatusForbidden || err.Error() != "ghi" {
		t.Fail()
	}
}
