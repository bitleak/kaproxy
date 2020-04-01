package util

import "testing"

func TestWithRecover(t *testing.T) {
	WithRecover(nil, func() { panic("test1") })
	defer WithRecover(nil)
	panic("test2")
}
