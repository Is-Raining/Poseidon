package model

import "testing"

func TestAtomic1(t *testing.T) {
	type hello struct {
		name string
	}

	h1 := &hello{name: "hello"}
	ptr := &Atomic[*hello]{}

	ptr.Set(h1)
	h2 := ptr.Get()
	h2.name = "empty"

	t.Log(h1, h2)
}

func TestAtomic2(t *testing.T) {
	type hello struct {
		name string
	}

	h1 := hello{name: "hello"}
	ptr := &Atomic[hello]{}

	ptr.Set(h1)
	h2 := ptr.Get()
	h2.name = "nil"

	t.Log(h1, h2)
}
