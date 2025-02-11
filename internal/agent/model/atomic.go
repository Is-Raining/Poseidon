package model

import (
	"sync/atomic"
)

// Atomic ...
type Atomic[T any] struct {
	v atomic.Value
}

// Set ...
func (c *Atomic[T]) Set(t T) {
	c.v.Store(t)
}

// Get ...
func (c *Atomic[T]) Get() T {
	v := c.v.Load()
	if v == nil {
		var t T
		return t
	}
	return v.(T)
}
