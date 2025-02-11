package utils

import (
	"context"
	"sync"
	"time"
)

// WaitGroup ...
type WaitGroup struct {
	ctx    context.Context
	cancel context.CancelFunc

	fn sync.WaitGroup
	wg sync.WaitGroup
}

// NewWaitGroup ...
func NewWaitGroup() *WaitGroup {
	w := &WaitGroup{}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	return w
}

// AddFunc ...
func (x *WaitGroup) AddFunc(interval time.Duration, f func()) {
	x.fn.Add(1)
	go func() {
		defer x.fn.Done()

		for {
			select {
			case <-time.After(interval):
				f()
			case <-x.ctx.Done():
				return
			}
		}
	}()
}

// Add ...
func (x *WaitGroup) Add(delta int) {
	x.wg.Add(1)
}

// Done ...
func (x *WaitGroup) Done() {
	x.wg.Done()
}

// TerminatedAndWait ...
func (x *WaitGroup) TerminatedAndWait() {
	x.cancel()
	x.fn.Wait()
	x.wg.Wait()
}
