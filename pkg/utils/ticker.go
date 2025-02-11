package utils

import (
	"time"
)

type Ticker struct {
	start time.Time
}

func NewTicker() *Ticker {
	t := &Ticker{}
	t.start = time.Now()

	return t
}

func (t *Ticker) Tick() {
	t.start = time.Now()
}

func (t *Ticker) Tock() time.Duration {
	return time.Since(t.start)
}
