package eventkit

import (
	"sync/atomic"
	"time"
)

type EventMetric interface {
	Serialize() Metric
}

type Counter struct {
	key string
	val atomic.Int64
}

func NewCounter(key string) *Counter {
	return &Counter{key: key}
}

func (c *Counter) Inc() {
	c.val.Add(1)
}

func (c *Counter) Dec() {
	c.val.Add(-1)
}

func (c *Counter) Add(n int64) {
	c.val.Add(n)
}

func (c *Counter) Serialize() Metric {
	v := c.val.Load()
	return Metric{Key: c.key, Count: &v}
}

type Timer struct {
	key   string
	start time.Time
	stop  time.Time
}

func NewTimer(key string) *Timer {
	return &Timer{key: key, start: NowFunc()}
}

func (t *Timer) Restart() {
	t.start = NowFunc()
	t.stop = time.Time{}
}

func (t *Timer) Stop() *Timer {
	t.stop = NowFunc()
	return t
}

func (t *Timer) Serialize() Metric {
	if t.stop.IsZero() {
		panic("eventkit: Timer must be stopped before serialization")
	}
	d := t.stop.Sub(t.start)
	return Metric{Key: t.key, Duration: &d}
}
