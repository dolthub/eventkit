// Copyright 2026 DoltHub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the root
// of this repository or at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventkit

import (
	"sync"
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
	mu    sync.Mutex
	key   string
	start time.Time
	stop  time.Time
}

func NewTimer(key string) *Timer {
	return &Timer{key: key, start: NowFunc()}
}

func (t *Timer) Stop() *Timer {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stop = NowFunc()
	return t
}

func (t *Timer) Serialize() Metric {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stop.IsZero() {
		panic("eventkit: Timer must be stopped before serialization")
	}
	d := t.stop.Sub(t.start)
	return Metric{Key: t.key, Duration: &d}
}
