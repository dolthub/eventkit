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
	"time"

	"github.com/google/uuid"
)

var NowFunc = time.Now

type Event struct {
	mu         sync.Mutex
	id         string
	name       string
	startTime  time.Time
	endTime    time.Time
	attributes map[string]string
	metrics    []Metric
	closed     bool
}

func NewEvent(name string) *Event {
	return &Event{
		id:         uuid.NewString(),
		name:       name,
		startTime:  NowFunc(),
		attributes: make(map[string]string),
	}
}

func (e *Event) SetAttribute(key, value string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		panic("eventkit: Event mutated after close")
	}
	e.attributes[key] = value
}

func (e *Event) AddMetric(m EventMetric) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		panic("eventkit: Event mutated after close")
	}
	e.metrics = append(e.metrics, m.Serialize())
}

func (e *Event) close() EventRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		panic("eventkit: Event closed twice")
	}
	e.closed = true
	e.endTime = NowFunc()

	attrs := make([]Attribute, 0, len(e.attributes))
	for k, v := range e.attributes {
		attrs = append(attrs, Attribute{Key: k, Value: v})
	}

	return EventRecord{
		ID:         e.id,
		Name:       e.name,
		StartTime:  e.startTime,
		EndTime:    e.endTime,
		Attributes: attrs,
		Metrics:    append([]Metric(nil), e.metrics...),
	}
}
