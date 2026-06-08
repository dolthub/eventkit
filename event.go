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
