package eventkit

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type memEmitter struct {
	mu       sync.Mutex
	requests []*LogEventsRequest
	failN    atomic.Int32
	fail     error
}

func (m *memEmitter) Send(_ context.Context, req *LogEventsRequest) error {
	if n := m.failN.Add(-1); n >= 0 {
		return m.fail
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = append(m.requests, req)
	return nil
}

func (m *memEmitter) batches() [][]EventRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]EventRecord, len(m.requests))
	for i, r := range m.requests {
		out[i] = r.Events
	}
	return out
}

func newEvent(t *testing.T, name string) *Event {
	t.Helper()
	return NewEvent(name)
}

func TestCollectorBatchThreshold(t *testing.T) {
	em := &memEmitter{}
	c := NewCollector(em,
		WithMaxBatch(3),
		WithDistinctID("did"), WithAppName("test"),
	)
	for i := 0; i < 3; i++ {
		c.CloseEventAndAdd(newEvent(t, "e"))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(em.batches()) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.CloseEventAndAdd(newEvent(t, "e"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	batches := em.batches()
	if len(batches) < 2 {
		t.Fatalf("want >= 2 batches, got %d", len(batches))
	}
	total := 0
	for _, b := range batches {
		total += len(b)
	}
	if total != 4 {
		t.Fatalf("total events = %d, want 4", total)
	}
	if len(batches[0]) != 3 {
		t.Fatalf("first batch size = %d, want 3", len(batches[0]))
	}
}

func TestCollectorBackoffRetries(t *testing.T) {
	em := &memEmitter{fail: errors.New("boom")}
	em.failN.Store(2)

	c := NewCollector(em,
		WithMaxBatch(2),
		WithBackoff(10*time.Millisecond, 20*time.Millisecond),
		WithDistinctID("did"), WithAppName("test"),
	)
	c.CloseEventAndAdd(newEvent(t, "e"))
	c.CloseEventAndAdd(newEvent(t, "e"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(em.batches()) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	if len(em.batches()) == 0 {
		t.Fatal("expected eventual delivery after retries")
	}
}

func TestCollectorRequestEnvelope(t *testing.T) {
	em := &memEmitter{}
	c := NewCollector(em,
		WithDistinctID("machine-xyz"),
		WithAppName("mycli"),
		WithAppVersion("1.2.3"),
		WithPlatform("linux"),
	)
	c.CloseEventAndAdd(newEvent(t, "e"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.Close(ctx)

	em.mu.Lock()
	defer em.mu.Unlock()
	if len(em.requests) != 1 {
		t.Fatalf("requests = %d", len(em.requests))
	}
	r := em.requests[0]
	if r.DistinctID != "machine-xyz" || r.AppName != "mycli" || r.AppVersion != "1.2.3" || r.Platform != "linux" {
		t.Fatalf("envelope = %+v", r)
	}
}

func TestCollectorDisabledDropsEvents(t *testing.T) {
	em := &memEmitter{}
	c := NewCollector(em, WithDisabled(func() bool { return true }))
	for i := 0; i < 5; i++ {
		c.CloseEventAndAdd(newEvent(t, "e"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.Close(ctx)
	if len(em.batches()) != 0 {
		t.Fatalf("expected zero batches, got %d", len(em.batches()))
	}
}

func TestCollectorCloseRespectsContext(t *testing.T) {
	em := &slowEmitter{delay: 500 * time.Millisecond}
	c := NewCollector(em, WithDistinctID("d"), WithAppName("a"))
	c.CloseEventAndAdd(newEvent(t, "e"))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := c.Close(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}

type slowEmitter struct{ delay time.Duration }

func (s *slowEmitter) Send(ctx context.Context, _ *LogEventsRequest) error {
	select {
	case <-time.After(s.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type recordingFailEmitter struct {
	mu        sync.Mutex
	attempted []*LogEventsRequest
}

func (e *recordingFailEmitter) Send(_ context.Context, req *LogEventsRequest) error {
	e.mu.Lock()
	cp := *req
	cp.Events = append([]EventRecord(nil), req.Events...)
	e.attempted = append(e.attempted, &cp)
	e.mu.Unlock()
	return errors.New("always fails")
}

func TestCollectorTruncatesOldestUnderBackpressure(t *testing.T) {
	em := &recordingFailEmitter{}
	const maxBatch = 4
	const total = 50

	c := NewCollector(em,
		WithMaxBatch(maxBatch),
		WithBackoff(time.Microsecond, time.Microsecond),
		WithDistinctID("d"), WithAppName("a"),
	)
	for i := 1; i <= total; i++ {
		evt := NewEvent("e")
		evt.SetAttribute("seq", strconv.Itoa(i))
		c.CloseEventAndAdd(evt)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}

	em.mu.Lock()
	defer em.mu.Unlock()
	if len(em.attempted) == 0 {
		t.Fatal("no send attempts recorded")
	}

	for i, r := range em.attempted {
		if len(r.Events) > maxBatch {
			t.Fatalf("attempt %d had %d events, want <= %d", i, len(r.Events), maxBatch)
		}
	}

	last := em.attempted[len(em.attempted)-1]
	if len(last.Events) == 0 {
		t.Fatal("last attempt had no events")
	}
	lastEvt := last.Events[len(last.Events)-1]
	var lastSeq string
	for _, a := range lastEvt.Attributes {
		if a.Key == "seq" {
			lastSeq = a.Value
		}
	}
	if lastSeq != strconv.Itoa(total) {
		t.Fatalf("final unsent buffer does not contain newest event: tail seq = %q, want %d", lastSeq, total)
	}
}

func TestNextBackoff(t *testing.T) {
	if got := nextBackoff(time.Second, 10*time.Second); got != 2*time.Second {
		t.Fatalf("got %v", got)
	}
	if got := nextBackoff(8*time.Second, 10*time.Second); got != 10*time.Second {
		t.Fatalf("got %v", got)
	}
}
