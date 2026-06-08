package eventkit

import (
	"context"
	"testing"
	"time"
)

func TestEventLifecycle(t *testing.T) {
	prev := NowFunc
	defer func() { NowFunc = prev }()

	now := time.Unix(1000, 0)
	NowFunc = func() time.Time { return now }

	evt := NewEvent("command.test")
	evt.SetAttribute("remote", "origin")
	evt.SetAttribute("scheme", "https")

	c := NewCounter("rows")
	c.Add(5)
	c.Inc()
	evt.AddMetric(c)

	NowFunc = func() time.Time { return now.Add(2 * time.Second) }
	rec := evt.close()

	if rec.Name != "command.test" {
		t.Fatalf("name = %q", rec.Name)
	}
	if rec.ID == "" {
		t.Fatal("empty ID")
	}
	if !rec.StartTime.Equal(now) {
		t.Fatalf("start = %v", rec.StartTime)
	}
	if !rec.EndTime.Equal(now.Add(2 * time.Second)) {
		t.Fatalf("end = %v", rec.EndTime)
	}
	if len(rec.Attributes) != 2 {
		t.Fatalf("attrs = %d", len(rec.Attributes))
	}
	if len(rec.Metrics) != 1 || rec.Metrics[0].Count == nil || *rec.Metrics[0].Count != 6 {
		t.Fatalf("metrics = %+v", rec.Metrics)
	}
}

func TestEventDoubleCloseePanics(t *testing.T) {
	evt := NewEvent("x")
	evt.close()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	evt.close()
}

func TestContextRoundTrip(t *testing.T) {
	evt := NewEvent("x")
	ctx := NewContextForEvent(context.Background(), evt)
	if got := GetEventFromContext(ctx); got != evt {
		t.Fatalf("got %p want %p", got, evt)
	}
	if got := GetEventFromContext(context.Background()); got != nil {
		t.Fatalf("expected nil, got %p", got)
	}
}
