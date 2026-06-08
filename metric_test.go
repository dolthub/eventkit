package eventkit

import (
	"sync"
	"testing"
	"time"
)

func TestCounterSerialize(t *testing.T) {
	c := NewCounter("hits")
	c.Add(3)
	c.Inc()
	c.Dec()
	m := c.Serialize()
	if m.Key != "hits" {
		t.Fatalf("key = %q", m.Key)
	}
	if m.Count == nil || *m.Count != 3 {
		t.Fatalf("count = %+v", m.Count)
	}
	if m.Duration != nil || m.Gauge != nil {
		t.Fatal("only Count should be set")
	}
}

func TestTimerSerialize(t *testing.T) {
	prev := NowFunc
	defer func() { NowFunc = prev }()

	t0 := time.Unix(1000, 0)
	NowFunc = func() time.Time { return t0 }
	tm := NewTimer("op")
	NowFunc = func() time.Time { return t0.Add(150 * time.Millisecond) }
	tm.Stop()

	m := tm.Serialize()
	if m.Key != "op" || m.Duration == nil || *m.Duration != 150*time.Millisecond {
		t.Fatalf("got %+v", m)
	}
}

func TestCounterAtomicity(t *testing.T) {
	c := NewCounter("hits")
	const goroutines = 100
	const perGoroutine = 1000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				c.Inc()
				c.Inc()
				c.Dec()
				c.Add(2)
				c.Add(-1)
			}
		}()
	}
	wg.Wait()
	m := c.Serialize()
	want := int64(goroutines * perGoroutine * 2)
	if m.Count == nil || *m.Count != want {
		t.Fatalf("count = %v, want %d", m.Count, want)
	}
}

func TestTimerSerializeWithoutStopPanics(t *testing.T) {
	tm := NewTimer("op")
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	tm.Serialize()
}
