package eventkit

import (
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

func TestTimerSerializeWithoutStopPanics(t *testing.T) {
	tm := NewTimer("op")
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	tm.Serialize()
}
