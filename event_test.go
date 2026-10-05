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

func TestEventDoubleClosePanics(t *testing.T) {
	evt := NewEvent("x")
	evt.close()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	evt.close()
}

func TestEventMutationAfterClosePanics(t *testing.T) {
	evt := NewEvent("x")
	evt.close()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	evt.SetAttribute("late", "value")
}

func TestEventCloseCopiesMetrics(t *testing.T) {
	evt := NewEvent("x")
	evt.metrics = []Metric{{Key: "one"}}
	rec := evt.close()

	evt.metrics[0].Key = "changed"

	if len(rec.Metrics) != 1 || rec.Metrics[0].Key != "one" {
		t.Fatalf("record metrics changed after close: %+v", rec.Metrics)
	}
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
