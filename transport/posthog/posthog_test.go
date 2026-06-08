package posthog

import (
	"context"
	"errors"
	"testing"
	"time"

	posthogsdk "github.com/posthog/posthog-go"

	"github.com/dolthub/eventkit"
)

func TestBuildCaptureMapping(t *testing.T) {
	count := int64(42)
	dur := 150 * time.Millisecond
	gauge := 3.14
	req := &eventkit.LogEventsRequest{
		DistinctID: "machine-1",
		AppName:    "mycli",
		AppVersion: "1.2.3",
		Platform:   "linux",
	}
	evt := eventkit.EventRecord{
		ID:        "evt-1",
		Name:      "command.clone",
		StartTime: time.Unix(1000, 0).UTC(),
		EndTime:   time.Unix(1000, 0).Add(250 * time.Millisecond).UTC(),
		Attributes: []eventkit.Attribute{
			{Key: "remote_scheme", Value: "https"},
		},
		Metrics: []eventkit.Metric{
			{Key: "rows", Count: &count},
			{Key: "phase1", Duration: &dur},
			{Key: "load", Gauge: &gauge},
		},
	}

	cap := buildCapture(req, evt)

	if cap.DistinctId != "machine-1" || cap.Event != "command.clone" || cap.Uuid != "evt-1" {
		t.Fatalf("envelope = %+v", cap)
	}
	if !cap.Timestamp.Equal(evt.StartTime) {
		t.Fatalf("timestamp = %v", cap.Timestamp)
	}

	props := cap.Properties
	if props["event_id"] != "evt-1" {
		t.Fatalf("event_id = %v", props["event_id"])
	}
	if props["$app_name"] != "mycli" || props["$app_version"] != "1.2.3" || props["$os"] != "linux" {
		t.Fatalf("conventions = %+v", props)
	}
	if props["duration_ms"] != 250.0 {
		t.Fatalf("duration_ms = %v", props["duration_ms"])
	}
	if props["remote_scheme"] != "https" {
		t.Fatalf("attribute = %v", props["remote_scheme"])
	}
	if v, ok := props["rows"].(int64); !ok || v != 42 {
		t.Fatalf("rows = %v", props["rows"])
	}
	if props["phase1_ms"] != 150.0 {
		t.Fatalf("phase1_ms = %v", props["phase1_ms"])
	}
	if props["load"] != 3.14 {
		t.Fatalf("load = %v", props["load"])
	}
}

func TestFailureCallbackRecordsByUuid(t *testing.T) {
	e := &Emitter{failed: make(map[string]error)}
	cb := (*callback)(e)
	boom := errors.New("boom")

	cb.Success(posthogsdk.CaptureInApi{Uuid: "a"})
	cb.Failure(posthogsdk.CaptureInApi{Uuid: "b"}, boom)
	cb.Failure(posthogsdk.CaptureInApi{Uuid: "c"}, boom)

	if len(e.failed) != 2 {
		t.Fatalf("failed map = %v", e.failed)
	}
	if !errors.Is(e.failed["b"], boom) || !errors.Is(e.failed["c"], boom) {
		t.Fatalf("failed contents = %v", e.failed)
	}
	if _, present := e.failed["a"]; present {
		t.Fatal("success should not be recorded")
	}
}

func TestFailureCallbackIgnoresNonCapture(t *testing.T) {
	e := &Emitter{failed: make(map[string]error)}
	cb := (*callback)(e)
	cb.Failure(struct{}{}, errors.New("ignored"))
	if len(e.failed) != 0 {
		t.Fatalf("expected empty failed map, got %v", e.failed)
	}
}

func TestDrainReturnsAndClearsFailures(t *testing.T) {
	e := &Emitter{failed: map[string]error{"x": errors.New("nope")}, client: stubClient{}}
	failed, err := e.Drain(context.Background())
	if err != nil {
		t.Fatalf("Drain err = %v", err)
	}
	if len(failed) != 1 || failed["x"] == nil {
		t.Fatalf("failed = %v", failed)
	}
	if len(e.failed) != 0 {
		t.Fatalf("emitter state not cleared: %v", e.failed)
	}
}

type stubClient struct{ posthogsdk.Client }

func (stubClient) Close() error { return nil }
