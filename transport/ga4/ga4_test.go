package ga4

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dolthub/eventkit"
)

func TestBuildEventMapping(t *testing.T) {
	count := int64(42)
	dur := 150 * time.Millisecond
	r := eventkit.EventRecord{
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
		},
	}

	e := buildEvent(r, "sess-1")

	if e.Name != "command_clone" {
		t.Fatalf("name = %q, want command_clone", e.Name)
	}
	if e.Params["session_id"] != "sess-1" {
		t.Fatalf("session_id = %v", e.Params["session_id"])
	}
	if v, ok := e.Params["engagement_time_msec"].(int64); !ok || v != 1 {
		t.Fatalf("engagement_time_msec = %v", e.Params["engagement_time_msec"])
	}
	if e.Params["event_id"] != "evt-1" {
		t.Fatalf("event_id = %v", e.Params["event_id"])
	}
	if e.Params["duration_ms"] != 250.0 {
		t.Fatalf("duration_ms = %v", e.Params["duration_ms"])
	}
	if e.Params["remote_scheme"] != "https" {
		t.Fatalf("remote_scheme = %v", e.Params["remote_scheme"])
	}
	if v, ok := e.Params["rows"].(int64); !ok || v != 42 {
		t.Fatalf("rows = %v", e.Params["rows"])
	}
	if e.Params["phase1_ms"] != 150.0 {
		t.Fatalf("phase1_ms = %v", e.Params["phase1_ms"])
	}
}

func TestBuildPayloadUserProperties(t *testing.T) {
	req := &eventkit.LogEventsRequest{
		DistinctID: "machine-1",
		AppName:    "mycli",
		AppVersion: "1.2.3",
		Platform:   "linux",
	}
	p := buildPayload(req, []eventkit.EventRecord{{ID: "x", Name: "ping"}})
	if p.ClientID != "machine-1" {
		t.Fatalf("client_id = %q", p.ClientID)
	}
	if p.UserProperties["app_name"].Value != "mycli" ||
		p.UserProperties["app_version"].Value != "1.2.3" ||
		p.UserProperties["platform"].Value != "linux" {
		t.Fatalf("user_properties = %+v", p.UserProperties)
	}
	if len(p.Events) != 1 || p.Events[0].Name != "ping" {
		t.Fatalf("events = %+v", p.Events)
	}
	if _, ok := p.Events[0].Params["session_id"]; !ok {
		t.Fatalf("session_id missing: %+v", p.Events[0].Params)
	}
	if v, ok := p.Events[0].Params["engagement_time_msec"].(int64); !ok || v != 1 {
		t.Fatalf("engagement_time_msec = %v", p.Events[0].Params["engagement_time_msec"])
	}
}

func TestBuildPayloadSharedSessionID(t *testing.T) {
	req := &eventkit.LogEventsRequest{DistinctID: "c"}
	p := buildPayload(req, []eventkit.EventRecord{
		{ID: "a", Name: "ping"},
		{ID: "b", Name: "pong"},
	})
	if p.Events[0].Params["session_id"] != p.Events[1].Params["session_id"] {
		t.Fatalf("session_id should be shared across batch: %v vs %v",
			p.Events[0].Params["session_id"], p.Events[1].Params["session_id"])
	}
}

func TestSanitizeName(t *testing.T) {
	cases := map[string]string{
		"command.clone": "command_clone",
		"":              "event",
		"123start":      "_123start",
		"hello world!":  "hello_world_",
	}
	for in, want := range cases {
		if got := sanitizeName(in); got != want {
			t.Errorf("sanitizeName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSendChunksAt25Events(t *testing.T) {
	var requests int
	var totalEvents int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		body, _ := io.ReadAll(r.Body)
		var p payload
		if err := json.Unmarshal(body, &p); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(p.Events) > MaxEventsPerRequest {
			t.Fatalf("got %d events in one request", len(p.Events))
		}
		totalEvents += len(p.Events)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	e, err := New(Config{
		MeasurementID: "G-XXX",
		APISecret:     "secret",
		Endpoint:      srv.URL,
	})
	if err != nil {
		t.Fatal(err)
	}

	events := make([]eventkit.EventRecord, 60)
	for i := range events {
		events[i] = eventkit.EventRecord{ID: "e", Name: "ping"}
	}
	if err := e.Send(context.Background(), &eventkit.LogEventsRequest{DistinctID: "c", Events: events}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if requests != 3 {
		t.Fatalf("requests = %d, want 3", requests)
	}
	if totalEvents != 60 {
		t.Fatalf("totalEvents = %d, want 60", totalEvents)
	}
}

func TestSendReturnsErrorOnHTTPFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	e, err := New(Config{MeasurementID: "G-X", APISecret: "s", Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	req := &eventkit.LogEventsRequest{
		DistinctID: "c",
		Events:     []eventkit.EventRecord{{ID: "a", Name: "ping"}},
	}
	if err := e.Send(context.Background(), req); err == nil {
		t.Fatal("expected error from Send when server returns 500")
	}
}

func TestNewAcceptsEmptyCredentials(t *testing.T) {
	if _, err := New(Config{Endpoint: "http://example"}); err != nil {
		t.Fatalf("MeasurementID and APISecret should be optional: %v", err)
	}
}

func TestSendOmitsCredentialsWhenEmpty(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	e, err := New(Config{Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	req := &eventkit.LogEventsRequest{
		DistinctID: "c",
		Events:     []eventkit.EventRecord{{ID: "a", Name: "ping"}},
	}
	if err := e.Send(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if gotQuery != "" {
		t.Fatalf("query = %q, want empty (no measurement_id or api_secret)", gotQuery)
	}
}
