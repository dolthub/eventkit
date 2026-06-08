package posthog

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	posthogsdk "github.com/posthog/posthog-go"

	"github.com/dolthub/eventkit"
)

const (
	DefaultInterval  = 250 * time.Millisecond
	DefaultBatchSize = 250
)

type Config struct {
	APIKey           string
	Endpoint         string
	BatchSize        int
	Interval         time.Duration
	UploadTimeout    time.Duration
	ShutdownDeadline time.Duration
}

type Emitter struct {
	client posthogsdk.Client

	mu     sync.Mutex
	failed map[string]error
}

func New(cfg Config) (*Emitter, error) {
	if cfg.APIKey == "" {
		return nil, errors.New("posthog: APIKey is required")
	}
	if cfg.Interval == 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = DefaultBatchSize
	}

	e := &Emitter{failed: make(map[string]error)}

	pcfg := posthogsdk.Config{
		Endpoint:           cfg.Endpoint,
		BatchSize:          cfg.BatchSize,
		Interval:           cfg.Interval,
		BatchUploadTimeout: cfg.UploadTimeout,
		ShutdownTimeout:    cfg.ShutdownDeadline,
		IsServer:           posthogsdk.Ptr(false),
		Callback:           (*callback)(e),
	}
	client, err := posthogsdk.NewWithConfig(cfg.APIKey, pcfg)
	if err != nil {
		return nil, err
	}
	e.client = client
	return e, nil
}

func (e *Emitter) Send(_ context.Context, req *eventkit.LogEventsRequest) error {
	if req == nil || len(req.Events) == 0 {
		return nil
	}
	for _, evt := range req.Events {
		if err := e.client.Enqueue(buildCapture(req, evt)); err != nil {
			e.recordFailure(evt.ID, err)
		}
	}
	return nil
}

func (e *Emitter) Drain(_ context.Context) (map[string]error, error) {
	closeErr := e.client.Close()
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]error, len(e.failed))
	for k, v := range e.failed {
		out[k] = v
	}
	e.failed = make(map[string]error)
	return out, closeErr
}

func (e *Emitter) recordFailure(uuid string, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.failed[uuid] = err
}

type callback Emitter

func (c *callback) Success(_ posthogsdk.APIMessage) {}

func (c *callback) Failure(msg posthogsdk.APIMessage, err error) {
	cap, ok := msg.(posthogsdk.CaptureInApi)
	if !ok {
		return
	}
	(*Emitter)(c).recordFailure(cap.Uuid, err)
}

func buildCapture(req *eventkit.LogEventsRequest, evt eventkit.EventRecord) posthogsdk.Capture {
	props := posthogsdk.NewProperties()
	props.Set("event_id", evt.ID)
	if req.AppName != "" {
		props.Set("$app_name", req.AppName)
	}
	if req.AppVersion != "" {
		props.Set("$app_version", req.AppVersion)
	}
	if req.Platform != "" {
		props.Set("$os", req.Platform)
	}
	if !evt.EndTime.IsZero() && !evt.StartTime.IsZero() {
		props.Set("duration_ms", float64(evt.EndTime.Sub(evt.StartTime).Nanoseconds())/1e6)
	}
	for _, a := range evt.Attributes {
		props.Set(a.Key, a.Value)
	}
	for _, m := range evt.Metrics {
		switch {
		case m.Count != nil:
			props.Set(m.Key, *m.Count)
		case m.Duration != nil:
			props.Set(fmt.Sprintf("%s_ms", m.Key), float64(m.Duration.Nanoseconds())/1e6)
		case m.Gauge != nil:
			props.Set(m.Key, *m.Gauge)
		}
	}
	return posthogsdk.Capture{
		Uuid:       evt.ID,
		DistinctId: req.DistinctID,
		Event:      evt.Name,
		Timestamp:  evt.StartTime,
		Properties: props,
	}
}
