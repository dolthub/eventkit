package ga4

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/dolthub/eventkit"
)

const (
	DefaultEndpoint         = "https://www.google-analytics.com/mp/collect"
	DefaultValidateEndpoint = "https://www.google-analytics.com/debug/mp/collect"
	DefaultRequestTimeout   = 10 * time.Second
	MaxEventsPerRequest     = 25
	MaxParamsPerEvent       = 25
	MaxEventNameLen         = 40
	MaxParamNameLen         = 40
	MaxParamValueLen        = 100
)

type Config struct {
	MeasurementID  string
	APISecret      string
	Endpoint       string
	HTTPClient     *http.Client
	RequestTimeout time.Duration
	Validate       bool
}

type Emitter struct {
	endpoint string
	mid      string
	secret   string
	client   *http.Client
}

func New(cfg Config) (*Emitter, error) {
	endpoint := cfg.Endpoint
	if endpoint == "" {
		if cfg.Validate {
			endpoint = DefaultValidateEndpoint
		} else {
			endpoint = DefaultEndpoint
		}
	}
	client := cfg.HTTPClient
	if client == nil {
		timeout := cfg.RequestTimeout
		if timeout == 0 {
			timeout = DefaultRequestTimeout
		}
		client = &http.Client{Timeout: timeout}
	}
	return &Emitter{
		endpoint: endpoint,
		mid:      cfg.MeasurementID,
		secret:   cfg.APISecret,
		client:   client,
	}, nil
}

func (e *Emitter) Send(ctx context.Context, req *eventkit.LogEventsRequest) error {
	if req == nil || len(req.Events) == 0 {
		return nil
	}
	for start := 0; start < len(req.Events); start += MaxEventsPerRequest {
		end := start + MaxEventsPerRequest
		if end > len(req.Events) {
			end = len(req.Events)
		}
		if err := e.post(ctx, buildPayload(req, req.Events[start:end])); err != nil {
			return err
		}
	}
	return nil
}

func (e *Emitter) post(ctx context.Context, payload payload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	url := e.endpoint
	var params []string
	if e.mid != "" {
		params = append(params, "measurement_id="+e.mid)
	}
	if e.secret != "" {
		params = append(params, "api_secret="+e.secret)
	}
	if len(params) > 0 {
		url = url + "?" + strings.Join(params, "&")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("ga4: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	return nil
}

type payload struct {
	ClientID        string                  `json:"client_id"`
	TimestampMicros int64                   `json:"timestamp_micros,omitempty"`
	UserProperties  map[string]userProperty `json:"user_properties,omitempty"`
	Events          []event                 `json:"events"`
}

type userProperty struct {
	Value string `json:"value"`
}

type event struct {
	Name   string                 `json:"name"`
	Params map[string]interface{} `json:"params,omitempty"`
}

func buildPayload(req *eventkit.LogEventsRequest, records []eventkit.EventRecord) payload {
	p := payload{
		ClientID:        req.DistinctID,
		TimestampMicros: time.Now().UnixMicro(),
	}
	if req.AppName != "" || req.AppVersion != "" || req.Platform != "" {
		p.UserProperties = make(map[string]userProperty)
		if req.AppName != "" {
			p.UserProperties["app_name"] = userProperty{Value: truncate(req.AppName, MaxParamValueLen)}
		}
		if req.AppVersion != "" {
			p.UserProperties["app_version"] = userProperty{Value: truncate(req.AppVersion, MaxParamValueLen)}
		}
		if req.Platform != "" {
			p.UserProperties["platform"] = userProperty{Value: truncate(req.Platform, MaxParamValueLen)}
		}
	}
	sessionID := fmt.Sprintf("%d", time.Now().Unix())
	p.Events = make([]event, 0, len(records))
	for _, r := range records {
		p.Events = append(p.Events, buildEvent(r, sessionID))
	}
	return p
}

func buildEvent(r eventkit.EventRecord, sessionID string) event {
	params := make(map[string]interface{})
	params["session_id"] = sessionID
	params["engagement_time_msec"] = int64(1)
	params["event_id"] = r.ID
	if !r.EndTime.IsZero() && !r.StartTime.IsZero() {
		params["duration_ms"] = float64(r.EndTime.Sub(r.StartTime).Nanoseconds()) / 1e6
	}
	for _, a := range r.Attributes {
		setParam(params, a.Key, truncate(a.Value, MaxParamValueLen))
	}
	for _, m := range r.Metrics {
		switch {
		case m.Count != nil:
			setParam(params, m.Key, *m.Count)
		case m.Duration != nil:
			setParam(params, fmt.Sprintf("%s_ms", m.Key), float64(m.Duration.Nanoseconds())/1e6)
		}
	}
	return event{
		Name:   sanitizeName(r.Name),
		Params: params,
	}
}

func setParam(params map[string]interface{}, key string, value interface{}) {
	if len(params) >= MaxParamsPerEvent {
		return
	}
	k := sanitizeKey(key)
	if k == "" {
		return
	}
	params[k] = value
}

func sanitizeName(s string) string {
	out := sanitizeKey(s)
	if len(out) > MaxEventNameLen {
		out = out[:MaxEventNameLen]
	}
	if out == "" {
		return "event"
	}
	return out
}

func sanitizeKey(s string) string {
	var b strings.Builder
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			if i == 0 {
				b.WriteRune('_')
			}
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := b.String()
	if len(out) > MaxParamNameLen {
		out = out[:MaxParamNameLen]
	}
	return out
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
