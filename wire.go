package eventkit

import "time"

type LogEventsRequest struct {
	DistinctID string        `json:"distinct_id"`
	AppName    string        `json:"app_name"`
	AppVersion string        `json:"app_version"`
	Platform   string        `json:"platform"`
	Events     []EventRecord `json:"events"`
}

type EventRecord struct {
	ID         string      `json:"id"`
	Name       string      `json:"name"`
	StartTime  time.Time   `json:"start_time"`
	EndTime    time.Time   `json:"end_time"`
	Attributes []Attribute `json:"attributes,omitempty"`
	Metrics    []Metric    `json:"metrics,omitempty"`
}

type Attribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Metric struct {
	Key      string         `json:"key"`
	Count    *int64         `json:"count,omitempty"`
	Duration *time.Duration `json:"duration_ns,omitempty"`
}
