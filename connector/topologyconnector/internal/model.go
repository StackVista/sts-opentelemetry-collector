package internal

import (
	"encoding/json"
	"fmt"
)

// -----------------------------
// Intermediate OTel model types used to cache relevant data for expressions at various levels.
// -----------------------------

type OtelDataType interface {
	ToMap() map[string]any
}

type Resource struct {
	cachedMap map[string]any
}

func NewResource(attrs map[string]any) *Resource {
	// Strip internal routing attributes that must never be exposed as topology component tags/labels.
	delete(attrs, "sts_api_key")
	delete(attrs, "client_sts_api_key")
	delete(attrs, "server_sts_api_key")
	return &Resource{
		cachedMap: map[string]any{
			"attributes": attrs,
		},
	}
}

func (r *Resource) ToMap() map[string]any {
	return r.cachedMap
}

type Scope struct {
	// not storing individuals fields on the struct as we don't need them (at the moment),
	// so they'll just use unnecessary memory

	cachedMap map[string]any
}

func NewScope(name, version string, attrs map[string]any) *Scope {
	return &Scope{
		cachedMap: map[string]any{
			"name":       name,
			"version":    version,
			"attributes": attrs,
		},
	}
}

func (s *Scope) ToMap() map[string]any {
	return s.cachedMap
}

type Span struct {
	// not storing individual fields on the struct as we don't need them (at the moment),
	// so they'll just use unnecessary memory

	cachedMap map[string]any
}

func NewSpan(name, kind, statusCode, statusMessage string, attrs map[string]any) *Span {
	return &Span{
		cachedMap: map[string]any{
			"name":          name,
			"kind":          kind,
			"statusCode":    statusCode,
			"statusMessage": statusMessage,
			"attributes":    attrs,
		},
	}
}

func (s *Span) ToMap() map[string]any {
	return s.cachedMap
}

type Metric struct {
	cachedMap map[string]any
}

func NewMetric(name, description, unit string) *Metric {
	return &Metric{
		cachedMap: map[string]any{
			"name":        name,
			"description": description,
			"unit":        unit,
		},
	}
}

func (m *Metric) ToMap() map[string]any {
	return m.cachedMap
}

type Datapoint struct {
	cachedMap map[string]any
}

func NewDatapoint(attrs map[string]any) *Datapoint {
	return &Datapoint{
		cachedMap: map[string]any{
			"attributes": attrs,
		},
	}
}

func (d *Datapoint) ToMap() map[string]any {
	return d.cachedMap
}

type Log struct {
	cachedMap map[string]any
}

// NewLog constructs a Log from a log record's data. The body is stored as-is:
// - If it's a structured map, it's used directly
// - If it's JSON bytes, it's unmarshaled to a map
// - Otherwise (including unstructured text), it's stored unparsed
func NewLog(
	name string, body any, attrs map[string]any,
) *Log {
	var processedBody any

	// Try to use body as a map directly
	if m, ok := body.(map[string]any); ok {
		processedBody = m
	} else if b, ok := body.([]byte); ok {
		// Try to unmarshal JSON bytes, fall back to string if not valid JSON
		var bodyMap map[string]any
		if err := json.Unmarshal(b, &bodyMap); err != nil {
			processedBody = string(b)
		} else {
			processedBody = bodyMap
		}
	} else if s, ok := body.(string); ok {
		processedBody = s
	} else {
		// For other types, convert to string representation
		processedBody = fmt.Sprintf("%v", body)
	}

	return &Log{
		cachedMap: map[string]any{
			"name":       name,
			"body":       processedBody,
			"attributes": attrs,
		},
	}
}

func (l *Log) ToMap() map[string]any {
	return l.cachedMap
}
