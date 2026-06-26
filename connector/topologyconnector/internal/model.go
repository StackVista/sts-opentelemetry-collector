package internal

import (
	"encoding/json"
	"fmt"
)

const (
	mapKeyAttributes = "attributes"
	mapKeyBody       = "body"
	mapKeyName       = "name"
	mapKeyKind       = "kind"
)

// -----------------------------
// Intermediate OTel model types used to cache relevant data for expressions at various levels.
// -----------------------------

type OtelDataType interface {
	ToMap() map[string]any
}

func stripSensitiveAttributes(attrs map[string]any) {
	// Strip internal routing attributes that must never be exposed as topology component tags/labels.
	delete(attrs, "sts_api_key")
	delete(attrs, "client_sts_api_key")
	delete(attrs, "server_sts_api_key")
}

type Resource struct {
	cachedMap map[string]any
}

func NewResource(attrs map[string]any) *Resource {
	stripSensitiveAttributes(attrs)
	return &Resource{
		cachedMap: map[string]any{
			mapKeyAttributes: attrs,
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
			mapKeyName:       name,
			"version":        version,
			mapKeyAttributes: attrs,
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

	// CollectionTimestampMs is the span's own collection time (end time, falling back to start time),
	// in milliseconds. 0 means the span carried no usable timestamp. Used as the topology operation's
	// collection_timestamp so the platform can order out-of-order operations per element.
	CollectionTimestampMs int64
}

func NewSpan(name, kind, statusCode, statusMessage string, attrs map[string]any) *Span {
	stripSensitiveAttributes(attrs)
	return &Span{
		cachedMap: map[string]any{
			mapKeyName:       name,
			mapKeyKind:       kind,
			"statusCode":     statusCode,
			"statusMessage":  statusMessage,
			mapKeyAttributes: attrs,
		},
	}
}

// WithCollectionTimestamp sets the span's collection time (ms) and returns it for chaining.
func (s *Span) WithCollectionTimestamp(collectionTimestampMs int64) *Span {
	s.CollectionTimestampMs = collectionTimestampMs
	return s
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
			mapKeyName:    name,
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

	// CollectionTimestampMs is the datapoint's own timestamp in milliseconds. 0 means none was set.
	CollectionTimestampMs int64
}

func NewDatapoint(attrs map[string]any) *Datapoint {
	stripSensitiveAttributes(attrs)
	return &Datapoint{
		cachedMap: map[string]any{
			mapKeyAttributes: attrs,
		},
	}
}

// WithCollectionTimestamp sets the datapoint's collection time (ms) and returns it for chaining.
func (d *Datapoint) WithCollectionTimestamp(collectionTimestampMs int64) *Datapoint {
	d.CollectionTimestampMs = collectionTimestampMs
	return d
}

func (d *Datapoint) ToMap() map[string]any {
	return d.cachedMap
}

type Log struct {
	cachedMap map[string]any

	// CollectionTimestampMs is the log record's collection time (observed time, falling back to event time),
	// in milliseconds. 0 means the record carried no usable timestamp.
	CollectionTimestampMs int64
}

// NewLog constructs a Log from a log record's data. The body is stored as-is:
// - If it's a structured map, it's used directly
// - If it's JSON bytes, it's unmarshaled to a map
// - Otherwise (including unstructured text), it's stored unparsed
func NewLog(
	name string, body any, attrs map[string]any,
) *Log {
	stripSensitiveAttributes(attrs)

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
			"eventName":      name,
			mapKeyBody:       processedBody,
			mapKeyAttributes: attrs,
		},
	}
}

// WithCollectionTimestamp sets the log's collection time (ms) and returns it for chaining.
func (l *Log) WithCollectionTimestamp(collectionTimestampMs int64) *Log {
	l.CollectionTimestampMs = collectionTimestampMs
	return l
}

func (l *Log) ToMap() map[string]any {
	return l.cachedMap
}
