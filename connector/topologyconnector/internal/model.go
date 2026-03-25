package internal

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
	rawBody   string         // Immutable original raw body
	cachedMap map[string]any // For ToMap, body field is updated to formatted version
}

func NewLog(
	name, severityNumber, severityText string, attrs map[string]any, bodyRaw string, traceId, spanId string,
) *Log {
	return &Log{
		rawBody: bodyRaw,
		cachedMap: map[string]any{
			"name":           name,
			"severityNumber": severityNumber,
			"severityText":   severityText,
			"attributes":     attrs,
			"body":           bodyRaw,
			"traceId":        traceId,
			"spanId":         spanId,
		},
	}
}

// Body returns the raw log body string (immutable).
func (l *Log) Body() string {
	return l.rawBody
}

// IsFormatted checks if the body in cachedMap has been formatted (differs from raw).
func (l *Log) IsFormatted() bool {
	current := l.cachedMap["body"]
	// If current body is the raw string by value, it hasn't been formatted
	if str, ok := current.(string); ok && str == l.rawBody {
		return false
	}
	return true
}

// SetFormattedBody updates the body in cachedMap to the formatted version,
// making it available to CEL expressions through ToMap().
func (l *Log) SetFormattedBody(formatted any) {
	l.cachedMap["body"] = formatted
}

func (l *Log) ToMap() map[string]any {
	return l.cachedMap
}
