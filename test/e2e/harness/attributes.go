package harness

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
)

// convertToAttributesFromInterface is a helper to convert map[string]interface{} to []attribute.KeyValue
func convertToAttributesFromInterface(attrs map[string]interface{}) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		switch val := v.(type) {
		case string:
			out = append(out, attribute.String(k, val))
		case int:
			out = append(out, attribute.Int(k, val))
		case int64:
			out = append(out, attribute.Int64(k, val))
		case float64:
			out = append(out, attribute.Float64(k, val))
		case bool:
			out = append(out, attribute.Bool(k, val))
		default:
			out = append(out, attribute.String(k, fmt.Sprintf("%v", val)))
		}
	}
	return out
}

// helper to convert map[string]string to []attribute.KeyValue
func convertToAttributes(attrs map[string]string) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		out = append(out, attribute.String(k, v))
	}
	return out
}
