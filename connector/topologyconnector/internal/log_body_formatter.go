package internal

import (
	"encoding/json"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"gopkg.in/yaml.v3"
)

// FormatLogBody formats the raw log body string according to the specified format.
// For JSON and YAML formats, it returns a parsed map[string]any.
// For PLAIN_TEXT and AUTO formats that fall back to plain text, it returns the raw string.
func FormatLogBody(rawBody string, format *settingsproto.OtelInputLogFormat) any {
	if format == nil || *format == settingsproto.AUTO {
		return formatAuto(rawBody)
	}

	switch *format {
	case settingsproto.JSON:
		return formatJSON(rawBody)
	case settingsproto.YAML:
		return formatYAML(rawBody)
	case settingsproto.PLAINTEXT:
		return rawBody
	default:
		return rawBody
	}
}

// formatAuto attempts to parse the body as JSON first, then YAML, falling back to plain text.
func formatAuto(rawBody string) any {
	if result := formatJSON(rawBody); result != rawBody {
		return result
	}
	if result := formatYAML(rawBody); result != rawBody {
		return result
	}
	return rawBody
}

func formatJSON(rawBody string) any {
	var result map[string]any
	if err := json.Unmarshal([]byte(rawBody), &result); err == nil {
		return result
	}
	return rawBody
}

func formatYAML(rawBody string) any {
	var result map[string]any
	if err := yaml.Unmarshal([]byte(rawBody), &result); err == nil {
		return result
	}
	return rawBody
}
