package internal

import (
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/stretchr/testify/assert"
)

func TestFormatLogBody_JSON(t *testing.T) {
	jsonBody := `{"level": "ERROR", "message": "Something went wrong"}`
	format := settingsproto.JSON

	result := FormatLogBody(jsonBody, &format)

	parsed, ok := result.(map[string]any)
	assert.True(t, ok, "result should be a map for valid JSON")
	assert.Equal(t, "ERROR", parsed["level"])
	assert.Equal(t, "Something went wrong", parsed["message"])
}

func TestFormatLogBody_JSON_InvalidFallback(t *testing.T) {
	invalidJSON := "not valid json"
	format := settingsproto.JSON

	result := FormatLogBody(invalidJSON, &format)

	// Should return raw string when JSON parsing fails
	str, ok := result.(string)
	assert.True(t, ok)
	assert.Equal(t, invalidJSON, str)
}

func TestFormatLogBody_YAML(t *testing.T) {
	yamlBody := `level: WARN
message: This is a warning`
	format := settingsproto.YAML

	result := FormatLogBody(yamlBody, &format)

	parsed, ok := result.(map[string]any)
	assert.True(t, ok, "result should be a map for valid YAML")
	assert.Equal(t, "WARN", parsed["level"])
	assert.Equal(t, "This is a warning", parsed["message"])
}

func TestFormatLogBody_PLAIN_TEXT(t *testing.T) {
	plainText := "This is just plain text"
	format := settingsproto.PLAINTEXT

	result := FormatLogBody(plainText, &format)

	str, ok := result.(string)
	assert.True(t, ok)
	assert.Equal(t, plainText, str)
}

func TestFormatLogBody_AUTO_JSON(t *testing.T) {
	jsonBody := `{"status": "ok", "code": 200}`
	format := settingsproto.AUTO

	result := FormatLogBody(jsonBody, &format)

	parsed, ok := result.(map[string]any)
	assert.True(t, ok, "AUTO format should detect and parse JSON")
	assert.Equal(t, "ok", parsed["status"])
	assert.Equal(t, float64(200), parsed["code"])
}

func TestFormatLogBody_AUTO_YAML(t *testing.T) {
	yamlBody := `key: value
nested:
  field: data`
	format := settingsproto.AUTO

	result := FormatLogBody(yamlBody, &format)

	parsed, ok := result.(map[string]any)
	assert.True(t, ok, "AUTO format should detect and parse YAML")
	assert.Equal(t, "value", parsed["key"])
}

func TestFormatLogBody_AUTO_PlainText(t *testing.T) {
	plainText := "Just plain log text"
	format := settingsproto.AUTO

	result := FormatLogBody(plainText, &format)

	str, ok := result.(string)
	assert.True(t, ok, "AUTO format should fall back to plain text for unparseable input")
	assert.Equal(t, plainText, str)
}

func TestFormatLogBody_NilFormat(t *testing.T) {
	jsonBody := `{"test": "data"}`

	result := FormatLogBody(jsonBody, nil)

	parsed, ok := result.(map[string]any)
	assert.True(t, ok, "nil format should default to AUTO and detect JSON")
	assert.Equal(t, "data", parsed["test"])
}
