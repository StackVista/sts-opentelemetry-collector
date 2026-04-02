package internal_test

import (
	"encoding/json"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stretchr/testify/require"
)

func TestNewLog_StructuredMapBody(t *testing.T) {
	body := map[string]any{
		"key1": "value1",
		"key2": 42,
	}
	attrs := map[string]any{"attr1": "test"}

	log := internal.NewLog("test_event", body, attrs)

	if log == nil {
		t.Fatal("NewLog returned nil")
	}

	m := log.ToMap()
	if m["name"] != "test_event" {
		t.Errorf("expected name 'test_event', got %v", m["name"])
	}

	bodyMap, ok := m["body"].(map[string]any)
	if !ok {
		t.Fatalf("expected body to be map, got %T", m["body"])
	}
	if bodyMap["key1"] != "value1" || bodyMap["key2"] != 42 {
		t.Errorf("body map not preserved correctly: %v", bodyMap)
	}
}

func TestNewLog_ValidJSONBytes(t *testing.T) {
	jsonBody := map[string]any{
		"field1": "value1",
		"field2": 123,
	}
	bodyBytes, _ := json.Marshal(jsonBody) //nolint:errchkjson
	attrs := map[string]any{"attr1": "test"}
	log := internal.NewLog("test_event", bodyBytes, attrs)
	if log == nil {
		t.Fatal("NewLog returned nil")
	}
	m := log.ToMap()
	bodyMap, ok := m["body"].(map[string]any)
	if !ok {
		t.Fatalf("expected body to be unmarshaled map, got %T", m["body"])
	}

	if bodyMap["field1"] != "value1" {
		t.Errorf("JSON bytes not unmarshaled correctly: %v", bodyMap)
	}
}

func TestNewLog_InvalidJSON(t *testing.T) {
	bodyBytes := []byte("this is not json, just plain text")
	attrs := map[string]any{"attr1": "test"}

	log := internal.NewLog("test_event", bodyBytes, attrs)

	if log == nil {
		t.Fatal("NewLog returned nil")
	}

	m := log.ToMap()
	bodyStr, ok := m["body"].(string)
	if !ok {
		t.Fatalf("expected body to be string, got %T", m["body"])
	}

	if bodyStr != "this is not json, just plain text" {
		t.Errorf("expected unparsed body as string, got %s", bodyStr)
	}
}

func TestNewLog_StringBody(t *testing.T) {
	body := "plain text log message"
	attrs := map[string]any{"attr1": "test"}

	log := internal.NewLog("test_event", body, attrs)

	if log == nil {
		t.Fatal("NewLog returned nil")
	}

	m := log.ToMap()
	bodyStr, ok := m["body"].(string)
	if !ok {
		t.Fatalf("expected body to be string, got %T", m["body"])
	}

	if bodyStr != "plain text log message" {
		t.Errorf("expected body to be preserved, got %s", bodyStr)
	}
}

func TestNewLog_EmptyBody(t *testing.T) {
	log := internal.NewLog("test_event", "", map[string]any{})

	if log == nil {
		t.Fatal("NewLog returned nil")
	}

	m := log.ToMap()
	if m["body"] == nil {
		t.Error("expected body to be stored even if empty")
	}
}

func TestNewResource_stripsSensitiveAttributes(t *testing.T) {
	attrs := map[string]any{
		"service.name":           "my-service",
		"service.namespace":      "default",
		"sts_api_key":            "SECRET_KEY",
		"client_sts_api_key":     "CLIENT_SECRET",
		"server_sts_api_key":     "SERVER_SECRET",
		"deployment.environment": "production",
	}

	resource := internal.NewResource(attrs)
	resourceMap := resource.ToMap()
	attributes, ok := resourceMap["attributes"].(map[string]any)
	require.True(t, ok, "attributes should be a map[string]any")

	require.Equal(t, "my-service", attributes["service.name"])
	require.Equal(t, "default", attributes["service.namespace"])
	require.Equal(t, "production", attributes["deployment.environment"])

	_, hasAPIKey := attributes["sts_api_key"]
	require.False(t, hasAPIKey, "sts_api_key should be stripped from resource attributes")

	_, hasClientKey := attributes["client_sts_api_key"]
	require.False(t, hasClientKey, "client_sts_api_key should be stripped from resource attributes")

	_, hasServerKey := attributes["server_sts_api_key"]
	require.False(t, hasServerKey, "server_sts_api_key should be stripped from resource attributes")
}

func TestNewSpan_stripsSensitiveAttributes(t *testing.T) {
	attrs := map[string]any{
		"http.method":        "GET",
		"http.status_code":   200,
		"sts_api_key":        "SECRET_KEY",
		"client_sts_api_key": "CLIENT_SECRET",
		"server_sts_api_key": "SERVER_SECRET",
	}

	span := internal.NewSpan("test_span", "client", "OK", "", attrs)
	spanMap := span.ToMap()
	attributes, ok := spanMap["attributes"].(map[string]any)
	require.True(t, ok, "attributes should be a map[string]any")

	require.Equal(t, "GET", attributes["http.method"])
	require.Equal(t, 200, attributes["http.status_code"])

	_, hasAPIKey := attributes["sts_api_key"]
	require.False(t, hasAPIKey, "sts_api_key should be stripped from span attributes")

	_, hasClientKey := attributes["client_sts_api_key"]
	require.False(t, hasClientKey, "client_sts_api_key should be stripped from span attributes")

	_, hasServerKey := attributes["server_sts_api_key"]
	require.False(t, hasServerKey, "server_sts_api_key should be stripped from span attributes")
}

func TestNewDatapoint_stripsSensitiveAttributes(t *testing.T) {
	attrs := map[string]any{
		"cpu":                "0.5",
		"sts_api_key":        "SECRET_KEY",
		"client_sts_api_key": "CLIENT_SECRET",
		"server_sts_api_key": "SERVER_SECRET",
	}

	datapoint := internal.NewDatapoint(attrs)
	datapointMap := datapoint.ToMap()
	attributes, ok := datapointMap["attributes"].(map[string]any)
	require.True(t, ok, "attributes should be a map[string]any")

	require.Equal(t, "0.5", attributes["cpu"])

	_, hasAPIKey := attributes["sts_api_key"]
	require.False(t, hasAPIKey, "sts_api_key should be stripped from datapoint attributes")

	_, hasClientKey := attributes["client_sts_api_key"]
	require.False(t, hasClientKey, "client_sts_api_key should be stripped from datapoint attributes")

	_, hasServerKey := attributes["server_sts_api_key"]
	require.False(t, hasServerKey, "server_sts_api_key should be stripped from datapoint attributes")
}

func TestNewLog_stripsSensitiveAttributes(t *testing.T) {
	attrs := map[string]any{
		"log.level":          "info",
		"sts_api_key":        "SECRET_KEY",
		"client_sts_api_key": "CLIENT_SECRET",
		"server_sts_api_key": "SERVER_SECRET",
	}

	log := internal.NewLog("test_log", "log message", attrs)
	logMap := log.ToMap()
	attributes, ok := logMap["attributes"].(map[string]any)
	require.True(t, ok, "attributes should be a map[string]any")

	require.Equal(t, "info", attributes["log.level"])

	_, hasAPIKey := attributes["sts_api_key"]
	require.False(t, hasAPIKey, "sts_api_key should be stripped from log attributes")

	_, hasClientKey := attributes["client_sts_api_key"]
	require.False(t, hasClientKey, "client_sts_api_key should be stripped from log attributes")

	_, hasServerKey := attributes["server_sts_api_key"]
	require.False(t, hasServerKey, "server_sts_api_key should be stripped from log attributes")
}
