package stsk8slogsexporter_test

import (
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8slogsexporter"
	"go.opentelemetry.io/collector/confmap"
)

func TestMarshalDisabledQueueStaysDisabled(t *testing.T) {
	cfg := stsk8slogsexporter.NewFactory().CreateDefaultConfig()
	input := confmap.NewFromStringMap(map[string]any{
		"sending_queue": map[string]any{"enabled": false},
		"api_key":       "synthetic-secret-not-for-serialization",
	})
	if err := input.Unmarshal(cfg); err != nil {
		t.Fatal(err)
	}
	effective := confmap.New()
	if err := effective.Marshal(cfg); err != nil {
		t.Fatal(err)
	}
	if effective.Get("sending_queue") != nil {
		t.Fatal("config marshaling enabled a disabled queue")
	}
	if effective.Get("api_key") == "synthetic-secret-not-for-serialization" {
		t.Fatal("config marshaling exposed the credential")
	}
}
