package stsk8slogsexporter_test

import (
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8slogsexporter"
	"go.opentelemetry.io/collector/confmap"
)

func TestMarshalPreservesQueueUnits(t *testing.T) {
	for _, unit := range []string{"requests", "items", "bytes"} {
		t.Run(unit, func(t *testing.T) {
			cfg := stsk8slogsexporter.NewFactory().CreateDefaultConfig()
			input := confmap.NewFromStringMap(map[string]any{
				"sending_queue": map[string]any{"sizer": unit},
				"api_key":       "synthetic-secret-not-for-serialization",
			})
			if err := input.Unmarshal(cfg); err != nil {
				t.Fatal(err)
			}
			effective := confmap.New()
			if err := effective.Marshal(cfg); err != nil {
				t.Fatal(err)
			}
			if got := effective.Get("sending_queue::sizer"); got != unit {
				t.Fatalf("serialized unit %v, want %s", got, unit)
			}
			if effective.Get("api_key") == "synthetic-secret-not-for-serialization" {
				t.Fatal("config marshaling exposed the credential")
			}
		})
	}
}

func TestMarshalDisabledQueueStaysDisabled(t *testing.T) {
	cfg := stsk8slogsexporter.NewFactory().CreateDefaultConfig()
	input := confmap.NewFromStringMap(map[string]any{
		"sending_queue": map[string]any{"enabled": false},
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
}
