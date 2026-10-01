package stslogsagentextension_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/extension/stslogsagentextension"
	"go.opentelemetry.io/collector/confmap"
)

const discoveryKey = "discovery_enabled"

func TestDiscoveryConfig(t *testing.T) {
	fields := map[string]any{
		discoveryKey:   true,
		"receiver_url": "https://receiver.example/stsAgent",
		"api_key":      "synthetic-config-test",
		"proxy_url":    "http://proxy.example:8080",
		"tls":          map[string]any{"insecure_skip_verify": true},
	}
	config, ok := stslogsagentextension.NewFactory().CreateDefaultConfig().(*stslogsagentextension.Config)
	if !ok {
		t.Fatal("wrong default config type")
	}
	if err := confmap.NewFromStringMap(fields).Unmarshal(config); err != nil {
		t.Fatal(err)
	}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryTuningIsNotConfigurable(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for key, value := range map[string]any{
			"query_timeout": "20s", "attempt_timeout": "5s", "max_attempts": 3,
			"initial_backoff": "500ms", "max_backoff": "2s",
			"poll_interval": "60s", "jitter": 0.2, "stable_observations": 3, "restart_cooldown": "10m",
		} {
			t.Run(fmt.Sprintf("discovery=%t/%s", enabled, key), func(t *testing.T) {
				config := stslogsagentextension.NewFactory().CreateDefaultConfig()
				err := confmap.NewFromStringMap(map[string]any{
					discoveryKey: enabled, key: value,
				}).Unmarshal(config)
				if err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("expected unsupported configuration key %q, got %v", key, err)
				}
			})
		}
	}
}

func TestFixedModeConfig(t *testing.T) {
	for _, tc := range []struct {
		discovery bool
		mode      string
		valid     bool
	}{
		{false, "", true}, {false, "promtail", true}, {false, "native", true},
		{false, "otel", false}, {true, "promtail", false}, {true, "native", false},
	} {
		t.Run(fmt.Sprintf("discovery=%t/%q", tc.discovery, tc.mode), func(t *testing.T) {
			config, ok := stslogsagentextension.NewFactory().CreateDefaultConfig().(*stslogsagentextension.Config)
			if !ok {
				t.Fatal("wrong default config type")
			}
			fields := map[string]any{
				discoveryKey: tc.discovery, "fixed_mode": tc.mode,
				"receiver_url": "https://receiver.example/stsAgent", "api_key": "synthetic-config-test",
			}
			if err := confmap.NewFromStringMap(fields).Unmarshal(config); err != nil {
				t.Fatal(err)
			}
			if err := config.Validate(); (err == nil) != tc.valid {
				t.Fatalf("valid=%t, got %v", tc.valid, err)
			}
		})
	}
}
