//nolint:testpackage,goconst // Checks typed configs directly and keeps fixture values visible.
package otlpconfigexporter

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
)

type factoryCase struct {
	name    string
	stock   func() exporter.Factory
	wrapped func() exporter.Factory
}

func factoryCases() []factoryCase {
	return []factoryCase{
		{"http", otlphttpexporter.NewFactory, NewHTTPFactory},
		{"grpc", otlpexporter.NewFactory, NewGRPCFactory},
	}
}

func configInput() map[string]any {
	return map[string]any{
		"endpoint": "https://localhost:4317",
		"headers":  map[string]any{"x-synthetic": "synthetic-test-value"},
		"auth":     map[string]any{"authenticator": "bearertokenauth/native"},
		"tls": map[string]any{
			"ca_file": "/tmp/fixture-ca.pem", "cert_file": "/tmp/fixture-cert.pem",
			"key_file": "/tmp/fixture-key.pem", "server_name_override": "fixture.invalid",
		},
	}
}

func decodeConfig(t *testing.T, cfg component.Config, input map[string]any) {
	t.Helper()
	if err := confmap.NewFromStringMap(input).Unmarshal(cfg); err != nil {
		t.Fatal(err)
	}
}

func wrappedConfig(t *testing.T, cfg component.Config) *config {
	t.Helper()
	wrapped, ok := cfg.(*config)
	if !ok {
		t.Fatal("expected a serialization wrapper")
	}
	return wrapped
}

func marshalConfig(t *testing.T, cfg component.Config, nested bool) *confmap.Conf {
	t.Helper()
	result := confmap.New()
	var input any = cfg
	if nested {
		input = map[string]any{"exporters": map[string]component.Config{"otlp/test": cfg}}
	}
	if err := result.Marshal(input); err != nil {
		t.Fatal(err)
	}
	if nested {
		var err error
		result, err = result.Sub("exporters::otlp/test")
		if err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func TestQueueSizerSerialization(t *testing.T) {
	for _, transport := range factoryCases() {
		for _, sizer := range []string{"requests", "items", "bytes"} {
			for _, batchSizer := range []string{"", "items", "bytes"} {
				t.Run(transport.name+"/"+sizer+"/batch="+batchSizer, func(t *testing.T) {
					input := configInput()
					queue := map[string]any{
						"sizer": sizer, "queue_size": 100, "num_consumers": 2,
						"block_on_overflow": true, "wait_for_result": true,
					}
					if batchSizer != "" {
						queue["batch"] = map[string]any{
							"sizer": batchSizer, "flush_timeout": "1s", "min_size": 10, "max_size": 50,
							"partition": map[string]any{"metadata_keys": []any{"x-tenant"}},
						}
					}
					input["sending_queue"] = queue
					cfg := transport.wrapped().CreateDefaultConfig()
					stock := transport.stock().CreateDefaultConfig()
					decodeConfig(t, cfg, input)
					decodeConfig(t, stock, input)
					if err := confmap.Validate(cfg); err != nil {
						t.Fatal(err)
					}
					for _, nested := range []bool{false, true} {
						got := marshalConfig(t, cfg, nested)
						if got.Get("sending_queue::sizer") != sizer {
							t.Fatal("queue sizer did not retain its actual typed value")
						}
						if batchSizer != "" && got.Get("sending_queue::batch::sizer") != batchSizer {
							t.Fatal("batch sizer did not retain its actual typed value")
						}
						expected := marshalConfig(t, stock, nested)
						if !reflect.DeepEqual(expected.Get("sending_queue::sizer"), map[string]any{}) {
							t.Fatal("pinned upstream sizer serialization defect changed")
						}
						for _, path := range []string{"sending_queue::sizer", "sending_queue::batch::sizer"} {
							got.Delete(path)
							expected.Delete(path)
						}
						if !reflect.DeepEqual(got.ToStringMap(), expected.ToStringMap()) {
							t.Fatal("serialization changed fields other than sizers")
						}
					}
					if !reflect.DeepEqual(wrappedConfig(t, cfg).upstream, stock) {
						t.Fatal("marshaling mutated runtime configuration or authentication")
					}
				})
			}
		}
	}
}

func TestOptionalQueueAndBatchSerialization(t *testing.T) {
	for _, transport := range factoryCases() {
		for _, tc := range []struct {
			name      string
			input     map[string]any
			wantQueue bool
			wantBatch bool
		}{
			{"defaults", nil, true, false},
			{"null queue retains upstream defaults", map[string]any{"sending_queue": nil}, true, false},
			{"disabled queue", map[string]any{"sending_queue": map[string]any{"enabled": false}}, false, false},
			{"disabled queue with batch", map[string]any{"sending_queue": map[string]any{
				"enabled": false, "sizer": "bytes", "batch": map[string]any{"sizer": "bytes"},
			}}, false, false},
			{"null batch enables upstream batch defaults", map[string]any{"sending_queue": map[string]any{"batch": nil}}, true, true},
			{"empty batch enables defaults", map[string]any{"sending_queue": map[string]any{"batch": map[string]any{}}}, true, true},
			{"disabled batch", map[string]any{"sending_queue": map[string]any{
				"sizer": "requests", "batch": map[string]any{"enabled": false, "sizer": "items"},
			}}, true, false},
		} {
			t.Run(transport.name+"/"+tc.name, func(t *testing.T) {
				cfg := transport.wrapped().CreateDefaultConfig()
				stock := transport.stock().CreateDefaultConfig()
				input := configInput()
				for key, value := range tc.input {
					input[key] = value
				}
				decodeConfig(t, cfg, input)
				decodeConfig(t, stock, input)
				for _, nested := range []bool{false, true} {
					got := marshalConfig(t, cfg, nested)
					if (got.Get("sending_queue") != nil) != tc.wantQueue {
						t.Fatal("queue enabled state changed")
					}
					if tc.wantQueue {
						if got.Get("sending_queue::sizer") != "requests" ||
							(got.Get("sending_queue::batch") != nil) != tc.wantBatch {
							t.Fatal("queue or batch defaults changed")
						}
						if tc.wantBatch && got.Get("sending_queue::batch::sizer") != "items" {
							t.Fatal("batch default sizer changed")
						}
					}
					expected := marshalConfig(t, stock, nested)
					for _, path := range []string{"sending_queue::sizer", "sending_queue::batch::sizer"} {
						got.Delete(path)
						expected.Delete(path)
					}
					if !reflect.DeepEqual(got.ToStringMap(), expected.ToStringMap()) {
						t.Fatal("upstream defaults or disabled configuration changed")
					}
				}
			})
		}
	}
}

func TestMarshalMasksCredentials(t *testing.T) {
	for _, transport := range factoryCases() {
		t.Run(transport.name, func(t *testing.T) {
			cfg := transport.wrapped().CreateDefaultConfig()
			stock := transport.stock().CreateDefaultConfig()
			decodeConfig(t, cfg, configInput())
			decodeConfig(t, stock, configInput())
			for _, nested := range []bool{false, true} {
				marshaled := marshalConfig(t, cfg, nested)
				encoded, err := json.Marshal(marshaled.ToStringMap())
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(encoded), "synthetic-test-value") ||
					!strings.Contains(string(encoded), "[REDACTED]") {
					t.Fatal("serialized configuration exposed an opaque header")
				}
			}
			if !reflect.DeepEqual(wrappedConfig(t, cfg).upstream, stock) {
				t.Fatal("redaction altered runtime authentication")
			}
		})
	}
}

func TestValidationAndUnmarshalDelegate(t *testing.T) {
	for _, transport := range factoryCases() {
		for _, invalid := range []map[string]any{
			{"unknown_field": true},
			{"sending_queue": map[string]any{"sizer": map[string]any{}}},
			{"sending_queue": map[string]any{"num_consumers": 0}},
			{"sending_queue": map[string]any{"batch": map[string]any{"sizer": "requests"}}},
			{"timeout": "-1s"},
			{"retry_on_failure": map[string]any{"max_elapsed_time": "-1s"}},
			{"endpoint": ""},
		} {
			input := configInput()
			for key, value := range invalid {
				input[key] = value
			}
			cfg := transport.wrapped().CreateDefaultConfig()
			stock := transport.stock().CreateDefaultConfig()
			got := confmap.NewFromStringMap(input).Unmarshal(cfg)
			want := confmap.NewFromStringMap(input).Unmarshal(stock)
			if (got == nil) != (want == nil) {
				t.Fatal("unmarshal acceptance differs from upstream")
			}
			if got == nil {
				got, want = confmap.Validate(cfg), confmap.Validate(stock)
				if (got == nil) != (want == nil) || (got != nil && got.Error() != want.Error()) {
					t.Fatal("recursive validation differs from upstream")
				}
			}
		}
	}
}
