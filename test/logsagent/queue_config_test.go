//nolint:goconst // Keep authored keys beside the effective-config assertions.
package logsagent_test

import (
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
)

func TestStockOTLPDisabledQueueMarshaling(t *testing.T) {
	for _, factory := range []exporter.Factory{otlphttpexporter.NewFactory(), otlpexporter.NewFactory()} {
		for _, tc := range []struct {
			name    string
			queue   any
			omitted bool
			valid   bool
		}{
			{name: "disabled", queue: map[string]any{"enabled": false}, valid: true},
			{name: "omitted", omitted: true},
			{name: "null"},
			{name: "empty", queue: map[string]any{}},
			{name: "enabled", queue: map[string]any{"enabled": true}},
		} {
			t.Run(factory.Type().String()+"/"+tc.name, func(t *testing.T) {
				for _, nested := range []bool{false, true} {
					values := renderConfig(t, defaultSettings())
					input := section(values, "exporters", "otlp_http/native")
					if tc.omitted {
						delete(input, "sending_queue")
					} else {
						input["sending_queue"] = tc.queue
					}
					cfg := factory.CreateDefaultConfig()
					if err := confmap.NewFromStringMap(input).Unmarshal(cfg); err != nil {
						t.Fatal(err)
					}
					var object any = cfg
					if nested {
						object = map[string]any{"exporters": map[string]component.Config{"otlp_http/native": cfg}}
					}
					effective := confmap.New()
					if err := effective.Marshal(object); err != nil {
						t.Fatal(err)
					}
					if nested {
						var err error
						effective, err = effective.Sub("exporters::otlp_http/native")
						if err != nil {
							t.Fatal(err)
						}
					}
					queue, present := effective.ToStringMap()["sending_queue"]
					if !present || (queue == nil) != tc.valid {
						t.Fatalf("unexpected effective queue: present=%t disabled=%t", present, queue == nil)
					}
					section(values, "exporters")["otlp_http/native"] = effective.ToStringMap()
					if _, err := logsagent.ValidateEffectivePipelineConfig(confmap.NewFromStringMap(values)); (err == nil) != tc.valid {
						t.Fatalf("effective config accepted=%t: %v", err == nil, err)
					}
				}
			})
		}
	}
}

func TestStartupNativeQueueDefaultsAreRejected(t *testing.T) {
	for _, transport := range []string{"native", "native_grpc"} {
		for _, tc := range []struct {
			name    string
			queue   any
			omitted bool
		}{
			{name: "omitted", omitted: true},
			{name: "null"},
			{name: "empty", queue: map[string]any{}},
		} {
			t.Run(transport+"/"+tc.name, func(t *testing.T) {
				f, _ := newTransportFixture(t, transport)
				p := f.start(func(config map[string]any) {
					id := "otlp_http/native"
					if transport == "native_grpc" {
						id = "otlp/native"
					}
					exporter := section(config, "exporters", id)
					if tc.omitted {
						delete(exporter, "sending_queue")
					} else {
						exporter["sending_queue"] = tc.queue
					}
				}, true)
				p.wait(8*time.Second, false)
				p.assertStartupReason("sending_queue")
				if len(f.backend.snapshot()) != 0 {
					t.Fatal("unsafe effective queue started collection")
				}
			})
		}
	}
}
