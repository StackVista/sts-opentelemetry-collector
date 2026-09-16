//nolint:testpackage,goconst // Verifies upstream pointer identity and keeps fixture values visible.
package otlpconfigexporter

import (
	"context"
	"reflect"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/exporter/xexporter"
)

type recordingFactory struct {
	upstreamFactory
	t       *testing.T
	want    component.Config
	wantID  component.ID
	signals map[string]int
}

func (f *recordingFactory) record(ctx context.Context, signal string, set exporter.Settings, cfg component.Config) {
	f.t.Helper()
	if cfg != f.want || ctx != f.t.Context() || set.ID != f.wantID {
		f.t.Fatal("signal creation changed the upstream config pointer, context or component ID")
	}
	f.signals[signal]++
}

func (f *recordingFactory) CreateTraces(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	f.record(ctx, "traces", set, cfg)
	return f.upstreamFactory.CreateTraces(ctx, set, cfg)
}

func (f *recordingFactory) CreateMetrics(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	f.record(ctx, "metrics", set, cfg)
	return f.upstreamFactory.CreateMetrics(ctx, set, cfg)
}

func (f *recordingFactory) CreateLogs(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	f.record(ctx, "logs", set, cfg)
	return f.upstreamFactory.CreateLogs(ctx, set, cfg)
}

func (f *recordingFactory) CreateProfiles(ctx context.Context, set exporter.Settings, cfg component.Config) (xexporter.Profiles, error) {
	f.record(ctx, "profiles", set, cfg)
	return f.upstreamFactory.CreateProfiles(ctx, set, cfg)
}

func TestFactoryDelegatesAllSignalsAndAliases(t *testing.T) {
	for _, transport := range factoryCases() {
		t.Run(transport.name, func(t *testing.T) {
			wrapped, ok := transport.wrapped().(*factory)
			if !ok {
				t.Fatal("expected wrapped factory")
			}
			stock := wrapped.upstreamFactory
			if wrapped.Type() != stock.Type() || wrapped.DeprecatedAlias() != stock.DeprecatedAlias() ||
				wrapped.TracesStability() != stock.TracesStability() ||
				wrapped.MetricsStability() != stock.MetricsStability() ||
				wrapped.LogsStability() != stock.LogsStability() ||
				wrapped.ProfilesStability() != stock.ProfilesStability() {
				t.Fatal("factory metadata differs from upstream")
			}
			for _, typ := range []component.Type{stock.Type(), stock.DeprecatedAlias()} {
				t.Run(typ.String(), func(t *testing.T) {
					cfg := wrapped.CreateDefaultConfig()
					input := configInput()
					input["sending_queue"] = map[string]any{"enabled": false}
					decodeConfig(t, cfg, input)
					original := wrappedConfig(t, cfg).upstream
					ctx := t.Context()
					set := exportertest.NewNopSettings(typ)
					recorder := &recordingFactory{
						upstreamFactory: stock, t: t, want: original,
						wantID: set.ID, signals: make(map[string]int),
					}
					wrapped.upstreamFactory = recorder
					cases := []struct {
						name   string
						create func() (component.Component, error)
						stock  func() (component.Component, error)
					}{
						{"traces",
							func() (component.Component, error) { return wrapped.CreateTraces(ctx, set, cfg) },
							func() (component.Component, error) { return stock.CreateTraces(ctx, set, original) }},
						{"metrics",
							func() (component.Component, error) { return wrapped.CreateMetrics(ctx, set, cfg) },
							func() (component.Component, error) { return stock.CreateMetrics(ctx, set, original) }},
						{"logs",
							func() (component.Component, error) { return wrapped.CreateLogs(ctx, set, cfg) },
							func() (component.Component, error) { return stock.CreateLogs(ctx, set, original) }},
						{"profiles",
							func() (component.Component, error) { return wrapped.CreateProfiles(ctx, set, cfg) },
							func() (component.Component, error) { return stock.CreateProfiles(ctx, set, original) }},
					}
					for _, signal := range cases {
						t.Run(signal.name, func(t *testing.T) {
							got, err := signal.create()
							if err != nil {
								t.Fatal(err)
							}
							want, err := signal.stock()
							if err != nil {
								t.Fatal(err)
							}
							if reflect.TypeOf(got) != reflect.TypeOf(want) || recorder.signals[signal.name] != 1 {
								t.Fatal("signal factory did not return the original upstream implementation")
							}
							if err := got.Shutdown(ctx); err != nil {
								t.Fatal(err)
							}
							if err := want.Shutdown(ctx); err != nil {
								t.Fatal(err)
							}
						})
					}
				})
			}
		})
	}
}
