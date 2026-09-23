package stslogsrouteconnector_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	route "github.com/stackvista/sts-opentelemetry-collector/connector/stslogsrouteconnector"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type controllerStub struct {
	component.StartFunc
	component.ShutdownFunc
	mu       sync.Mutex
	mode     logsagent.Mode
	deadline time.Time
	bound    time.Duration
	observer logsagent.ExportObserver
	err      error
}

func (s *controllerStub) SelectedMode() logsagent.Mode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode
}

func (s *controllerStub) DrainDeadline() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deadline
}

func (s *controllerStub) RetryBound() time.Duration { return s.bound }

func (s *controllerStub) RegisterExportObserver(observer logsagent.ExportObserver) error {
	s.observer = observer
	return s.err
}

type hostStub map[component.ID]component.Component

func (h hostStub) GetExtensions() map[component.ID]component.Component { return h }

func defaults(t *testing.T) *route.Config {
	t.Helper()
	cfg, ok := route.NewFactory().CreateDefaultConfig().(*route.Config)
	require.True(t, ok)
	cfg.OTELNativePipeline = pipeline.NewIDWithName(pipeline.SignalLogs, "otel_native")
	return cfg
}

func settings(provider *sdkmetric.MeterProvider) connector.Settings {
	telemetry := componenttest.NewNopTelemetrySettings()
	if provider != nil {
		telemetry.MeterProvider = provider
	}
	return connector.Settings{
		ID: component.MustNewIDWithName("stslogsroute", "logs"), TelemetrySettings: telemetry,
	}
}

func newRoute(
	t *testing.T, cfg *route.Config, ctrl *controllerStub, promtail, native consumer.Logs,
) (connector.Logs, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	router := connector.NewLogsRouter(map[pipeline.ID]consumer.Logs{
		cfg.PromtailPipeline: promtail, cfg.OTELNativePipeline: native,
	})
	c, err := route.NewFactory().CreateLogsToLogs(context.Background(), settings(provider), cfg, router)
	require.NoError(t, err)
	require.NoError(t, c.Start(context.Background(), hostStub{cfg.ControllerExtension: ctrl}))
	t.Cleanup(func() { require.NoError(t, c.Shutdown(context.Background())) })
	return c, reader
}

func logsConsumer(t *testing.T, fn consumer.ConsumeLogsFunc) consumer.Logs {
	t.Helper()
	c, err := consumer.NewLogs(fn)
	require.NoError(t, err)
	return c
}

func logsData() plog.Logs {
	data := plog.NewLogs()
	resource := data.ResourceLogs().AppendEmpty()
	resource.Resource().Attributes().PutStr("k8s.cluster.name", "fixture")
	scope := resource.ScopeLogs().AppendEmpty()
	scope.Scope().SetName("fixture")
	scope.LogRecords().AppendEmpty().Body().SetStr("record")
	return data
}

func metricSum(t *testing.T, reader *sdkmetric.ManualReader, name string, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &data))
	var total int64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				matches := true
				for _, attr := range attrs {
					v, exists := dp.Attributes.Value(attr.Key)
					matches = matches && exists && v == attr.Value
				}
				if matches {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func TestFixedRoute(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			ctrl := &controllerStub{mode: mode, bound: time.Second}
			var promtail, native atomic.Int64
			c, reader := newRoute(t, defaults(t), ctrl,
				logsConsumer(t, func(context.Context, plog.Logs) error { promtail.Add(1); return nil }),
				logsConsumer(t, func(context.Context, plog.Logs) error { native.Add(1); return nil }))
			require.NoError(t, c.ConsumeLogs(context.Background(), logsData()))
			ctrl.mu.Lock()
			if mode == logsagent.PromtailMode {
				ctrl.mode = logsagent.OTELNativeMode
			} else {
				ctrl.mode = logsagent.PromtailMode
			}
			ctrl.mu.Unlock()
			require.NoError(t, c.ConsumeLogs(context.Background(), logsData()))
			if mode == logsagent.PromtailMode {
				require.EqualValues(t, 2, promtail.Load())
				require.Zero(t, native.Load())
			} else {
				require.Zero(t, promtail.Load())
				require.EqualValues(t, 2, native.Load())
			}
			require.Equal(t, logsagent.ExportSnapshot{Acknowledged: 2}, ctrl.observer.Snapshot())
			label := "promtail"
			if mode == logsagent.OTELNativeMode {
				label = "native"
			}
			require.EqualValues(t, 2, metricSum(t, reader, "stslogsagent.export_requests",
				attribute.String("mode", label), attribute.String("outcome", "acknowledged")))
		})
	}
}
