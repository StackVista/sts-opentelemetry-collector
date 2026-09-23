package logsagent_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type controllerStub struct {
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

func (s *controllerStub) drain(deadline time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadline = deadline
}

func deliveryDefaults(t testing.TB) *logsagent.DeliveryConfig {
	t.Helper()
	return &logsagent.DeliveryConfig{
		ControllerExtension: component.MustNewIDWithName("stslogsagent", "logs"),
		MaxConcurrentCalls:  8,
		MaxRecordBytes:      262144,
		MaxRequestBytes:     1048576,
		ExportLifetime:      90 * time.Second,
	}
}

func deliveryTelemetry(provider *sdkmetric.MeterProvider) component.TelemetrySettings {
	telemetry := componenttest.NewNopTelemetrySettings()
	if provider != nil {
		telemetry.MeterProvider = provider
	}
	return telemetry
}

func newDelivery(
	t *testing.T, cfg *logsagent.DeliveryConfig, ctrl *controllerStub, selected consumer.Logs,
) (*logsagent.Delivery, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	set := deliveryTelemetry(provider)
	c, err := logsagent.NewDelivery(*cfg, set.MeterProvider, set.Logger)
	require.NoError(t, err)
	require.NoError(t, c.Start(ctrl, selected))
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

func TestDeliveryKeepsSelectedConsumerAndMode(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			ctrl := &controllerStub{mode: mode, bound: time.Second}
			var calls atomic.Int64
			c, reader := newDelivery(t, deliveryDefaults(t), ctrl,
				logsConsumer(t, func(context.Context, plog.Logs) error { calls.Add(1); return nil }))
			require.False(t, c.Capabilities().MutatesData)
			require.NoError(t, c.ConsumeLogs(context.Background(), logsData()))
			replacement := &controllerStub{mode: mode, bound: time.Second}
			require.ErrorContains(t, c.Start(replacement, logsConsumer(t, func(context.Context, plog.Logs) error {
				t.Error("replacement consumer must not receive logs")
				return nil
			})), "already started or stopped")
			require.Nil(t, replacement.observer)
			ctrl.mu.Lock()
			ctrl.mode = "invalid"
			ctrl.mu.Unlock()
			require.NoError(t, c.ConsumeLogs(context.Background(), logsData()))
			require.EqualValues(t, 2, calls.Load())
			require.Equal(t, logsagent.ExportSnapshot{Acknowledged: 2}, ctrl.observer.Snapshot())
			require.EqualValues(t, 2, metricSum(t, reader, "stslogsagent.export_requests",
				attribute.String("mode", string(mode)), attribute.String("outcome", "acknowledged")))
		})
	}
}

func TestDetachedCancellationAndValues(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			type valueKey struct{}
			for _, canceledBefore := range []bool{true, false} {
				t.Run(map[bool]string{true: "before", false: "during"}[canceledBefore], func(t *testing.T) {
					cfg := deliveryDefaults(t)
					cfg.ExportLifetime = time.Second
					ctrl := &controllerStub{mode: mode, bound: time.Millisecond}
					parent, cancel := context.WithCancel(context.WithValue(context.Background(), valueKey{}, "retained"))
					defer cancel()
					if canceledBefore {
						cancel()
					}
					next := logsConsumer(t, func(ctx context.Context, _ plog.Logs) error {
						cancel()
						require.Equal(t, "retained", ctx.Value(valueKey{}))
						require.NoError(t, ctx.Err())
						deadline, ok := ctx.Deadline()
						require.True(t, ok)
						require.InDelta(t, cfg.ExportLifetime.Seconds(), time.Until(deadline).Seconds(), .1)
						return nil
					})
					c, _ := newDelivery(t, cfg, ctrl, next)
					require.NoError(t, c.ConsumeLogs(parent, logsData()))
				})
			}
		})
	}
}

func TestExpiredCallerDeadlineIsDetached(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			type valueKey struct{}
			parent, cancel := context.WithDeadline(
				context.WithValue(context.Background(), valueKey{}, "retained"), time.Now().Add(-time.Second))
			defer cancel()
			next := logsConsumer(t, func(ctx context.Context, _ plog.Logs) error {
				require.NoError(t, ctx.Err())
				require.Equal(t, "retained", ctx.Value(valueKey{}))
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.True(t, deadline.After(time.Now()))
				return nil
			})
			ctrl := &controllerStub{mode: mode, bound: time.Second}
			c, _ := newDelivery(t, deliveryDefaults(t), ctrl, next)
			require.NoError(t, c.ConsumeLogs(parent, logsData()))
		})
	}
}

func TestAdmissionSaturation(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := deliveryDefaults(t)
			ctrl := &controllerStub{mode: mode, bound: time.Millisecond}
			entered := make(chan struct{}, cfg.MaxConcurrentCalls)
			release := make(chan struct{})
			next := logsConsumer(t, func(context.Context, plog.Logs) error {
				entered <- struct{}{}
				<-release
				return nil
			})
			c, reader := newDelivery(t, cfg, ctrl, next)
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			results := make(chan error, cfg.MaxConcurrentCalls)
			for range cfg.MaxConcurrentCalls {
				go func() { results <- c.ConsumeLogs(context.Background(), logsData()) }()
			}
			for range cfg.MaxConcurrentCalls {
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("admitted call did not reach downstream")
				}
			}
			start := time.Now()
			err := c.ConsumeLogs(context.Background(), logsData())
			require.ErrorContains(t, err, "admission_saturated")
			require.False(t, consumererror.IsPermanent(err))
			require.Less(t, time.Since(start), time.Second)
			require.EqualValues(t, cfg.MaxConcurrentCalls, ctrl.observer.Snapshot().Outstanding)
			require.EqualValues(t, 1, metricSum(t, reader, "stslogsagent.pre_export_rejected_requests",
				attribute.String("reason", "admission_saturated")))
			releaseOnce.Do(func() { close(release) })
			for range cfg.MaxConcurrentCalls {
				require.NoError(t, <-results)
			}
			require.Zero(t, ctrl.observer.Snapshot().Outstanding)
			require.Zero(t, metricSum(t, reader, "stslogsagent.outstanding_requests"))
		})
	}
}

func TestDrainRejectionsInFinalSnapshot(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := deliveryDefaults(t)
			cfg.MaxConcurrentCalls = 1
			cfg.MaxRecordBytes = 128
			cfg.MaxRequestBytes = 512
			ctrl := &controllerStub{mode: mode, bound: time.Millisecond}
			entered, release := make(chan struct{}), make(chan struct{})
			next := logsConsumer(t, func(context.Context, plog.Logs) error {
				close(entered)
				<-release
				return nil
			})
			c, reader := newDelivery(t, cfg, ctrl, next)
			var once sync.Once
			defer once.Do(func() { close(release) })
			result := make(chan error, 1)
			go func() { result <- c.ConsumeLogs(context.Background(), logsData()) }()
			<-entered
			require.ErrorContains(t, c.ConsumeLogs(context.Background(), logsData()), "admission_saturated")
			require.Zero(t, ctrl.observer.Snapshot().DrainRejected)

			ctrl.drain(time.Now().Add(time.Minute))
			for _, rejection := range []struct {
				reason string
				bytes  int
			}{
				{reason: "admission_saturated"},
				{reason: "record_too_large", bytes: 200},
				{reason: "request_too_large", bytes: 600},
			} {
				data := logsData()
				if rejection.bytes != 0 {
					data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().SetStr(
						strings.Repeat("x", rejection.bytes))
				}
				require.ErrorContains(t, c.ConsumeLogs(context.Background(), data), rejection.reason)
			}
			ctrl.drain(time.Now().Add(-time.Second))
			require.ErrorContains(t, c.ConsumeLogs(context.Background(), logsData()), "drain_budget_insufficient")
			once.Do(func() { close(release) })
			require.NoError(t, <-result)
			require.NoError(t, c.Shutdown(context.Background()))
			require.ErrorContains(t, c.ConsumeLogs(context.Background(), logsData()), "not_running")
			require.Equal(t, logsagent.ExportSnapshot{Acknowledged: 1, DrainRejected: 5}, ctrl.observer.Snapshot())
			require.EqualValues(t, 6, metricSum(t, reader, "stslogsagent.pre_export_rejected_requests"))
		})
	}
}

func TestConcurrentDrainRejections(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := deliveryDefaults(t)
			cfg.MaxRecordBytes = 128
			cfg.MaxRequestBytes = 256
			ctrl := &controllerStub{mode: mode, bound: time.Second}
			next := logsConsumer(t, func(context.Context, plog.Logs) error { return nil })
			c, _ := newDelivery(t, cfg, ctrl, next)
			ctrl.drain(time.Now().Add(time.Minute))
			data := logsData()
			data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().SetStr(strings.Repeat("x", 512))
			data.MarkReadOnly()
			const calls = 64
			results := make(chan error, calls)
			for range calls {
				go func() { results <- c.ConsumeLogs(context.Background(), data) }()
			}
			for range calls {
				require.ErrorContains(t, <-results, "request_too_large")
			}
			require.NoError(t, c.Shutdown(context.Background()))
			require.Equal(t, logsagent.ExportSnapshot{DrainRejected: calls}, ctrl.observer.Snapshot())
		})
	}
}

func TestAbsoluteDrainBudget(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := deliveryDefaults(t)
			cfg.ExportLifetime = time.Second
			ctrl := &controllerStub{mode: mode, bound: 100 * time.Millisecond}
			fixedDeadline := time.Now().Add(700 * time.Millisecond)
			ctrl.drain(fixedDeadline)
			var received []time.Time
			next := logsConsumer(t, func(ctx context.Context, _ plog.Logs) error {
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				received = append(received, deadline)
				return nil
			})
			c, reader := newDelivery(t, cfg, ctrl, next)
			for range 3 {
				require.NoError(t, c.ConsumeLogs(context.Background(), logsData()))
			}
			require.Equal(t, []time.Time{fixedDeadline, fixedDeadline, fixedDeadline}, received)
			// Advance the drain to the rejection boundary without waiting on wall time.
			ctrl.drain(time.Now().Add(ctrl.bound / 2))
			require.ErrorContains(t, c.ConsumeLogs(context.Background(), logsData()), "drain_budget_insufficient")
			ctrl.drain(time.Now().Add(-time.Second))
			require.ErrorContains(t, c.ConsumeLogs(context.Background(), logsData()), "drain_budget_insufficient")
			require.Len(t, received, 3)
			require.EqualValues(t, 2, ctrl.observer.Snapshot().DrainRejected)
			require.EqualValues(t, 2, metricSum(t, reader, "stslogsagent.pre_export_rejected_records",
				attribute.String("reason", "drain_budget_insufficient")))
		})
	}
}

func TestDrainingPreservesAdmittedDeadline(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := deliveryDefaults(t)
			ctrl := &controllerStub{mode: mode, bound: time.Second}
			next := logsConsumer(t, func(ctx context.Context, _ plog.Logs) error {
				before, _ := ctx.Deadline()
				ctrl.drain(time.Now().Add(-time.Second))
				after, _ := ctx.Deadline()
				require.Equal(t, before, after)
				require.NoError(t, ctx.Err())
				return nil
			})
			c, reader := newDelivery(t, cfg, ctrl, next)
			require.NoError(t, c.ConsumeLogs(context.Background(), logsData()))
			require.EqualValues(t, 1, metricSum(t, reader, "stslogsagent.export_requests", attribute.Bool("draining", true)))
		})
	}
}

func TestRecordAndRequestSizeBounds(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		for _, part := range []string{"body", "resource", "scope", "resource_schema", "scope_schema"} {
			t.Run(string(mode)+"/"+part, func(t *testing.T) {
				cfg := deliveryDefaults(t)
				cfg.MaxRecordBytes = 128
				cfg.MaxRequestBytes = 4096
				data := logsData()
				rl := data.ResourceLogs().At(0)
				sl := rl.ScopeLogs().At(0)
				value := strings.Repeat("x", 200)
				switch part {
				case "body":
					sl.LogRecords().At(0).Body().SetStr(value)
				case "resource":
					rl.Resource().Attributes().PutStr("metadata", value)
				case "scope":
					sl.Scope().Attributes().PutStr("metadata", value)
				case "resource_schema":
					rl.SetSchemaUrl(value)
				case "scope_schema":
					sl.SetSchemaUrl(value)
				}
				original, err := plogotlp.NewExportRequestFromLogs(data).MarshalProto()
				require.NoError(t, err)
				var calls int
				next := logsConsumer(t, func(context.Context, plog.Logs) error { calls++; return nil })
				ctrl := &controllerStub{mode: mode, bound: time.Second}
				c, reader := newDelivery(t, cfg, ctrl, next)
				err = c.ConsumeLogs(context.Background(), data)
				require.ErrorContains(t, err, "record_too_large")
				require.True(t, consumererror.IsPermanent(err))
				require.Zero(t, calls)
				require.EqualValues(t, 1, metricSum(t, reader, "stslogsagent.oversized_records"))
				after, err := plogotlp.NewExportRequestFromLogs(data).MarshalProto()
				require.NoError(t, err)
				require.Equal(t, original, after)
			})
		}
	}
	t.Run("exact protobuf request boundary", func(t *testing.T) {
		data := logsData()
		wire, err := plogotlp.NewExportRequestFromLogs(data).MarshalProto()
		require.NoError(t, err)
		for _, delta := range []int{0, -1} {
			cfg := deliveryDefaults(t)
			cfg.MaxRecordBytes = len(wire) + delta
			cfg.MaxRequestBytes = len(wire) + delta
			ctrl := &controllerStub{mode: logsagent.PromtailMode, bound: time.Second}
			next := logsConsumer(t, func(context.Context, plog.Logs) error { return nil })
			c, reader := newDelivery(t, cfg, ctrl, next)
			err = c.ConsumeLogs(context.Background(), data)
			if delta == 0 {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "request_too_large")
				require.True(t, consumererror.IsPermanent(err))
				require.EqualValues(t, 1, metricSum(t, reader, "stslogsagent.pre_export_rejected_records",
					attribute.String("reason", "request_too_large")))
			}
		}
	})
}

func TestUnknownAttemptDeadlineIsNotLifetimeExpiry(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			ctrl := &controllerStub{mode: mode, bound: time.Second}
			next := logsConsumer(t, func(context.Context, plog.Logs) error { return context.DeadlineExceeded })
			c, reader := newDelivery(t, deliveryDefaults(t), ctrl, next)
			require.ErrorIs(t, c.ConsumeLogs(context.Background(), logsData()), context.DeadlineExceeded)
			require.Zero(t, ctrl.observer.Snapshot().DeadlineExpired)
			require.EqualValues(t, 1, metricSum(t, reader, "stslogsagent.export_requests",
				attribute.String("outcome", "terminal_export_error")))
		})
	}
}

func TestOversizeRejectsAllAffectedRecordsBeforeSend(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			for _, requestLimit := range []bool{true, false} {
				t.Run(map[bool]string{true: "request", false: "record"}[requestLimit], func(t *testing.T) {
					data := logsData()
					data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().AppendEmpty().Body().SetStr(strings.Repeat("x", 256))
					cfg := deliveryDefaults(t)
					cfg.MaxRecordBytes = 128
					if requestLimit {
						cfg.MaxRequestBytes = 128
					}
					calls := 0
					next := logsConsumer(t, func(context.Context, plog.Logs) error { calls++; return nil })
					ctrl := &controllerStub{mode: mode, bound: time.Second}
					c, reader := newDelivery(t, cfg, ctrl, next)
					require.True(t, consumererror.IsPermanent(c.ConsumeLogs(context.Background(), data)))
					require.Zero(t, calls)
					require.EqualValues(t, 2, metricSum(t, reader, "stslogsagent.pre_export_rejected_records"))
					require.Equal(t, logsagent.ExportSnapshot{}, ctrl.observer.Snapshot())
					if !requestLimit {
						require.EqualValues(t, 1, metricSum(t, reader, "stslogsagent.oversized_records"))
					}
				})
			}
		})
	}
}

func TestRecordSizeDoesNotAccumulateSiblingMetadata(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			data := logsData()
			wire, err := plogotlp.NewExportRequestFromLogs(data).MarshalProto()
			require.NoError(t, err)
			first := data.ResourceLogs().At(0)
			first.CopyTo(data.ResourceLogs().AppendEmpty())
			first.ScopeLogs().At(0).CopyTo(first.ScopeLogs().AppendEmpty())
			cfg := deliveryDefaults(t)
			cfg.MaxRecordBytes = len(wire)
			ctrl := &controllerStub{mode: mode, bound: time.Second}
			next := logsConsumer(t, func(_ context.Context, data plog.Logs) error {
				require.Equal(t, 3, data.LogRecordCount())
				return nil
			})
			c, _ := newDelivery(t, cfg, ctrl, next)
			require.NoError(t, c.ConsumeLogs(context.Background(), data))
		})
	}
}

func TestOtherRecordValidationRemainsWithExporter(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			data := logsData()
			data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().AppendEmpty().Body().SetBool(true)
			ctrl := &controllerStub{mode: mode, bound: time.Second}
			next := logsConsumer(t, func(_ context.Context, received plog.Logs) error {
				require.Equal(t, 2, received.LogRecordCount())
				require.True(t, received.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(1).Body().Bool())
				return nil
			})
			c, reader := newDelivery(t, deliveryDefaults(t), ctrl, next)
			require.NoError(t, c.ConsumeLogs(context.Background(), data))
			require.Zero(t, metricSum(t, reader, "stslogsagent.pre_export_rejected_records"))
		})
	}
}

func TestLifetimeExpiryEvenWhenDownstreamReturnsSuccess(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := deliveryDefaults(t)
			cfg.ExportLifetime = 20 * time.Millisecond
			ctrl := &controllerStub{mode: mode, bound: time.Millisecond}
			next := logsConsumer(t, func(ctx context.Context, _ plog.Logs) error { <-ctx.Done(); return nil })
			c, _ := newDelivery(t, cfg, ctrl, next)
			require.ErrorIs(t, c.ConsumeLogs(context.Background(), logsData()), context.DeadlineExceeded)
			require.EqualValues(t, 1, ctrl.observer.Snapshot().DeadlineExpired)
		})
	}
}

func TestShutdownWaitsForCallsAndRejectsNewCalls(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			ctrl := &controllerStub{mode: mode, bound: time.Second}
			entered, release := make(chan struct{}), make(chan struct{})
			next := logsConsumer(t, func(context.Context, plog.Logs) error { close(entered); <-release; return nil })
			c, _ := newDelivery(t, deliveryDefaults(t), ctrl, next)
			var once sync.Once
			defer once.Do(func() { close(release) })
			result := make(chan error, 1)
			go func() { result <- c.ConsumeLogs(context.Background(), logsData()) }()
			<-entered
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorIs(t, c.Shutdown(ctx), context.Canceled)
			require.ErrorContains(t, c.ConsumeLogs(context.Background(), logsData()), "not_running")
			once.Do(func() { close(release) })
			require.NoError(t, <-result)
			require.NoError(t, c.Shutdown(context.Background()))
			require.NoError(t, c.Shutdown(context.Background()))
		})
	}
}

func TestDownstreamMutationDoesNotChangeInput(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			ctrl := &controllerStub{mode: mode, bound: time.Second}
			next, err := consumer.NewLogs(func(_ context.Context, data plog.Logs) error {
				data.ResourceLogs().RemoveIf(func(plog.ResourceLogs) bool { return true })
				return nil
			}, consumer.WithCapabilities(consumer.Capabilities{MutatesData: true}))
			require.NoError(t, err)
			c, _ := newDelivery(t, deliveryDefaults(t), ctrl, next)
			data := logsData()
			require.NoError(t, c.ConsumeLogs(context.Background(), data))
			require.Equal(t, 1, data.LogRecordCount())
		})
	}
}

func TestUnknownErrorContainingHelperTextIsNotClassified(t *testing.T) {
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
			ctrl := &controllerStub{mode: mode, bound: time.Second}
			next := logsConsumer(t, func(context.Context, plog.Logs) error {
				return errors.New("backend response: no more retries left: unavailable")
			})
			c, reader := newDelivery(t, deliveryDefaults(t), ctrl, next)
			require.Error(t, c.ConsumeLogs(context.Background(), logsData()))
			require.EqualValues(t, 1, metricSum(t, reader, "stslogsagent.export_requests",
				attribute.String("outcome", "terminal_export_error")))
		})
	}
}
