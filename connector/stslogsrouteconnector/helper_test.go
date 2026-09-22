package stslogsrouteconnector_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
)

func helperExporter(t *testing.T, push consumer.ConsumeLogsFunc, options ...exporterhelper.Option) exporter.Logs {
	t.Helper()
	set := exporter.Settings{
		ID:                component.MustNewID("testexporter"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}
	options = append([]exporterhelper.Option{exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{})}, options...)
	exp, err := exporterhelper.NewLogs(context.Background(), set, struct{}{}, push, options...)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })
	return exp
}

func TestActualHelperTerminalClassifications(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failure    error
		retryLimit time.Duration
		lifetime   time.Duration
		prefix     string
		outcome    string
	}{
		{
			name: "retry exhausted", failure: errors.New("unavailable"),
			retryLimit: time.Millisecond, lifetime: time.Second,
			prefix: "no more retries left: ", outcome: "retry_exhausted",
		},
		{
			name: "attempt timeout exhausts retries", failure: context.DeadlineExceeded,
			retryLimit: time.Millisecond, lifetime: time.Second,
			prefix: "no more retries left: ", outcome: "retry_exhausted",
		},
		{
			name: "next retry beyond lifetime", failure: errors.New("unavailable"),
			retryLimit: time.Minute, lifetime: 50 * time.Millisecond,
			prefix: "request will be cancelled before next retry: ", outcome: "deadline_expired",
		},
		{
			name: "permanent", failure: consumererror.NewPermanent(errors.New("rejected")),
			retryLimit: time.Second, lifetime: time.Second,
			prefix: "not retryable error: ", outcome: "permanent_rejection",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			retry := configretry.NewDefaultBackOffConfig()
			retry.InitialInterval, retry.MaxInterval = 200*time.Millisecond, 200*time.Millisecond
			retry.RandomizationFactor = 0
			retry.MaxElapsedTime = tc.retryLimit
			exp := helperExporter(t, func(context.Context, plog.Logs) error { return tc.failure },
				exporterhelper.WithRetry(retry))
			cfg := defaults(t)
			cfg.ExportLifetime = tc.lifetime
			ctrl := &controllerStub{mode: logsagent.OTELNativeMode, bound: time.Millisecond}
			c, reader := newRoute(t, cfg, ctrl, exp, exp)
			before := time.Now()
			err := c.ConsumeLogs(context.Background(), logsData())
			require.Error(t, err)
			require.True(t, strings.HasPrefix(err.Error(), tc.prefix), "unexpected helper error: %v", err)
			if tc.outcome == "deadline_expired" {
				require.Less(t, time.Since(before), cfg.ExportLifetime)
				require.EqualValues(t, 1, ctrl.observer.Snapshot().DeadlineExpired)
			} else {
				require.Zero(t, ctrl.observer.Snapshot().DeadlineExpired)
			}
			require.EqualValues(t, 1, metricSum(t, reader, "stslogsroute.export_requests",
				attribute.String("outcome", tc.outcome)))
		})
	}
}

func TestHelperLifetimeExpiryDuringAttempt(t *testing.T) {
	retry := configretry.NewDefaultBackOffConfig()
	retry.InitialInterval, retry.MaxInterval = 30*time.Millisecond, 30*time.Millisecond
	retry.RandomizationFactor = 0
	retry.MaxElapsedTime = time.Second
	exp := helperExporter(t, func(ctx context.Context, _ plog.Logs) error {
		<-ctx.Done()
		return errors.New("unavailable")
	}, exporterhelper.WithRetry(retry))
	cfg := defaults(t)
	cfg.ExportLifetime = 20 * time.Millisecond
	ctrl := &controllerStub{mode: logsagent.PromtailMode, bound: time.Millisecond}
	c, reader := newRoute(t, cfg, ctrl, exp, exp)
	require.Error(t, c.ConsumeLogs(context.Background(), logsData()))
	require.EqualValues(t, 1, ctrl.observer.Snapshot().DeadlineExpired)
	require.EqualValues(t, 1, metricSum(t, reader, "stslogsroute.export_requests",
		attribute.String("outcome", "deadline_expired")))
}

func TestHelperRetryRecovery(t *testing.T) {
	retry := configretry.NewDefaultBackOffConfig()
	retry.InitialInterval, retry.MaxInterval = time.Millisecond, time.Millisecond
	retry.MaxElapsedTime = time.Second
	attempts := 0
	exp := helperExporter(t, func(context.Context, plog.Logs) error {
		attempts++
		if attempts < 3 {
			return errors.New("unavailable")
		}
		return nil
	}, exporterhelper.WithRetry(retry))
	ctrl := &controllerStub{mode: logsagent.PromtailMode, bound: time.Second}
	c, reader := newRoute(t, defaults(t), ctrl, exp, exp)
	require.NoError(t, c.ConsumeLogs(context.Background(), logsData()))
	require.Equal(t, 3, attempts)
	require.EqualValues(t, 1, metricSum(t, reader, "stslogsroute.export_requests",
		attribute.String("outcome", "acknowledged")))
}

func TestQueueWaiterCompletionDoesNotMeanWorkerCompletion(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	workerDone := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	queue := exporterhelper.NewDefaultQueueConfig()
	queue.QueueSize, queue.NumConsumers = 1, 1
	queue.WaitForResult, queue.BlockOnOverflow = true, true
	exp := helperExporter(t, func(context.Context, plog.Logs) error {
		close(entered)
		<-release
		close(workerDone)
		return nil
	}, exporterhelper.WithQueue(configoptional.Some(queue)))
	cfg := defaults(t)
	cfg.MaxConcurrentCalls = 1
	cfg.ExportLifetime = 50 * time.Millisecond
	ctrl := &controllerStub{mode: logsagent.OTELNativeMode, bound: time.Millisecond}
	c, reader := newRoute(t, cfg, ctrl, exp, exp)
	result := make(chan error, 1)
	go func() { result <- c.ConsumeLogs(context.Background(), logsData()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	ctrl.drain(time.Now().Add(cfg.ExportLifetime))
	require.ErrorIs(t, <-result, context.DeadlineExceeded)
	require.Equal(t, logsagent.ExportSnapshot{Failed: 1, DeadlineExpired: 1}, ctrl.observer.Snapshot())
	require.NoError(t, c.Shutdown(context.Background()))
	select {
	case <-workerDone:
		t.Fatal("worker should still be active after connector shutdown")
	default:
	}
	require.EqualValues(t, 1, metricSum(t, reader, "stslogsroute.export_requests",
		attribute.String("outcome", "deadline_expired"), attribute.Bool("draining", true)))
	releaseOnce.Do(func() { close(release) })
	<-workerDone
}
