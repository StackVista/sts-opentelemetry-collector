package stslogsrouteconnector_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	route "github.com/stackvista/sts-opentelemetry-collector/connector/stslogsrouteconnector"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pipeline"
)

func TestConfiguration(t *testing.T) {
	cfg := defaults(t)
	require.NoError(t, cfg.Validate())
	require.Equal(t, 8, cfg.MaxConcurrentCalls)
	require.Equal(t, 262144, cfg.MaxRecordBytes)
	require.Equal(t, 1048576, cfg.MaxRequestBytes)
	require.Equal(t, 90*time.Second, cfg.ExportLifetime)
	var decoded route.Config
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"capability_extension": "stslogscapability/logs",
		"legacy_pipeline":      "logs/legacy",
		"native_pipeline":      "logs/native",
		"max_concurrent_calls": 8,
		"max_record_bytes":     262144,
		"max_request_bytes":    1048576,
		"export_lifetime":      "90s",
	}).Unmarshal(&decoded))
	require.Equal(t, *cfg, decoded)
	require.Error(t, confmap.NewFromStringMap(map[string]any{"unknown_key": 1}).Unmarshal(&decoded))
	for _, tc := range []struct {
		name   string
		change func(*route.Config)
	}{
		{"missing extension", func(c *route.Config) { c.CapabilityExtension = component.ID{} }},
		{"missing legacy", func(c *route.Config) { c.LegacyPipeline = pipeline.ID{} }},
		{"wrong signal", func(c *route.Config) { c.NativePipeline = pipeline.NewID(pipeline.SignalTraces) }},
		{"same pipeline", func(c *route.Config) { c.NativePipeline = c.LegacyPipeline }},
		{"zero concurrency", func(c *route.Config) { c.MaxConcurrentCalls = 0 }},
		{"negative concurrency", func(c *route.Config) { c.MaxConcurrentCalls = -1 }},
		{"zero record bytes", func(c *route.Config) { c.MaxRecordBytes = 0 }},
		{"request below record", func(c *route.Config) { c.MaxRequestBytes = c.MaxRecordBytes - 1 }},
		{"zero lifetime", func(c *route.Config) { c.ExportLifetime = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaults(t)
			tc.change(cfg)
			require.Error(t, cfg.Validate())
		})
	}
}

func TestFactoryAndStartupFailures(t *testing.T) {
	next := logsConsumer(t, func(context.Context, plog.Logs) error { return nil })
	cfg := defaults(t)
	factory := route.NewFactory()
	require.Equal(t, "stslogsroute", factory.Type().String())
	require.Equal(t, component.StabilityLevelDevelopment, factory.LogsToLogsStability())
	_, err := factory.CreateLogsToLogs(context.Background(), settings(nil), cfg, next)
	require.ErrorContains(t, err, "router")
	router := connector.NewLogsRouter(map[pipeline.ID]consumer.Logs{cfg.LegacyPipeline: next})
	_, err = factory.CreateLogsToLogs(context.Background(), settings(nil), struct{}{}, router)
	require.Error(t, err)
	invalid := *cfg
	invalid.ExportLifetime = 0
	_, err = factory.CreateLogsToLogs(context.Background(), settings(nil), &invalid, router)
	require.Error(t, err)
	for _, tc := range []struct {
		name string
		ctrl component.Component
	}{
		{"missing extension", nil},
		{"wrong extension", &struct {
			component.StartFunc
			component.ShutdownFunc
		}{}},
		{"unknown mode", &controllerStub{mode: "unknown", bound: time.Second}},
		{"missing consumer", &controllerStub{mode: logsagent.Native, bound: time.Second}},
		{"zero bound", &controllerStub{mode: logsagent.Legacy}},
		{"bound exceeds lifetime", &controllerStub{mode: logsagent.Legacy, bound: time.Hour}},
		{"registration fails", &controllerStub{mode: logsagent.Legacy, bound: time.Second, err: errors.New("failed")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := factory.CreateLogsToLogs(context.Background(), settings(nil), cfg, router)
			require.NoError(t, err)
			require.Error(t, c.Start(context.Background(), hostStub{cfg.CapabilityExtension: tc.ctrl}))
			require.ErrorContains(t, c.ConsumeLogs(context.Background(), logsData()), "not_running")
			require.NoError(t, c.Shutdown(context.Background()))
			require.NoError(t, c.Shutdown(context.Background()))
		})
	}
}
