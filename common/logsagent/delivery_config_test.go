//nolint:goconst // Keep authored keys and fixture values together.
package logsagent_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestDeliveryConfiguration(t *testing.T) {
	cfg := deliveryDefaults(t)
	require.NoError(t, cfg.Validate())
	var decoded logsagent.DeliveryConfig
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"controller_extension": "stslogsagent/logs",
		"max_concurrent_calls": 8,
		"max_record_bytes":     262144,
		"max_request_bytes":    1048576,
		"export_lifetime":      "90s",
	}).Unmarshal(&decoded))
	require.Equal(t, *cfg, decoded)
	require.Error(t, confmap.NewFromStringMap(map[string]any{"unknown_key": 1}).Unmarshal(&decoded))
	for _, tc := range []struct {
		name   string
		change func(*logsagent.DeliveryConfig)
	}{
		{"missing extension", func(c *logsagent.DeliveryConfig) { c.ControllerExtension = component.ID{} }},
		{"zero concurrency", func(c *logsagent.DeliveryConfig) { c.MaxConcurrentCalls = 0 }},
		{"negative concurrency", func(c *logsagent.DeliveryConfig) { c.MaxConcurrentCalls = -1 }},
		{"zero record bytes", func(c *logsagent.DeliveryConfig) { c.MaxRecordBytes = 0 }},
		{"request below record", func(c *logsagent.DeliveryConfig) { c.MaxRequestBytes = c.MaxRecordBytes - 1 }},
		{"zero lifetime", func(c *logsagent.DeliveryConfig) { c.ExportLifetime = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := deliveryDefaults(t)
			tc.change(cfg)
			require.Error(t, cfg.Validate())
		})
	}
}

func TestDeliveryCreationAndStartupFailures(t *testing.T) {
	next := logsConsumer(t, func(context.Context, plog.Logs) error { return nil })
	cfg := deliveryDefaults(t)
	set := deliveryTelemetry(nil)
	invalid := *cfg
	invalid.ExportLifetime = 0
	_, err := logsagent.NewDelivery(invalid, set.MeterProvider, set.Logger)
	require.Error(t, err)
	for _, tc := range []struct {
		name string
		ctrl logsagent.Controller
		next consumer.Logs
	}{
		{"missing controller", nil, next},
		{"missing consumer", &controllerStub{bound: time.Second}, nil},
		{"zero bound", &controllerStub{}, next},
		{"bound exceeds lifetime", &controllerStub{bound: time.Hour}, next},
		{"registration fails", &controllerStub{bound: time.Second, err: errors.New("failed")}, next},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := logsagent.NewDelivery(*cfg, set.MeterProvider, set.Logger)
			require.NoError(t, err)
			require.Error(t, c.Start(tc.ctrl, tc.next))
			require.ErrorContains(t, c.ConsumeLogs(context.Background(), logsData()), "not_running")
			require.NoError(t, c.Shutdown(context.Background()))
			require.NoError(t, c.Shutdown(context.Background()))
			ctrl := &controllerStub{bound: time.Second}
			require.ErrorContains(t, c.Start(ctrl, next), "already started or stopped")
			require.Nil(t, ctrl.observer)
		})
	}
}
