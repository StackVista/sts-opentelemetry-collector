package logsagent_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestExportDiagnosticsAreBoundedAndDebugOnly(t *testing.T) {
	for _, level := range []zap.AtomicLevel{zap.NewAtomicLevelAt(zap.DebugLevel), zap.NewAtomicLevelAt(zap.InfoLevel)} {
		t.Run(level.String(), func(t *testing.T) {
			core, observed := observer.New(level)
			set := deliveryTelemetry(nil)
			set.Logger = zap.New(core)
			cfg := deliveryDefaults(t)
			cfg.MaxRecordBytes, cfg.MaxRequestBytes = 128, 512
			var downstreamErr error
			next := logsConsumer(t, func(context.Context, plog.Logs) error { return downstreamErr })
			c, err := logsagent.NewDelivery(*cfg, set.MeterProvider, set.Logger)
			require.NoError(t, err)
			ctrl := &controllerStub{bound: time.Second}
			require.NoError(t, c.Start(ctrl, next))
			t.Cleanup(func() { require.NoError(t, c.Shutdown(context.Background())) })

			data := logsData()
			data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().SetStr("fixture payload stays private")
			require.NoError(t, c.ConsumeLogs(context.Background(), data))
			downstreamErr = errors.New("fixture downstream detail stays private")
			ctrl.drain(time.Now().Add(time.Minute))
			require.ErrorIs(t, c.ConsumeLogs(context.Background(), data), downstreamErr)
			data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().SetStr(strings.Repeat("x", 600))
			require.ErrorContains(t, c.ConsumeLogs(context.Background(), data), "record_too_large")

			entries := observed.All()
			if level.Level() == zap.InfoLevel {
				require.Empty(t, entries)
				return
			}
			require.Len(t, entries, 3)
			for i, expected := range []struct {
				outcome  string
				draining bool
			}{
				{"acknowledged", false},
				{"terminal_export_error", true},
				{"permanent_rejection", true},
			} {
				require.Equal(t, zap.DebugLevel, entries[i].Level)
				require.Equal(t, "Logs export completed", entries[i].Message)
				require.Equal(t, map[string]any{
					"outcome": expected.outcome, "draining": expected.draining, "log_records": int64(1),
				}, entries[i].ContextMap())
			}
		})
	}
}
