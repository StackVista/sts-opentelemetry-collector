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
	for _, mode := range []logsagent.Mode{logsagent.PromtailMode, logsagent.OTELNativeMode} {
		t.Run(string(mode), func(t *testing.T) {
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
					ctrl := &controllerStub{mode: mode, bound: time.Second}
					require.NoError(t, c.Start(ctrl, next))
					t.Cleanup(func() { require.NoError(t, c.Shutdown(context.Background())) })

					data := logsData()
					data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().SetStr("fixture payload stays private")
					require.NoError(t, c.ConsumeLogs(context.Background(), data))
					downstreamErr = errors.New("fixture downstream detail stays private")
					ctrl.drain(time.Now().Add(time.Minute))
					require.ErrorIs(t, c.ConsumeLogs(context.Background(), data), downstreamErr)
					data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().SetStr(strings.Repeat("x", 600))
					require.ErrorContains(t, c.ConsumeLogs(context.Background(), data), "request_too_large")

					entries := observed.All()
					if level.Level() == zap.InfoLevel {
						require.Empty(t, entries)
						return
					}
					require.Len(t, entries, 3)
					for i, expected := range []struct {
						message  string
						outcome  string
						draining bool
					}{
						{"Logs export completed", "acknowledged", false},
						{"Logs export completed", "terminal_export_error", true},
						{"Logs export rejected", "request_too_large", true},
					} {
						require.Equal(t, zap.DebugLevel, entries[i].Level)
						require.Equal(t, expected.message, entries[i].Message)
						require.Equal(t, map[string]any{
							"outcome": expected.outcome, "mode": string(mode), "draining": expected.draining, "log_records": int64(1),
						}, entries[i].ContextMap())
					}
				})
			}
		})
	}
}
