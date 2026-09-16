package stslogsrouteconnector_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	route "github.com/stackvista/sts-opentelemetry-collector/connector/stslogsrouteconnector"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestExportDiagnosticsAreBoundedAndDebugOnly(t *testing.T) {
	for _, level := range []zap.AtomicLevel{zap.NewAtomicLevelAt(zap.DebugLevel), zap.NewAtomicLevelAt(zap.InfoLevel)} {
		t.Run(level.String(), func(t *testing.T) {
			core, observed := observer.New(level)
			set := settings(nil)
			set.Logger = zap.New(core)
			cfg := defaults(t)
			cfg.MaxRecordBytes, cfg.MaxRequestBytes = 128, 512
			var downstreamErr error
			next := logsConsumer(t, func(context.Context, plog.Logs) error { return downstreamErr })
			router := connector.NewLogsRouter(map[pipeline.ID]consumer.Logs{
				cfg.LegacyPipeline: next, cfg.NativePipeline: next,
			})
			c, err := route.NewFactory().CreateLogsToLogs(context.Background(), set, cfg, router)
			require.NoError(t, err)
			ctrl := &controllerStub{mode: logsagent.Native, bound: time.Second}
			require.NoError(t, c.Start(context.Background(), hostStub{cfg.CapabilityExtension: ctrl}))
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
					"outcome": expected.outcome, "mode": "native", "draining": expected.draining, "log_records": int64(1),
				}, entries[i].ContextMap())
			}
		})
	}
}
