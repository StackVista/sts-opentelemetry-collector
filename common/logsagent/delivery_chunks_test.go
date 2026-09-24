package logsagent_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/otel/attribute"
)

func individualRecords(t *testing.T, data plog.Logs) []string {
	t.Helper()
	var result []string
	for _, rl := range data.ResourceLogs().All() {
		for _, sl := range rl.ScopeLogs().All() {
			for _, lr := range sl.LogRecords().All() {
				single := plog.NewLogs()
				resource := single.ResourceLogs().AppendEmpty()
				rl.Resource().CopyTo(resource.Resource())
				resource.SetSchemaUrl(rl.SchemaUrl())
				scope := resource.ScopeLogs().AppendEmpty()
				sl.Scope().CopyTo(scope.Scope())
				scope.SetSchemaUrl(sl.SchemaUrl())
				lr.CopyTo(scope.LogRecords().AppendEmpty())
				wire, err := plogotlp.NewExportRequestFromLogs(single).MarshalProto()
				require.NoError(t, err)
				result = append(result, string(wire))
			}
		}
	}
	return result
}

func TestChunksPreserveRecordsMetadataAndInput(t *testing.T) {
	for _, mutates := range []bool{false, true} {
		t.Run(fmt.Sprintf("mutates=%t", mutates), func(t *testing.T) {
			data := plog.NewLogs()
			data.ResourceLogs().AppendEmpty()
			for i := range 2 {
				rl := data.ResourceLogs().AppendEmpty()
				rl.Resource().Attributes().PutInt("resource", int64(i))
				rl.Resource().SetDroppedAttributesCount(128)
				rl.SetSchemaUrl("resource/schema")
				for j := range 2 {
					sl := rl.ScopeLogs().AppendEmpty()
					sl.Scope().SetName(fmt.Sprint(j))
					sl.Scope().SetVersion("fixture")
					sl.Scope().Attributes().PutStr("metadata", strings.Repeat("m", 127))
					sl.SetSchemaUrl("scope/schema")
					for k := range 4 {
						lr := sl.LogRecords().AppendEmpty()
						lr.Body().SetStr(strings.Repeat("x", 300))
						lr.Attributes().PutInt("index", int64(k))
					}
				}
			}
			original, err := plogotlp.NewExportRequestFromLogs(data).MarshalProto()
			require.NoError(t, err)
			expected := individualRecords(t, data)
			data.MarkReadOnly()
			cfg := deliveryDefaults(t)
			cfg.MaxRecordBytes, cfg.MaxRequestBytes = 1024, 1200
			var received []string
			calls := 0
			next, err := consumer.NewLogs(func(_ context.Context, chunk plog.Logs) error {
				calls++
				wire, marshalErr := plogotlp.NewExportRequestFromLogs(chunk).MarshalProto()
				require.NoError(t, marshalErr)
				require.LessOrEqual(t, len(wire), cfg.MaxRequestBytes)
				received = append(received, individualRecords(t, chunk)...)
				if mutates {
					chunk.ResourceLogs().RemoveIf(func(plog.ResourceLogs) bool { return true })
				}
				return nil
			}, consumer.WithCapabilities(consumer.Capabilities{MutatesData: mutates}))
			require.NoError(t, err)
			ctrl := &controllerStub{bound: time.Second}
			c, reader := newDelivery(t, cfg, ctrl, next)
			require.NoError(t, c.ConsumeLogs(context.Background(), data))
			require.Greater(t, calls, 1)
			require.Equal(t, expected, received)
			after, err := plogotlp.NewExportRequestFromLogs(data).MarshalProto()
			require.NoError(t, err)
			require.Equal(t, original, after)
			require.Equal(t, logsagent.ExportSnapshot{Acknowledged: 1}, ctrl.observer.Snapshot())
			require.EqualValues(t, 16, metricSum(t, reader, "stslogsagent.export_records",
				attribute.String("outcome", "completed")))
		})
	}
}

func chunkFixture(t *testing.T) (*logsagent.DeliveryConfig, plog.Logs) {
	t.Helper()
	data := logsData()
	records := data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	records.At(0).Body().SetStr(strings.Repeat("a", 80))
	for _, body := range []string{"b", "c", "d"} {
		records.AppendEmpty().Body().SetStr(strings.Repeat(body, 80))
	}
	cfg := deliveryDefaults(t)
	cfg.MaxConcurrentCalls = 1
	cfg.MaxRecordBytes, cfg.MaxRequestBytes = 160, 160
	return cfg, data
}

func TestChunksShareAdmissionDeadlineAndCancellationProtection(t *testing.T) {
	cfg, data := chunkFixture(t)
	cfg.ExportLifetime = 50 * time.Millisecond
	ctrl := &controllerStub{bound: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var deadlines []time.Time
	next := logsConsumer(t, func(exportCtx context.Context, chunk plog.Logs) error {
		require.Equal(t, 1, chunk.LogRecordCount())
		require.EqualValues(t, 1, ctrl.observer.Snapshot().Outstanding)
		deadline, ok := exportCtx.Deadline()
		require.True(t, ok)
		deadlines = append(deadlines, deadline)
		cancel()
		if len(deadlines) == 2 {
			<-exportCtx.Done()
		}
		return nil
	})
	c, reader := newDelivery(t, cfg, ctrl, next)
	require.ErrorIs(t, c.ConsumeLogs(ctx, data), context.DeadlineExceeded)
	require.Len(t, deadlines, 2)
	require.Equal(t, deadlines[0], deadlines[1])
	require.Equal(t, logsagent.ExportSnapshot{Failed: 1, DeadlineExpired: 1}, ctrl.observer.Snapshot())
	for outcome, count := range map[string]int{"completed": 1, "failed": 1, "unsent": 2, "dropped": 0} {
		require.EqualValues(t, count, metricSum(t, reader, "stslogsagent.export_records", attribute.String("outcome", outcome)))
	}
}

func TestChunkRetriesDoNotRepeatAcknowledgedChunks(t *testing.T) {
	cfg, data := chunkFixture(t)
	ctrl := &controllerStub{bound: time.Second}
	attempts := make(map[string]int)
	retry := configretry.NewDefaultBackOffConfig()
	retry.InitialInterval, retry.MaxInterval = time.Millisecond, time.Millisecond
	retry.MaxElapsedTime = time.Second
	rejection := consumererror.NewPermanent(errors.New("rejected"))
	next := helperExporter(t, func(_ context.Context, chunk plog.Logs) error {
		body := chunk.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()[:1]
		attempts[body]++
		switch {
		case body == "b" && attempts[body] < 3:
			return errors.New("retry")
		case body == "c":
			return rejection
		default:
			return nil
		}
	}, exporterhelper.WithRetry(retry))
	c, reader := newDelivery(t, cfg, ctrl, next)
	require.ErrorIs(t, c.ConsumeLogs(context.Background(), data), rejection)
	require.Equal(t, map[string]int{"a": 1, "b": 3, "c": 1}, attempts)
	require.Equal(t, logsagent.ExportSnapshot{Failed: 1}, ctrl.observer.Snapshot())
	for outcome, count := range map[string]int{"completed": 2, "failed": 1, "unsent": 1} {
		require.EqualValues(t, count, metricSum(t, reader, "stslogsagent.export_records", attribute.String("outcome", outcome)))
	}
}
