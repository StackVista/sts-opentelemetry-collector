package stslogsrouteconnector_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	route "github.com/stackvista/sts-opentelemetry-collector/connector/stslogsrouteconnector"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pipeline"
)

func TestRecordSizeMatchesProtobufEnvelopes(t *testing.T) {
	for _, bodyBytes := range []int{0, 1, 100, 120, 127, 128, 16370, 16383, 16384} {
		t.Run(strconv.Itoa(bodyBytes), func(t *testing.T) {
			data := plog.NewLogs()
			recordSizes := make([]int, 0, 4)
			for resourceIndex := range 2 {
				rl := data.ResourceLogs().AppendEmpty()
				rl.Resource().Attributes().PutStr("metadata", strings.Repeat("r", resourceIndex*123))
				rl.Resource().SetDroppedAttributesCount(128)
				rl.SetSchemaUrl("resource/schema")
				for scopeIndex := range 2 {
					sl := rl.ScopeLogs().AppendEmpty()
					sl.Scope().SetName(strings.Repeat("s", scopeIndex*120))
					sl.Scope().SetVersion("fixture")
					sl.Scope().SetDroppedAttributesCount(16384)
					sl.Scope().Attributes().PutStr("scope", "metadata")
					sl.SetSchemaUrl("scope/schema")
					lr := sl.LogRecords().AppendEmpty()
					lr.Body().SetStr(strings.Repeat("x", bodyBytes))
					lr.Attributes().PutInt("record", 1)
					lr.SetTimestamp(pcommon.Timestamp(123456))
					lr.SetTraceID(pcommon.TraceID{1})
					lr.SetSpanID(pcommon.SpanID{2})
					lr.SetEventName("fixture")
					single := plog.NewLogs()
					singleResource := single.ResourceLogs().AppendEmpty()
					rl.Resource().CopyTo(singleResource.Resource())
					singleResource.SetSchemaUrl(rl.SchemaUrl())
					singleScope := singleResource.ScopeLogs().AppendEmpty()
					sl.Scope().CopyTo(singleScope.Scope())
					singleScope.SetSchemaUrl(sl.SchemaUrl())
					lr.CopyTo(singleScope.LogRecords().AppendEmpty())
					wire, err := plogotlp.NewExportRequestFromLogs(single).MarshalProto()
					require.NoError(t, err)
					recordSizes = append(recordSizes, len(wire))
				}
			}
			original, err := plogotlp.NewExportRequestFromLogs(data).MarshalProto()
			require.NoError(t, err)
			for _, recordSize := range recordSizes {
				for _, delta := range []int{-1, 0} {
					cfg := defaults(t)
					cfg.MaxRecordBytes = recordSize + delta
					cfg.MaxRequestBytes = len(original)
					expectedOversized := 0
					for _, size := range recordSizes {
						if size > cfg.MaxRecordBytes {
							expectedOversized++
						}
					}
					ctrl := &controllerStub{mode: logsagent.OTELNativeMode, bound: time.Second}
					calls := 0
					next := logsConsumer(t, func(context.Context, plog.Logs) error { calls++; return nil })
					c, reader := newRoute(t, cfg, ctrl, next, next)
					err := c.ConsumeLogs(context.Background(), data)
					if expectedOversized == 0 {
						require.NoError(t, err)
						require.Equal(t, 1, calls)
					} else {
						require.ErrorContains(t, err, "record_too_large")
						require.True(t, consumererror.IsPermanent(err))
						require.Zero(t, calls)
					}
					require.EqualValues(t, expectedOversized, metricSum(t, reader, "stslogsroute.oversized_records"))
				}
			}
			after, err := plogotlp.NewExportRequestFromLogs(data).MarshalProto()
			require.NoError(t, err)
			require.Equal(t, original, after)
		})
	}
}

func sharedMetadataLogs(records, attributes int) plog.Logs {
	data := plog.NewLogs()
	rl := data.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	rl.Resource().Attributes().EnsureCapacity(attributes)
	sl.Scope().Attributes().EnsureCapacity(attributes)
	for i := range attributes {
		key := strconv.Itoa(i)
		rl.Resource().Attributes().PutStr(key, "metadata")
		sl.Scope().Attributes().PutStr(key, "metadata")
	}
	sl.LogRecords().EnsureCapacity(records)
	for range records {
		sl.LogRecords().AppendEmpty().Body().SetStr("x")
	}
	return data
}

func TestManyRecordsWithSharedMetadata(t *testing.T) {
	cfg := defaults(t)
	data := sharedMetadataLogs(80000, 4000)
	wire, err := plogotlp.NewExportRequestFromLogs(data).MarshalProto()
	require.NoError(t, err)
	require.LessOrEqual(t, len(wire), cfg.MaxRequestBytes)
	ctrl := &controllerStub{mode: logsagent.OTELNativeMode, bound: time.Second}
	next := logsConsumer(t, func(_ context.Context, received plog.Logs) error {
		require.Equal(t, 80000, received.LogRecordCount())
		return nil
	})
	c, _ := newRoute(t, cfg, ctrl, next, next)
	require.NoError(t, c.ConsumeLogs(context.Background(), data))
	require.Equal(t, logsagent.ExportSnapshot{Acknowledged: 1}, ctrl.observer.Snapshot())
}

func BenchmarkSharedMetadataSizing(b *testing.B) {
	for _, records := range []int{1000, 80000} {
		for _, attributes := range []int{10, 4000} {
			b.Run(fmt.Sprintf("records=%d/attributes=%d", records, attributes), func(b *testing.B) {
				factory := route.NewFactory()
				cfg, ok := factory.CreateDefaultConfig().(*route.Config)
				require.True(b, ok)
				data := sharedMetadataLogs(records, attributes)
				size := (&plog.ProtoMarshaler{}).LogsSize(data)
				require.LessOrEqual(b, size, cfg.MaxRequestBytes)
				next, err := consumer.NewLogs(func(context.Context, plog.Logs) error { return nil })
				require.NoError(b, err)
				router := connector.NewLogsRouter(map[pipeline.ID]consumer.Logs{
					cfg.PromtailPipeline: next, cfg.OTELNativePipeline: next,
				})
				c, err := factory.CreateLogsToLogs(context.Background(), settings(nil), cfg, router)
				require.NoError(b, err)
				ctrl := &controllerStub{mode: logsagent.OTELNativeMode, bound: time.Second}
				require.NoError(b, c.Start(context.Background(), hostStub{cfg.ControllerExtension: ctrl}))
				b.Cleanup(func() { require.NoError(b, c.Shutdown(context.Background())) })
				b.ReportAllocs()
				b.SetBytes(int64(size))
				for b.Loop() {
					if err := c.ConsumeLogs(context.Background(), data); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
