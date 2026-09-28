//nolint:testpackage
package logsclickhouse

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"text/template"
	"time"

	"e2e/harness"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
)

const testTimeout = 3 * time.Minute

type collectorConfig struct {
	OtlpGRPCEndpoint   string
	ClickHouseAddr     string
	ClickHouseUser     string
	ClickHousePassword string
	Database           string
}

type testEnv struct {
	//nolint:containedctx
	ctx        context.Context
	clickHouse *harness.ClickHouseInstance
	collectors []*harness.CollectorInstance
	database   string
}

func setup(t *testing.T, numCollectors int) *testEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	network := harness.EnsureNetwork(ctx, t)
	clickHouse := harness.StartClickHouse(ctx, t, network.Name)
	database := "e2e_" + strings.ReplaceAll(uuid.NewString(), "-", "")

	env := &testEnv{ctx: ctx, clickHouse: clickHouse, database: database}
	for i := 0; i < numCollectors; i++ {
		configFile := writeCollectorConfig(t, i, collectorConfig{
			OtlpGRPCEndpoint:   "0.0.0.0:4317",
			ClickHouseAddr:     clickHouse.NativeAddr,
			ClickHouseUser:     harness.ClickHouseUser,
			ClickHousePassword: harness.ClickHousePassword,
			Database:           database,
		})
		env.collectors = append(env.collectors, harness.StartCollector(ctx, t, configFile, network.Name, i))
	}
	return env
}

func writeCollectorConfig(t *testing.T, instanceID int, cfg collectorConfig) string {
	t.Helper()
	//nolint:dogsled
	_, filename, _, _ := runtime.Caller(0)
	tmpl, err := template.ParseFiles(filepath.Join(filepath.Dir(filename), "templates", "collector-config.yaml.tmpl"))
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), fmt.Sprintf("collector-logs-clickhouse-%d.yaml", instanceID))
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, tmpl.Execute(f, cfg))
	return path
}

type logRecord struct {
	body      attribute.Value
	eventName string
	severity  otellog.Severity
	timestamp time.Time
	attrs     []attribute.KeyValue
	spanCtx   *trace.SpanContext
}

// sendLogs exports records through the collector's OTLP gRPC receiver, one export request per call.
func sendLogs(t *testing.T, env *testEnv, collector *harness.CollectorInstance, res *resource.Resource, records ...logRecord) {
	t.Helper()
	exporter, err := otlploggrpc.New(env.ctx, otlploggrpc.WithEndpoint(collector.HostAddr), otlploggrpc.WithInsecure())
	require.NoError(t, err)

	provider := sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)))
	logger := provider.Logger("e2e.logsclickhouse", otellog.WithInstrumentationVersion("1.0.0"))
	for _, r := range records {
		var record otellog.Record
		record.SetTimestamp(r.timestamp)
		record.SetObservedTimestamp(time.Now())
		record.SetBody(r.body)
		record.SetEventName(r.eventName)
		record.SetSeverity(r.severity)
		record.SetSeverityText(r.severity.String())
		record.AddAttributes(r.attrs...)
		ctx := env.ctx
		if r.spanCtx != nil {
			ctx = trace.ContextWithSpanContext(ctx, *r.spanCtx)
		}
		logger.Emit(ctx, record)
	}
	require.NoError(t, provider.Shutdown(env.ctx))
}

func testResource(extra ...attribute.KeyValue) *resource.Resource {
	return resource.NewSchemaless(append([]attribute.KeyValue{
		attribute.String("service.name", "checkout"),
		attribute.String("k8s.cluster.name", "prod"),
		attribute.String("k8s.namespace.name", "shop"),
	}, extra...)...)
}

func (env *testEnv) logsQuery(columns string) string {
	return fmt.Sprintf("SELECT %s FROM %s.otel_logs ORDER BY Timestamp", columns, env.database)
}

func (env *testEnv) resourcesQuery() string {
	return fmt.Sprintf(
		"SELECT ResourceRef, ResourceAttributes, AuthScope FROM %s.otel_logs_resources FINAL", env.database)
}

func hasRows(n int) func([]map[string]any) bool {
	return func(rows []map[string]any) bool { return len(rows) == n }
}

func TestLogsToClickHouse_PlatformPipeline(t *testing.T) {
	env := setup(t, 1)

	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x0a, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		SpanID:     trace.SpanID{0x0b, 1, 2, 3, 4, 5, 6, 7},
		TraceFlags: trace.FlagsSampled,
	})
	now := time.Now().UTC().Truncate(time.Microsecond)
	sendLogs(t, env, env.collectors[0], testResource(attribute.String("sts_api_key", "must-not-be-stored")),
		logRecord{
			body: attribute.StringValue("payment retried"), severity: otellog.SeverityWarn, timestamp: now,
			attrs: []attribute.KeyValue{attribute.Int("retry", 2)}, spanCtx: &spanCtx,
		},
		logRecord{
			body:      attribute.MapValue(attribute.String("msg", "structured")),
			severity:  otellog.SeverityInfo,
			timestamp: now.Add(time.Millisecond),
		},
		logRecord{body: attribute.StringValue("an event"), eventName: "checkout.completed", timestamp: now},
	)

	rows := env.clickHouse.QueryUntil(env.ctx, t, env.logsQuery(
		"Body, ServiceName, ScopeName, ScopeVersion, TraceId, SpanId, TraceFlags, SeverityText, SeverityNumber, "+
			"LogAttributes, AuthScope, ResourceRef, toUnixTimestamp64Micro(Timestamp) AS TsMicros",
	), time.Minute, hasRows(2))

	plain, structured := rows[0], rows[1]
	assert.Equal(t, "payment retried", plain["Body"])
	assert.Equal(t, "checkout", plain["ServiceName"])
	assert.Equal(t, "e2e.logsclickhouse", plain["ScopeName"])
	assert.Equal(t, "1.0.0", plain["ScopeVersion"])
	assert.Equal(t, "0a0102030405060708090a0b0c0d0e0f", plain["TraceId"])
	assert.Equal(t, "0b01020304050607", plain["SpanId"])
	assert.EqualValues(t, 1, plain["TraceFlags"])
	assert.Equal(t, "WARN", plain["SeverityText"])
	assert.EqualValues(t, otellog.SeverityWarn, plain["SeverityNumber"])
	assert.Equal(t, map[string]any{"retry": "2"}, plain["LogAttributes"])
	assert.Equal(t, []any{"k8s.cluster.name:prod", "k8s.scope:prod/shop"}, plain["AuthScope"])
	assert.EqualValues(t, now.UnixMicro(), plain["TsMicros"])
	assert.JSONEq(t, `{"msg":"structured"}`, fmt.Sprint(structured["Body"]))

	resources := env.clickHouse.QueryUntil(env.ctx, t, env.resourcesQuery(), time.Minute, hasRows(1))
	assert.Equal(t, plain["ResourceRef"], resources[0]["ResourceRef"])
	assert.Equal(t, map[string]any{
		"service.name": "checkout", "k8s.cluster.name": "prod", "k8s.namespace.name": "shop",
	}, resources[0]["ResourceAttributes"], "sts_api_key is stripped before export")
}

func TestLogsToClickHouse_MultipleCollectorsShareResourceRef(t *testing.T) {
	env := setup(t, 2)

	now := time.Now().UTC()
	for i, collector := range env.collectors {
		sendLogs(t, env, collector, testResource(), logRecord{
			body: attribute.StringValue(fmt.Sprintf("from collector %d", i)), timestamp: now,
		})
	}

	rows := env.clickHouse.QueryUntil(env.ctx, t, env.logsQuery("ResourceRef"), time.Minute, hasRows(2))
	assert.Equal(t, rows[0]["ResourceRef"], rows[1]["ResourceRef"], "resource refs are content hashes")

	env.clickHouse.QueryUntil(env.ctx, t, env.resourcesQuery(), time.Minute, hasRows(1))
}

func TestLogsToClickHouse_BatchesInserts(t *testing.T) {
	env := setup(t, 1)

	const requests, perRequest = 10, 50
	now := time.Now().UTC()
	for i := 0; i < requests; i++ {
		records := make([]logRecord, perRequest)
		for j := range records {
			records[j] = logRecord{body: attribute.StringValue(fmt.Sprintf("request %d record %d", i, j)), timestamp: now}
		}
		sendLogs(t, env, env.collectors[0], testResource(), records...)
	}

	env.clickHouse.QueryUntil(env.ctx, t, fmt.Sprintf("SELECT count() AS n FROM %s.otel_logs", env.database),
		time.Minute, func(rows []map[string]any) bool {
			return len(rows) == 1 && fmt.Sprint(rows[0]["n"]) == fmt.Sprint(requests*perRequest)
		})

	_, err := env.clickHouse.Query(env.ctx, "SYSTEM FLUSH LOGS")
	require.NoError(t, err)
	inserts, err := env.clickHouse.Query(env.ctx, fmt.Sprintf(`SELECT count() AS n FROM system.query_log
		WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND has(tables, '%s.otel_logs')`, env.database))
	require.NoError(t, err)
	require.Len(t, inserts, 1)
	var n int
	_, err = fmt.Sscan(fmt.Sprint(inserts[0]["n"]), &n)
	require.NoError(t, err)
	assert.Positive(t, n, "query_log must see the inserts for this assertion to mean anything")
	assert.Less(t, n, requests, "export requests are merged into fewer inserts")
}
