//go:build integration

package clickhousestsexporter_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap/zaptest"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter"
)

// Keep in step with clickhouse.image.tag in the suse-observability chart.
const defaultClickHouseImage = "quay.io/stackstate/clickhouse:26.8.2.7-so3"

const (
	clickHouseUser     = "admin"
	clickHousePassword = "admin"
)

type clickHouse struct {
	endpoint string
	db       *sql.DB
	database string
}

func startClickHouse(t *testing.T) *clickHouse {
	t.Helper()
	ctx := context.Background()

	image := defaultClickHouseImage
	if override := os.Getenv("CLICKHOUSE_IMAGE"); override != "" {
		image = override
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"9000/tcp", "8123/tcp"},
			Env: map[string]string{
				"CLICKHOUSE_USER":                      clickHouseUser,
				"CLICKHOUSE_PASSWORD":                  clickHousePassword,
				"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
			},
			WaitingFor: wait.ForHTTP("/ping").WithPort("8123/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)

	endpoint := fmt.Sprintf("tcp://%s:%s", host, port.Port())
	database := "it_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	db, err := sql.Open("clickhouse", fmt.Sprintf("%s?username=%s&password=%s", endpoint, clickHouseUser, clickHousePassword))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return &clickHouse{endpoint: endpoint, db: db, database: database}
}

func (ch *clickHouse) exporterConfig(mods ...func(*clickhousestsexporter.Config)) *clickhousestsexporter.Config {
	//nolint:forcetypeassert
	cfg := clickhousestsexporter.CreateDefaultConfig().(*clickhousestsexporter.Config)
	cfg.Endpoint = ch.endpoint
	cfg.Username = clickHouseUser
	cfg.Password = clickHousePassword
	cfg.Database = ch.database
	for _, mod := range mods {
		mod(cfg)
	}
	return cfg
}

func startLogsExporter(t *testing.T, cfg *clickhousestsexporter.Config) *clickhousestsexporter.LogsExporter {
	t.Helper()
	exporter, err := clickhousestsexporter.NewLogsExporter(zaptest.NewLogger(t), cfg)
	require.NoError(t, err)
	require.NoError(t, exporter.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { _ = exporter.Shutdown(context.Background()) })
	return exporter
}

type logRow struct {
	Timestamp         time.Time
	ObservedTimestamp time.Time
	ResourceRef       uuid.UUID
	ResourceSchemaURL string
	ServiceName       string
	ScopeName         string
	ScopeVersion      string
	ScopeSchemaURL    string
	ScopeAttributes   map[string]string
	TraceID           string
	SpanID            string
	TraceFlags        uint8
	SeverityText      string
	SeverityNumber    uint8
	Body              string
	LogAttributes     map[string]string
	AuthScope         []string
}

func (ch *clickHouse) logs(t *testing.T) map[string]logRow {
	t.Helper()
	rows, err := ch.db.Query(fmt.Sprintf(`SELECT Timestamp, ObservedTimestamp, ResourceRef, ResourceSchemaUrl, ServiceName,
		ScopeName, ScopeVersion, ScopeSchemaUrl, ScopeAttributes, TraceId, SpanId, TraceFlags, SeverityText,
		SeverityNumber, Body, LogAttributes, AuthScope FROM %s.otel_logs`, ch.database))
	require.NoError(t, err)
	defer rows.Close()

	byBody := map[string]logRow{}
	for rows.Next() {
		var r logRow
		require.NoError(t, rows.Scan(&r.Timestamp, &r.ObservedTimestamp, &r.ResourceRef, &r.ResourceSchemaURL,
			&r.ServiceName, &r.ScopeName, &r.ScopeVersion, &r.ScopeSchemaURL, &r.ScopeAttributes, &r.TraceID,
			&r.SpanID, &r.TraceFlags, &r.SeverityText, &r.SeverityNumber, &r.Body, &r.LogAttributes, &r.AuthScope))
		byBody[r.Body] = r
	}
	require.NoError(t, rows.Err())
	return byBody
}

type resourceRow struct {
	ResourceRef uuid.UUID
	Attributes  map[string]string
	AuthScope   []string
}

func (ch *clickHouse) resources(t *testing.T) []resourceRow {
	t.Helper()
	rows, err := ch.db.Query(fmt.Sprintf(
		`SELECT ResourceRef, ResourceAttributes, AuthScope FROM %s.otel_logs_resources FINAL`, ch.database))
	require.NoError(t, err)
	defer rows.Close()

	var result []resourceRow
	for rows.Next() {
		var r resourceRow
		require.NoError(t, rows.Scan(&r.ResourceRef, &r.Attributes, &r.AuthScope))
		result = append(result, r)
	}
	require.NoError(t, rows.Err())
	return result
}

func (ch *clickHouse) createTableQuery(t *testing.T, table string) string {
	t.Helper()
	var query string
	require.NoError(t, ch.db.QueryRow(
		"SELECT create_table_query FROM system.tables WHERE database = ? AND name = ?", ch.database, table,
	).Scan(&query))
	return query
}

var (
	testTraceID = pcommon.TraceID([16]byte{0x0a, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
	testSpanID  = pcommon.SpanID([8]byte{0x0b, 1, 2, 3, 4, 5, 6, 7})
	testTime    = time.Date(2026, 9, 1, 12, 0, 0, 123456789, time.UTC)
)

func testLogs() plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.SetSchemaUrl("https://opentelemetry.io/schemas/1.37.0")
	rl.Resource().Attributes().PutStr("service.name", "checkout")
	rl.Resource().Attributes().PutStr("k8s.cluster.name", "prod")
	rl.Resource().Attributes().PutStr("k8s.namespace.name", "shop")

	sl := rl.ScopeLogs().AppendEmpty()
	sl.SetSchemaUrl("https://opentelemetry.io/schemas/1.36.0")
	sl.Scope().SetName("io.opentelemetry.logback")
	sl.Scope().SetVersion("2.1.0")
	sl.Scope().Attributes().PutStr("scope.attr", "x")

	plain := sl.LogRecords().AppendEmpty()
	plain.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	plain.SetObservedTimestamp(pcommon.NewTimestampFromTime(testTime.Add(time.Second)))
	plain.SetTraceID(testTraceID)
	plain.SetSpanID(testSpanID)
	plain.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
	plain.SetSeverityText("WARN")
	plain.SetSeverityNumber(plog.SeverityNumberWarn)
	plain.Body().SetStr("payment retried")
	plain.Attributes().PutStr("code.function", "charge")
	plain.Attributes().PutInt("retry", 2)

	structured := sl.LogRecords().AppendEmpty()
	structured.SetObservedTimestamp(pcommon.NewTimestampFromTime(testTime))
	structured.Body().SetEmptyMap().PutStr("msg", "structured")

	event := sl.LogRecords().AppendEmpty()
	event.SetTimestamp(pcommon.NewTimestampFromTime(testTime))
	event.SetEventName("checkout.completed")
	event.Body().SetStr("an event")

	return logs
}

func TestLogsExporter_WritesAllLogFields(t *testing.T) {
	ch := startClickHouse(t)
	exporter := startLogsExporter(t, ch.exporterConfig())

	require.NoError(t, exporter.PushLogsData(context.Background(), testLogs()))

	rows := ch.logs(t)
	require.Len(t, rows, 2, "the event record is not exported")
	require.NotContains(t, rows, "an event")

	plain := rows["payment retried"]
	require.True(t, testTime.Equal(plain.Timestamp))
	require.True(t, testTime.Add(time.Second).Equal(plain.ObservedTimestamp))
	require.Equal(t, "https://opentelemetry.io/schemas/1.37.0", plain.ResourceSchemaURL)
	require.Equal(t, "checkout", plain.ServiceName)
	require.Equal(t, "io.opentelemetry.logback", plain.ScopeName)
	require.Equal(t, "2.1.0", plain.ScopeVersion)
	require.Equal(t, "https://opentelemetry.io/schemas/1.36.0", plain.ScopeSchemaURL)
	require.Equal(t, map[string]string{"scope.attr": "x"}, plain.ScopeAttributes)
	require.Equal(t, "0a0102030405060708090a0b0c0d0e0f", plain.TraceID)
	require.Equal(t, "0b01020304050607", plain.SpanID)
	require.Equal(t, uint8(1), plain.TraceFlags)
	require.Equal(t, "WARN", plain.SeverityText)
	require.Equal(t, uint8(plog.SeverityNumberWarn), plain.SeverityNumber)
	require.Equal(t, map[string]string{"code.function": "charge", "retry": "2"}, plain.LogAttributes)
	require.Equal(t, []string{"k8s.cluster.name:prod", "k8s.scope:prod/shop"}, plain.AuthScope)

	structured := rows[`{"msg":"structured"}`]
	require.True(t, testTime.Equal(structured.Timestamp), "falls back to the observed timestamp")

	resources := ch.resources(t)
	require.Len(t, resources, 1)
	require.Equal(t, plain.ResourceRef, resources[0].ResourceRef)
	require.Equal(t, structured.ResourceRef, resources[0].ResourceRef)
	require.Equal(t, map[string]string{
		"service.name": "checkout", "k8s.cluster.name": "prod", "k8s.namespace.name": "shop",
	}, resources[0].Attributes)
	require.Equal(t, plain.AuthScope, resources[0].AuthScope)
}

func (ch *clickHouse) count(t *testing.T, query string) uint64 {
	t.Helper()
	var n uint64
	require.NoError(t, ch.db.QueryRow(fmt.Sprintf(query, ch.database)).Scan(&n))
	return n
}

func TestLogsExporter_WritesAnActiveResourceOncePerRefreshInterval(t *testing.T) {
	ch := startClickHouse(t)
	exporter := startLogsExporter(t, ch.exporterConfig())

	for i := 0; i < 3; i++ {
		require.NoError(t, exporter.PushLogsData(context.Background(), testLogs()))
	}

	require.Equal(t, uint64(6), ch.count(t, "SELECT count() FROM %s.otel_logs"))
	require.Equal(t, uint64(1), ch.count(t, "SELECT count() FROM %s.otel_logs_resources"), "no FINAL: one physical write")
}

func TestLogsExporter_ResourceRowsCollapseAcrossWriters(t *testing.T) {
	ch := startClickHouse(t)

	// Separate exporters (e.g. collector replicas) each write the resource; writes more than a second
	// apart get distinct timestamps, which a sort key containing the timestamp would keep apart.
	for i := 0; i < 2; i++ {
		if i > 0 {
			time.Sleep(1100 * time.Millisecond)
		}
		exporter := startLogsExporter(t, ch.exporterConfig())
		require.NoError(t, exporter.PushLogsData(context.Background(), testLogs()))
	}

	require.Equal(t, uint64(2),
		ch.count(t, "SELECT uniqExact(toUnixTimestamp(Timestamp)) FROM %s.otel_logs_resources"),
		"precondition: resource rows were written in different seconds")
	require.Len(t, ch.resources(t), 1)
}

func TestLogsExporter_OnlyEventsWriteNothing(t *testing.T) {
	ch := startClickHouse(t)
	exporter := startLogsExporter(t, ch.exporterConfig())

	logs := testLogs()
	records := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	for i := 0; i < records.Len(); i++ {
		records.At(i).SetEventName("an.event")
	}
	require.NoError(t, exporter.PushLogsData(context.Background(), logs))

	require.Equal(t, uint64(0), ch.count(t, "SELECT count() FROM %s.otel_logs"))
	require.Equal(t, uint64(0), ch.count(t, "SELECT count() FROM %s.otel_logs_resources"))
}

func TestLogsExporter_CreatesTablesWithTTL(t *testing.T) {
	ch := startClickHouse(t)
	startLogsExporter(t, ch.exporterConfig(func(cfg *clickhousestsexporter.Config) { cfg.TTL = 72 * time.Hour }))

	logsDDL := ch.createTableQuery(t, "otel_logs")
	require.Contains(t, logsDDL, "ENGINE = MergeTree")
	require.Contains(t, logsDDL, "TTL toDateTime(Timestamp) + toIntervalDay(3)")

	resourcesDDL := ch.createTableQuery(t, "otel_logs_resources")
	require.Contains(t, resourcesDDL, "ENGINE = ReplacingMergeTree")
	require.Contains(t, resourcesDDL, "TTL toDateTime(Timestamp) + toIntervalDay(4)", "one day of slack beyond the logs TTL")
}

func TestLogsExporter_WritesToExternallyManagedSchema(t *testing.T) {
	ch := startClickHouse(t)
	startLogsExporter(t, ch.exporterConfig())

	exporter := startLogsExporter(t, ch.exporterConfig(func(cfg *clickhousestsexporter.Config) {
		cfg.CreateLogsTable = false
	}))
	require.NoError(t, exporter.PushLogsData(context.Background(), testLogs()))

	require.Len(t, ch.logs(t), 2)
}
