package clickhousestsexporter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func testLogs(records int) plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.SetSchemaUrl("https://opentelemetry.io/schemas/1.4.0")
	rl.Resource().Attributes().PutStr("service.name", "test-service")
	rl.Resource().Attributes().PutStr("k8s.cluster.name", "cluster")
	rl.Resource().Attributes().PutStr("k8s.namespace.name", "ns")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.SetSchemaUrl("https://opentelemetry.io/schemas/1.7.0")
	sl.Scope().SetName("io.opentelemetry.contrib.clickhouse")
	sl.Scope().SetVersion("1.0.0")
	sl.Scope().Attributes().PutStr("lib", "clickhouse")
	for i := 0; i < records; i++ {
		r := sl.LogRecords().AppendEmpty()
		r.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		r.Attributes().PutStr("key", "v")
	}
	return logs
}

func TestLogRowsFromPData_MapsAllFields(t *testing.T) {
	logs := testLogs(1)
	r := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	ts := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	r.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	r.SetObservedTimestamp(pcommon.NewTimestampFromTime(ts.Add(time.Second)))
	r.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	r.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	r.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
	r.SetSeverityText("ERROR")
	r.SetSeverityNumber(plog.SeverityNumberError)
	r.Body().SetStr("something failed")

	rows, resources, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Len(t, resources, 1)

	row := rows[0]
	require.Equal(t, ts, row.timestamp)
	require.Equal(t, ts.Add(time.Second), row.observedTimestamp)
	require.Same(t, resources[0], row.resource)
	require.Equal(t, "https://opentelemetry.io/schemas/1.4.0", row.resourceSchemaURL)
	require.Equal(t, "test-service", row.serviceName)
	require.Equal(t, "io.opentelemetry.contrib.clickhouse", row.scopeName)
	require.Equal(t, "1.0.0", row.scopeVersion)
	require.Equal(t, "https://opentelemetry.io/schemas/1.7.0", row.scopeSchemaURL)
	require.Equal(t, map[string]string{"lib": "clickhouse"}, row.scopeAttributes)
	require.Equal(t, "0102030405060708090a0b0c0d0e0f10", row.traceID)
	require.Equal(t, "0102030405060708", row.spanID)
	require.Equal(t, uint8(1), row.traceFlags)
	require.Equal(t, "ERROR", row.severityText)
	require.Equal(t, uint8(plog.SeverityNumberError), row.severityNumber)
	require.Equal(t, "something failed", row.body)
	require.Equal(t, map[string]string{"key": "v"}, row.logAttributes)
	require.Equal(t, []string{"k8s.cluster.name:cluster", "k8s.scope:cluster/ns"}, row.resource.authScope)
}

func TestLogRowsFromPData_StructuredBodyIsJSON(t *testing.T) {
	logs := testLogs(1)
	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().SetEmptyMap().PutStr("msg", "hi")

	rows, _, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.JSONEq(t, `{"msg":"hi"}`, rows[0].body)
}

func TestLogRowsFromPData_TimestampFallsBackToObserved(t *testing.T) {
	logs := testLogs(1)
	r := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	observed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	r.SetTimestamp(0)
	r.SetObservedTimestamp(pcommon.NewTimestampFromTime(observed))

	rows, _, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.Equal(t, observed, rows[0].timestamp)
}

func TestLogRowsFromPData_SkipsEvents(t *testing.T) {
	logs := testLogs(3)
	records := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	records.At(0).SetEventName("device.app.lifecycle")
	records.At(2).SetEventName("browser.page_view")

	rows, resources, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Len(t, resources, 1)
}

func TestLogRowsFromPData_OnlyEventsProduceNothing(t *testing.T) {
	logs := testLogs(2)
	records := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	records.At(0).SetEventName("a")
	records.At(1).SetEventName("b")

	rows, resources, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Empty(t, resources)
}

func TestLogRowsFromPData_DeduplicatesResourcesPerBatch(t *testing.T) {
	logs := testLogs(1)
	testLogs(2).ResourceLogs().At(0).CopyTo(logs.ResourceLogs().AppendEmpty())

	rows, resources, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.Len(t, resources, 1)
}

func TestSeverityNumberIsClamped(t *testing.T) {
	require.Equal(t, uint8(plog.SeverityNumberFatal4), severityNumber(plog.SeverityNumberFatal4))
	require.Equal(t, uint8(0), severityNumber(plog.SeverityNumber(-1)))
	require.Equal(t, uint8(0), severityNumber(plog.SeverityNumber(1000)))
}

func TestWithOneDayTTLSlack(t *testing.T) {
	cfg := &Config{TTL: 72 * time.Hour}
	require.Equal(t, 96*time.Hour, withOneDayTTLSlack(cfg).TTL)
	require.Equal(t, 72*time.Hour, cfg.TTL, "the original config is not modified")

	require.Equal(t, uint(4), withOneDayTTLSlack(&Config{TTLDays: 3}).TTLDays)
	require.Equal(t, time.Duration(0), withOneDayTTLSlack(&Config{}).TTL, "no TTL stays no TTL")
}
