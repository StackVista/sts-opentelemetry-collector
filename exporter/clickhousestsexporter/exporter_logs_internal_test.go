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

	rows, resources, stats, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Len(t, resources, 1)
	require.Zero(t, stats.missingTimestamps)
	require.Zero(t, stats.severityClamped)

	row := rows[0]
	require.Equal(t, ts, row.sourceTimestamp)
	require.Equal(t, ts.Add(time.Second), row.observedTimestamp)
	require.Same(t, resources[0], row.resource)
	require.Equal(t, "test-service", row.resource.serviceName)
	require.Equal(t, "io.opentelemetry.contrib.clickhouse", row.scopeName)
	require.Equal(t, "1.0.0", row.scopeVersion)
	require.Equal(t, map[string]string{"lib": "clickhouse"}, row.scopeAttributes.strings)
	require.Equal(t, string([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}), row.traceID)
	require.Equal(t, string([]byte{1, 2, 3, 4, 5, 6, 7, 8}), row.spanID)
	require.Equal(t, uint8(1), row.traceFlags)
	require.Equal(t, "ERROR", row.severityText)
	require.Equal(t, uint8(plog.SeverityNumberError), row.severityNumber)
	require.Equal(t, "something failed", row.body)
	require.Equal(t, map[string]string{"key": "v"}, row.logAttributes.strings)
	require.Equal(t, []string{"k8s.cluster.name:cluster", "k8s.scope:cluster/ns"}, row.resource.authScope)
}

func TestLogRowsFromPData_StructuredBodyIsJSON(t *testing.T) {
	logs := testLogs(1)
	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().SetEmptyMap().PutStr("msg", "hi")

	rows, _, _, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.Empty(t, rows[0].body)
	require.JSONEq(t, `{"kvlistValue":{"values":[{"key":"msg","value":{"stringValue":"hi"}}]}}`, rows[0].bodyOther)
}

func TestLogRowsFromPData_PreservesObservedOnlyClock(t *testing.T) {
	logs := testLogs(1)
	r := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	observed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	r.SetTimestamp(0)
	r.SetObservedTimestamp(pcommon.NewTimestampFromTime(observed))

	rows, _, stats, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.Equal(t, time.Unix(0, 0).UTC(), rows[0].sourceTimestamp)
	require.Equal(t, observed, rows[0].observedTimestamp)
	require.Zero(t, stats.missingTimestamps, "observed-only logs remain valid")
}

func TestLogRowsFromPData_DropsOnlyRecordsWithoutEitherClock(t *testing.T) {
	logs := testLogs(2)
	r := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	r.SetTimestamp(0)
	r.SetObservedTimestamp(0)
	rows, resources, stats, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Len(t, resources, 1)
	require.Equal(t, 1, stats.missingTimestamps)

	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(1).SetTimestamp(0)
	rows, resources, stats, err = logRowsFromPData(logs)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Empty(t, resources)
	require.Equal(t, 2, stats.missingTimestamps)
}

func TestLogRowsFromPData_PreservesNamedApplicationLogs(t *testing.T) {
	logs := testLogs(2)
	records := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	records.At(0).SetEventName("application.start")
	records.At(1).SetEventName("application.stop")
	rows, resources, stats, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Len(t, resources, 1)
	require.Zero(t, stats.missingTimestamps)
	require.Equal(t, "application.start", rows[0].eventName)
	require.Equal(t, "application.stop", rows[1].eventName)
}

func TestLogRowsFromPData_DeduplicatesResourcesPerBatch(t *testing.T) {
	logs := testLogs(1)
	testLogs(2).ResourceLogs().At(0).CopyTo(logs.ResourceLogs().AppendEmpty())

	rows, resources, _, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.Len(t, resources, 1)
}

func TestSeverityNumberIsClamped(t *testing.T) {
	n, clamped := severityNumber(plog.SeverityNumberFatal4)
	require.Equal(t, uint8(plog.SeverityNumberFatal4), n)
	require.False(t, clamped)

	n, clamped = severityNumber(plog.SeverityNumber(-1))
	require.Equal(t, uint8(0), n)
	require.True(t, clamped)

	n, clamped = severityNumber(plog.SeverityNumberFatal4 + 1)
	require.Equal(t, uint8(0), n, "above Fatal4 but still within uint8 range must still clamp")
	require.True(t, clamped)

	n, clamped = severityNumber(plog.SeverityNumber(1000))
	require.Equal(t, uint8(0), n)
	require.True(t, clamped)
}
