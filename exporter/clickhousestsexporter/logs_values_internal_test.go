package clickhousestsexporter

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestLogAttributesPreserveTypesAndInt64Bounds(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("string.one", "1")
	attrs.PutStr("empty", "")
	attrs.PutInt("int.one", 1)
	attrs.PutInt("minimum", math.MinInt64)
	attrs.PutInt("maximum", math.MaxInt64)
	attrs.PutDouble("ratio", 0.25)
	attrs.PutBool("enabled", true)
	attrs.PutEmpty("present.empty")
	attrs.PutEmptyBytes("bytes").FromRaw([]byte{1, 0, 0})
	list := attrs.PutEmptySlice("mixed")
	list.AppendEmpty().SetInt(math.MaxInt64)
	list.AppendEmpty().SetStr("1")
	list.AppendEmpty().SetEmptyMap().PutBool("enabled", true)

	encoded, err := encodeLogAttributes(attrs)
	require.NoError(t, err)
	require.Equal(t, "1", encoded.strings["string.one"])
	require.Contains(t, encoded.strings, "empty")
	require.Equal(t, "", encoded.strings["empty"])
	require.Equal(t, int64(1), encoded.ints["int.one"])
	require.Equal(t, int64(math.MinInt64), encoded.ints["minimum"])
	require.Equal(t, int64(math.MaxInt64), encoded.ints["maximum"])
	require.Equal(t, 0.25, encoded.doubles["ratio"])
	require.True(t, encoded.bools["enabled"])
	require.Equal(t, "{}", encoded.other["present.empty"])
	require.JSONEq(t, `{"bytesValue":"AQAA"}`, encoded.other["bytes"])
	require.JSONEq(t, `{"arrayValue":{"values":[
		{"intValue":"9223372036854775807"},{"stringValue":"1"},
		{"kvlistValue":{"values":[{"key":"enabled","value":{"boolValue":true}}]}}
	]}}`, encoded.other["mixed"])
	require.NotContains(t, encoded.strings, "int.one")
}

func TestLogAnyValuePreservesNonFiniteDoubles(t *testing.T) {
	for _, test := range []struct {
		number float64
		json   string
	}{
		{math.NaN(), `{"doubleValue":"NaN"}`},
		{math.Inf(1), `{"doubleValue":"Infinity"}`},
		{math.Inf(-1), `{"doubleValue":"-Infinity"}`},
	} {
		encoded, err := encodeLogAnyValue(pcommon.NewValueDouble(test.number))
		require.NoError(t, err)
		require.JSONEq(t, test.json, encoded)
	}
}

func TestLogResourceNormalizesMissingServiceBeforeHashingWithoutMutatingInput(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutInt("worker.count", 4)
	normalized, err := newLogResourceModel(resource)
	require.NoError(t, err)
	require.Equal(t, "unknown_service", normalized.serviceName)
	require.Equal(t, normalized.serviceName, normalized.attributes.strings["service.name"])
	require.Equal(t, int64(4), normalized.attributes.ints["worker.count"])
	_, exists := resource.Attributes().Get("service.name")
	require.False(t, exists)

	resource.Attributes().PutStr("service.name", "unknown_service")
	explicit, err := newLogResourceModel(resource)
	require.NoError(t, err)
	require.Equal(t, explicit.resourceRef, normalized.resourceRef)

	resource.Attributes().PutStr("worker.count", "4")
	stringResource, err := newLogResourceModel(resource)
	require.NoError(t, err)
	require.NotEqual(t, explicit.resourceRef, stringResource.resourceRef)
}

func TestLogBodiesKeepEmptyStringsAndExactIntegers(t *testing.T) {
	logs := testLogs(3)
	records := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	records.At(0).Body().SetStr("")
	records.At(1).Body().SetInt(math.MinInt64)
	rows, _, _, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.Empty(t, rows[0].body)
	require.JSONEq(t, `{"stringValue":""}`, rows[0].bodyOther)
	require.JSONEq(t, `{"intValue":"-9223372036854775808"}`, rows[1].bodyOther)
	require.Empty(t, rows[2].body)
	require.Empty(t, rows[2].bodyOther)
}

func TestLogRowsRejectDuplicateTopLevelAttributeKeys(t *testing.T) {
	logs, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(`{
		"resourceLogs":[{"scopeLogs":[{"logRecords":[{
			"timeUnixNano":"1793707200123456789",
			"attributes":[{"key":"dup","value":{"intValue":"1"}},{"key":"dup","value":{"stringValue":"1"}}]
		}]}]}]
	}`))
	require.NoError(t, err)
	rows, resources, stats, err := logRowsFromPData(logs)
	require.Empty(t, rows)
	require.Empty(t, resources)
	require.Equal(t, 1, stats.rejected.LogRecordCount())
	require.ErrorContains(t, err, `duplicate attribute key "dup"`)
}

func TestLogRowsRejectOnlyAffectedAttributeGroups(t *testing.T) {
	dup := `[{"key":"dup","value":{"stringValue":"a"}},{"key":"dup","value":{"intValue":"1"}}]`
	record := `{"timeUnixNano":"1793707200123456789","body":{"stringValue":"invalid"}}`
	for _, test := range []struct{ name, group string }{
		{"resource", fmt.Sprintf(`{"resource":{"attributes":%s},"scopeLogs":[{"logRecords":[%s]}]}`, dup, record)},
		{"scope", fmt.Sprintf(`{"scopeLogs":[{"scope":{"attributes":%s},"logRecords":[%s]}]}`, dup, record)},
		{"record", fmt.Sprintf(`{"scopeLogs":[{"logRecords":[{"timeUnixNano":"1793707200123456789","attributes":%s}]}]}`, dup)},
	} {
		t.Run(test.name, func(t *testing.T) {
			logs, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(`{"resourceLogs":[` + test.group + `]}`))
			require.NoError(t, err)
			testLogs(1).ResourceLogs().At(0).CopyTo(logs.ResourceLogs().AppendEmpty())
			rows, resources, stats, err := logRowsFromPData(logs)
			require.ErrorContains(t, err, `duplicate attribute key "dup"`)
			require.Len(t, rows, 1)
			require.Len(t, resources, 1)
			require.Equal(t, 1, stats.rejected.LogRecordCount())
		})
	}
}

func TestLogResourcePreservesExistingTypedHash(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "checkout")
	resource.Attributes().PutInt("worker.count", math.MaxInt64)
	resource.Attributes().PutEmptyBytes("identity").FromRaw([]byte{1, 0, 0})
	legacy, err := NewResourceModel(resource)
	require.NoError(t, err)
	current, err := newLogResourceModel(resource)
	require.NoError(t, err)
	require.Equal(t, legacy.resourceRef, current.resourceRef)
}
