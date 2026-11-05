package clickhousestsexporter

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestLogAnyValueJSONEscaping(t *testing.T) {
	for _, text := range []string{
		"", "plain ASCII / text", `"quotes" and \backslashes`,
		"\x00\x01\b\f\n\r\t\x1f\x7f", "<script>&</script>",
		"café 日本語 🙂", "\u2028\u2029", string([]byte{0xff, 0xc0, 0x80}),
	} {
		t.Run(fmt.Sprintf("%q", text), func(t *testing.T) {
			expected, err := json.Marshal(text)
			require.NoError(t, err)
			encoded, err := encodeLogAnyValue(pcommon.NewValueStr(text))
			require.NoError(t, err)
			require.Equal(t, `{"stringValue":`+string(expected)+`}`, encoded)

			value := pcommon.NewValueMap()
			value.Map().PutStr(text, text)
			encoded, err = encodeLogAnyValue(value)
			require.NoError(t, err)
			require.Equal(t, `{"kvlistValue":{"values":[{"key":`+string(expected)+
				`,"value":{"stringValue":`+string(expected)+`}}]}}`, encoded)
		})
	}
}

func TestLogAnyValueFiniteDoubleJSON(t *testing.T) {
	for _, number := range []float64{
		0, math.Copysign(0, -1), 0.25, 1e-7, 1e-6, 1e20, 1e21,
		math.SmallestNonzeroFloat64, math.MaxFloat64,
	} {
		t.Run(fmt.Sprint(number), func(t *testing.T) {
			expected, err := json.Marshal(number)
			require.NoError(t, err)
			encoded, err := encodeLogAnyValue(pcommon.NewValueDouble(number))
			require.NoError(t, err)
			require.Equal(t, `{"doubleValue":`+string(expected)+`}`, encoded)
		})
	}
}

func TestLogAnyValueEmptyCollectionsAndBytes(t *testing.T) {
	for _, test := range []struct {
		name  string
		value pcommon.Value
		json  string
	}{
		{"unset", pcommon.NewValueEmpty(), `{}`},
		{"array", pcommon.NewValueSlice(), `{"arrayValue":{"values":[]}}`},
		{"map", pcommon.NewValueMap(), `{"kvlistValue":{"values":[]}}`},
		{"bytes", pcommon.NewValueBytes(), `{"bytesValue":""}`},
		{"false", pcommon.NewValueBool(false), `{"boolValue":false}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := encodeLogAnyValue(test.value)
			require.NoError(t, err)
			require.Equal(t, test.json, encoded)
		})
	}
}

func TestLogAnyValueNestedCollections(t *testing.T) {
	value := pcommon.NewValueMap()
	values := value.Map().PutEmptySlice("items")
	values.AppendEmpty().SetInt(math.MinInt64)
	values.AppendEmpty().SetInt(math.MaxInt64)
	values.AppendEmpty().SetDouble(math.Inf(-1))
	values.AppendEmpty().SetEmptyBytes().FromRaw([]byte{0, 255, 128})
	values.AppendEmpty().SetEmptyMap().PutStr("quote\"", "line\n")
	values.AppendEmpty()
	encoded, err := encodeLogAnyValue(value)
	require.NoError(t, err)
	require.Equal(t, `{"kvlistValue":{"values":[{"key":"items","value":{"arrayValue":{"values":[`+
		`{"intValue":"-9223372036854775808"},{"intValue":"9223372036854775807"},`+
		`{"doubleValue":"-Infinity"},{"bytesValue":"AP+A"},`+
		`{"kvlistValue":{"values":[{"key":"quote\"","value":{"stringValue":"line\n"}}]}},`+
		`{}]}}}]}}`, encoded)
}

func FuzzLogAnyValueString(f *testing.F) {
	for _, text := range []string{"", "ASCII", "\"\\\n", "<>&", "日本語", "\u2028", "\xff"} {
		f.Add(text)
	}
	f.Fuzz(func(t *testing.T, text string) {
		expected, err := json.Marshal(text)
		require.NoError(t, err)
		encoded, err := encodeLogAnyValue(pcommon.NewValueStr(text))
		require.NoError(t, err)
		require.Equal(t, `{"stringValue":`+string(expected)+`}`, encoded)
	})
}

func BenchmarkEncodeLogAnyValue(b *testing.B) {
	for _, escaped := range []bool{false, true} {
		b.Run(fmt.Sprintf("escaped=%t", escaped), func(b *testing.B) {
			body := pcommon.NewValueMap()
			body.Map().PutStr("kind", "SyntheticResource")
			items := body.Map().PutEmptySlice("items")
			text := strings.Repeat("x", 300)
			if escaped {
				text = strings.Repeat("日本語 <>&\n", 30)
			}
			for i := 0; i < 64; i++ {
				item := items.AppendEmpty().SetEmptyMap()
				item.PutStr("name", fmt.Sprintf("entry-%d", i))
				item.PutStr("data", text)
				labels := item.PutEmptyMap("labels")
				for label := 0; label < 4; label++ {
					labels.PutStr(fmt.Sprintf("key-%d", label), "synthetic-value")
				}
			}
			encoded, err := encodeLogAnyValue(body)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(encoded)))
			b.ResetTimer()
			for b.Loop() {
				encoded, err = encodeLogAnyValue(body)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if len(encoded) == 0 {
				b.Fatal("empty encoded body")
			}
		})
	}
}

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

func TestLogResourceIdentityIgnoresAttributeOrder(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "checkout")
	resource.Attributes().PutInt("worker.count", math.MaxInt64)
	resource.Attributes().PutEmptyBytes("identity").FromRaw([]byte{1, 0, 0})
	resource.Attributes().PutStr("large", strings.Repeat("value", 400))
	for i := 0; i < 40; i++ {
		resource.Attributes().PutInt(fmt.Sprintf("key-%d", i), int64(i))
	}
	first, err := newLogResourceModel(resource)
	require.NoError(t, err)
	reordered := pcommon.NewResource()
	resource.Attributes().Range(func(key string, value pcommon.Value) bool {
		value.CopyTo(reordered.Attributes().PutEmpty(key))
		return true
	})
	reordered.Attributes().Remove("service.name")
	reordered.Attributes().PutStr("service.name", "checkout")
	second, err := newLogResourceModel(reordered)
	require.NoError(t, err)
	require.Equal(t, first.resourceRef, second.resourceRef)
}
