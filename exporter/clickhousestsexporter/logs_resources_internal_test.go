package clickhousestsexporter

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestLogResourceModelsReuseEquivalentAttributes(t *testing.T) {
	for _, service := range []string{"missing", "", "unknown_service", "checkout"} {
		t.Run(service, func(t *testing.T) {
			resource := pcommon.NewResource()
			attrs := resource.Attributes()
			if service != "missing" {
				attrs.PutStr("service.name", service)
			}
			attrs.PutStr("k8s.cluster.name", "cluster")
			attrs.PutStr("k8s.namespace.name", "namespace")
			attrs.PutInt("workers", math.MaxInt64)
			attrs.PutDouble("ratio", 0.25)
			attrs.PutBool("enabled", true)
			attrs.PutEmptyBytes("identity").FromRaw([]byte{0, 255, 128})
			attrs.PutEmptyMap("nested").PutInt("minimum", math.MinInt64)
			original := pcommon.NewResource()
			resource.CopyTo(original)

			models := newLogResourceModels()
			first, err := models.get(resource)
			require.NoError(t, err)
			copyOfResource := pcommon.NewResource()
			resource.CopyTo(copyOfResource)
			copyOfResource.Attributes().Remove("workers")
			copyOfResource.Attributes().PutInt("workers", math.MaxInt64)
			second, err := models.get(copyOfResource)
			require.NoError(t, err)
			require.Same(t, first, second)
			expected, err := newLogResourceModel(original)
			require.NoError(t, err)
			require.Equal(t, expected, second)
			require.True(t, original.Attributes().Equal(resource.Attributes()))
		})
	}
}

func TestLogResourceModelsKeepDistinctResources(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "checkout")
	resource.Attributes().PutStr("k8s.pod.uid", "same-pod")
	resource.Attributes().PutStr("k8s.cluster.name", "cluster")
	resource.Attributes().PutStr("k8s.namespace.name", "first")
	models := newLogResourceModels()
	first, err := models.get(resource)
	require.NoError(t, err)
	for _, key := range []string{"service.name", "k8s.namespace.name"} {
		changed := pcommon.NewResource()
		resource.CopyTo(changed)
		changed.Attributes().PutStr(key, "changed")
		second, err := models.get(changed)
		require.NoError(t, err)
		require.NotSame(t, first, second)
		require.NotEqual(t, first.resourceRef, second.resourceRef)
		expected, err := newLogResourceModel(changed)
		require.NoError(t, err)
		require.Equal(t, expected, second)
	}
}

func TestLogResourceModelsVerifyCacheHits(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "checkout")
	resource.Attributes().PutInt("workers", 1)
	models := newLogResourceModels()
	first, err := models.get(resource)
	require.NoError(t, err)
	changed := pcommon.NewResource()
	resource.CopyTo(changed)
	changed.Attributes().PutInt("workers", 2)
	models.entries[models.fingerprint(changed.Attributes())] = cachedLogResource{
		attributes: resource.Attributes(), model: first,
	}
	second, err := models.get(changed)
	require.NoError(t, err)
	require.NotSame(t, first, second)
	require.Equal(t, int64(2), second.attributes.ints["workers"])
}

func TestLogResourceModelsBypassCacheForDistinctResources(t *testing.T) {
	models := newLogResourceModels()
	var firstResource pcommon.Resource
	var first *logResourceModel
	for i := 0; i < logResourceCacheSampleSize; i++ {
		resource := pcommon.NewResource()
		resource.Attributes().PutStr("service.name", "checkout")
		resource.Attributes().PutInt("worker", int64(i))
		model, err := models.get(resource)
		require.NoError(t, err)
		if i == 0 {
			firstResource, first = resource, model
		}
	}
	repeated, err := models.get(firstResource)
	require.NoError(t, err)
	require.NotSame(t, first, repeated)
	require.Equal(t, first, repeated)
}

func TestLogResourceModelsDoNotTrustHashMatches(t *testing.T) {
	logs, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(`{
		"resourceLogs":[{"resource":{"attributes":[
			{"key":"x","value":{"stringValue":"first"}},
			{"key":"x","value":{"stringValue":"second"}}
		]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1793707200123456789"}]}]}]
	}`))
	require.NoError(t, err)
	valid := logs.ResourceLogs().AppendEmpty()
	valid.Resource().Attributes().PutStr("x", "first\xf4x\xf7first")
	valid.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().SetTimestamp(1793707200123456789)
	require.Equal(t, pdatautil.MapHash(valid.Resource().Attributes()),
		pdatautil.MapHash(logs.ResourceLogs().At(0).Resource().Attributes()))
	for _, validFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(validFirst), func(t *testing.T) {
			payload := plog.NewLogs()
			order := []int{0, 1}
			if validFirst {
				order = []int{1, 0}
			}
			for _, index := range order {
				logs.ResourceLogs().At(index).CopyTo(payload.ResourceLogs().AppendEmpty())
			}
			rows, resources, stats, err := logRowsFromPData(payload)
			require.ErrorContains(t, err, `duplicate attribute key "x"`)
			require.Len(t, rows, 1)
			require.Len(t, resources, 1)
			require.Equal(t, 1, stats.rejected.LogRecordCount())
			require.Equal(t, "first\xf4x\xf7first", rows[0].resource.attributes.strings["x"])
		})
	}
}

func TestLogResourceModelsPreserveNestedValues(t *testing.T) {
	payload := `{
		"resourceLogs":[
			{"resource":{"attributes":[{"key":"k8s.cluster.name","value":{"kvlistValue":{"values":[
				{"key":"x","value":{"stringValue":"first"}},{"key":"x","value":{"stringValue":"first"}}
			]}}}]}},
			{"resource":{"attributes":[{"key":"k8s.cluster.name","value":{"kvlistValue":{"values":[
				{"key":"x","value":{"stringValue":"first"}},{"key":"x","value":{"stringValue":"changed"}}
			]}}}]}}
		]
	}`
	for _, key := range []string{"k8s.cluster.name", "metadata"} {
		t.Run(key, func(t *testing.T) {
			logs, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs(
				[]byte(strings.ReplaceAll(payload, "k8s.cluster.name", key)))
			require.NoError(t, err)
			models := newLogResourceModels()
			first, err := models.get(logs.ResourceLogs().At(0).Resource())
			require.NoError(t, err)
			second, err := models.get(logs.ResourceLogs().At(1).Resource())
			require.NoError(t, err)
			require.NotSame(t, first, second)
			require.NotEqual(t, first.resourceRef, second.resourceRef)
			require.NotEqual(t, first.attributes.other[key], second.attributes.other[key])
			if key == "k8s.cluster.name" {
				require.NotEqual(t, first.authScope, second.authScope)
			}
		})
	}
}

func TestLogResourceModelsAreLimitedToOneBatch(t *testing.T) {
	logs := testLogs(1)
	logs.ResourceLogs().At(0).CopyTo(logs.ResourceLogs().AppendEmpty())
	first, resources, _, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.Len(t, resources, 1)
	require.Same(t, first[0].resource, first[1].resource)
	second, _, _, err := logRowsFromPData(logs)
	require.NoError(t, err)
	require.NotSame(t, first[0].resource, second[0].resource)
	require.Equal(t, first[0].resource, second[0].resource)
}

func BenchmarkLogRowsResourceGroups(b *testing.B) {
	for _, named := range []bool{false, true} {
		for _, test := range []struct {
			name           string
			groups, unique int
		}{
			{"Grouped", 1, 1},
			{"Repeated", 1000, 64},
			{"Unique", 1000, 1000},
		} {
			b.Run(fmt.Sprintf("%s/named=%t", test.name, named), func(b *testing.B) {
				logs := plog.NewLogs()
				body := strings.Repeat("app log ", 30)
				for group := 0; group < test.groups; group++ {
					resource := logs.ResourceLogs().AppendEmpty()
					attrs := resource.Resource().Attributes()
					attrs.PutStr("k8s.cluster.name", "cluster")
					attrs.PutStr("k8s.namespace.name", "namespace")
					if named {
						attrs.PutStr("service.name", "checkout")
					}
					for i := 0; i < 10; i++ {
						attrs.PutStr(fmt.Sprintf("label-%d", i), fmt.Sprintf("resource-%d", group%test.unique))
					}
					records := resource.ScopeLogs().AppendEmpty().LogRecords()
					for i := 0; i < 1000/test.groups; i++ {
						record := records.AppendEmpty()
						record.SetTimestamp(1793707200123456789)
						record.Attributes().PutStr("log.file.name", "stdout")
						record.Body().SetStr(body)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					rows, _, _, err := logRowsFromPData(logs)
					if err != nil || len(rows) != 1000 {
						b.Fatalf("unexpected rows: %d, %v", len(rows), err)
					}
				}
			})
		}
	}
}
