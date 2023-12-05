package convert

import "go.opentelemetry.io/collector/pdata/pcommon"

func ConvertCommonMap(attrs pcommon.Map, m map[string]string) map[string]string {
	attrs.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})

	return m
}
