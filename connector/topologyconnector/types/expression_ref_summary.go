//nolint:revive
package types

import "sort"

// ExpressionRefSummary summarizes which inputs to include in the projection for a mapping.
// Resource attributes and Scope fields are always included entirely.
type ExpressionRefSummary struct {
	datapointKeys []string
	spanKeys      []string
	metricKeys    []string
}

// NewExpressionRefSummary Constructor ensures all slices are sorted deterministically
func NewExpressionRefSummary(
	datapointKeys, spanKeys, metricKeys []string,
) *ExpressionRefSummary {
	sorted := func(s []string) []string {
		c := append([]string(nil), s...) // copy to avoid mutating caller
		sort.Strings(c)
		return c
	}

	return &ExpressionRefSummary{
		datapointKeys: sorted(datapointKeys),
		spanKeys:      sorted(spanKeys),
		metricKeys:    sorted(metricKeys),
	}
}

func (r *ExpressionRefSummary) DatapointKeys() []string {
	return append([]string(nil), r.datapointKeys...)
}
func (r *ExpressionRefSummary) SpanKeys() []string   { return append([]string(nil), r.spanKeys...) }
func (r *ExpressionRefSummary) MetricKeys() []string { return append([]string(nil), r.metricKeys...) }
