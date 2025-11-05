package internal

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// ------------------------
// Metric traversal
// ------------------------

// MetricsTraverser provides a visitor-based traversal over OpenTelemetry metric data.
// It caches attribute maps at various levels to avoid repeated conversions and reduce
// allocation overhead during traversal.
//
// Fields:
//
//	metrics           - the raw pmetric.Metrics to traverse
//	resourceAttrCache  - caches resource-level attributes by resourceIndex
//	scopeAttrCache     - caches scope-level attributes by [resourceIndex, scopeIndex]
//	metricMetaCache    - caches metric-level metadata by [resourceIndex, scopeIndex, metricIndex]
//	dpAttrCache        - caches datapoint-level attributes by [resourceIndex, scopeIndex, metricIndex, datapointIndex]
type MetricsTraverser struct {
	metrics pmetric.Metrics

	resourceAttrCache sync.Map // map[int]map[string]any
	scopeAttrCache    sync.Map // map[[2]int]map[string]any
	metricMetaCache   sync.Map // map[[3]int]map[string]any
	dpAttrCache       sync.Map // map[[4]int]map[string]any
}

type SignalTraverser interface {
	Traverse(ctx context.Context, mappingVisitor MappingVisitor)
}

func NewMetricsTraverser(metrics pmetric.Metrics) *MetricsTraverser {
	return &MetricsTraverser{
		metrics: metrics,
	}
}

func (t *MetricsTraverser) resourceAttrs(i int, rm pmetric.ResourceMetricsSlice) map[string]any {
	if v, ok := t.resourceAttrCache.Load(i); ok {
		if cached, typeOk := v.(map[string]any); typeOk {
			return cached
		}
	}
	m := rm.At(i).Resource().Attributes().AsRaw()
	t.resourceAttrCache.Store(i, m)
	return m
}

func (t *MetricsTraverser) scopeAttrs(i, j int, sm pmetric.ScopeMetricsSlice) map[string]any {
	key := [2]int{i, j}
	if v, ok := t.scopeAttrCache.Load(key); ok {
		if cached, typeOk := v.(map[string]any); typeOk {
			return cached
		}
	}
	m := sm.At(j).Scope().Attributes().AsRaw()
	t.scopeAttrCache.Store(key, m)
	return m
}

func (t *MetricsTraverser) metricAttrs(i, j, k int, m pmetric.Metric) map[string]any {
	key := [3]int{i, j, k}
	if v, ok := t.metricMetaCache.Load(key); ok {
		if cached, typeOk := v.(map[string]any); typeOk {
			return cached
		}
	}
	meta := map[string]any{
		"name":        m.Name(),
		"unit":        m.Unit(),
		"description": m.Description(),
	}
	t.metricMetaCache.Store(key, meta)
	return meta
}

type attrProvider interface {
	Attributes() pcommon.Map
}

func (t *MetricsTraverser) datapointAttrs(i, j, k, l int, dp attrProvider) map[string]any {
	key := [4]int{i, j, k, l}
	if v, ok := t.dpAttrCache.Load(key); ok {
		if cached, typeOk := v.(map[string]any); typeOk {
			return cached
		}
	}
	m := dp.Attributes().AsRaw()
	t.dpAttrCache.Store(key, m)
	return m
}

func (t *MetricsTraverser) Traverse(ctx context.Context, mappingVisitor MappingVisitor) {
	rmSlice := t.metrics.ResourceMetrics()
	for i := 0; i < rmSlice.Len(); i++ {
		resourceAttrs := t.resourceAttrs(i, rmSlice)
		rm := rmSlice.At(i)

		resourceCtx := NewMetricEvalContext(nil, nil, nil, resourceAttrs)
		if mappingVisitor.VisitResource(ctx, resourceCtx) == VisitSkip {
			continue
		}

		smSlice := rm.ScopeMetrics()
		for j := 0; j < smSlice.Len(); j++ {
			scopeAttrs := t.scopeAttrs(i, j, smSlice)
			sm := smSlice.At(j)

			scopeCtx := NewMetricEvalContext(nil, nil, scopeAttrs, resourceAttrs)
			if mappingVisitor.VisitScope(ctx, scopeCtx) == VisitSkip {
				continue
			}

			msSlice := sm.Metrics()
			for k := 0; k < msSlice.Len(); k++ {
				m := msSlice.At(k)
				metricAttrs := t.metricAttrs(i, j, k, m)

				if mappingVisitor.VisitMetric(
					ctx, NewMetricEvalContext(nil, metricAttrs, scopeAttrs, resourceAttrs)) == VisitSkip {
					continue
				}

				switch m.Type() {
				case pmetric.MetricTypeEmpty:
					continue
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					for l := dps.Len() - 1; l >= 0; l-- {
						dp := dps.At(l)
						dpAttrs := t.datapointAttrs(i, j, k, l, dp)
						dpCtx := NewMetricEvalContext(dpAttrs, metricAttrs, scopeAttrs, resourceAttrs)
						mappingVisitor.VisitDatapoint(ctx, dpCtx)
					}
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					for l := dps.Len() - 1; l >= 0; l-- {
						dp := dps.At(l)
						dpAttrs := t.datapointAttrs(i, j, k, l, dp)
						dpCtx := NewMetricEvalContext(dpAttrs, metricAttrs, scopeAttrs, resourceAttrs)
						mappingVisitor.VisitDatapoint(ctx, dpCtx)
					}
				case pmetric.MetricTypeHistogram:
					dps := m.Histogram().DataPoints()
					for l := dps.Len() - 1; l >= 0; l-- {
						dp := dps.At(l)
						dpAttrs := t.datapointAttrs(i, j, k, l, dp)
						dpCtx := NewMetricEvalContext(dpAttrs, metricAttrs, scopeAttrs, resourceAttrs)
						mappingVisitor.VisitDatapoint(ctx, dpCtx)
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := m.ExponentialHistogram().DataPoints()
					for l := dps.Len() - 1; l >= 0; l-- {
						dp := dps.At(l)
						dpAttrs := t.datapointAttrs(i, j, k, l, dp)
						dpCtx := NewMetricEvalContext(dpAttrs, metricAttrs, scopeAttrs, resourceAttrs)
						mappingVisitor.VisitDatapoint(ctx, dpCtx)
					}
				case pmetric.MetricTypeSummary:
					dps := m.Summary().DataPoints()
					for l := dps.Len() - 1; l >= 0; l-- {
						dp := dps.At(l)
						dpAttrs := t.datapointAttrs(i, j, k, l, dp)
						dpCtx := NewMetricEvalContext(dpAttrs, metricAttrs, scopeAttrs, resourceAttrs)
						mappingVisitor.VisitDatapoint(ctx, dpCtx)
					}
				}
			}
		}
	}
}

// ------------------------
// Trace traversal
// ------------------------

// TracesTraverser provides a visitor-based traversal over OpenTelemetry trace data.
// It caches attribute maps at various levels to avoid repeated conversions and reduce
// allocation overhead during traversal.
//
// Fields:
//
//	traces - the raw ptrace.Traces to traverse
//	resourceAttrCache - caches resource-level attributes by resourceIndex
//	scopeAttrCache    - caches scope-level attributes by [resourceIndex, scopeIndex]
//	spanAttrCache     - caches span-level metadata by [resourceIndex, scopeIndex, spanIndex]
type TracesTraverser struct {
	traces ptrace.Traces

	resourceAttrCache sync.Map // map[int]map[string]any
	scopeAttrCache    sync.Map // map[[2]int]map[string]any
	spanAttrCache     sync.Map // map[[3]int]map[string]any
}

func NewTracesTraverser(traces ptrace.Traces) *TracesTraverser {
	return &TracesTraverser{
		traces: traces,
	}
}

func (t *TracesTraverser) resourceAttrs(i int, resources ptrace.ResourceSpansSlice) map[string]any {
	if v, ok := t.resourceAttrCache.Load(i); ok {
		if cached, typeOk := v.(map[string]any); typeOk {
			return cached
		}
	}
	m := resources.At(i).Resource().Attributes().AsRaw()
	t.resourceAttrCache.Store(i, m)
	return m
}

func (t *TracesTraverser) scopeAttrs(i, j int, scopes ptrace.ScopeSpansSlice) map[string]any {
	key := [2]int{i, j}
	if v, ok := t.scopeAttrCache.Load(key); ok {
		if cached, typeOk := v.(map[string]any); typeOk {
			return cached
		}
	}
	m := scopes.At(j).Scope().Attributes().AsRaw()
	t.scopeAttrCache.Store(key, m)
	return m
}

func (t *TracesTraverser) spanAttrs(i, j, k int, spans ptrace.SpanSlice) map[string]any {
	key := [3]int{i, j, k}
	if v, ok := t.spanAttrCache.Load(key); ok {
		if cached, typeOk := v.(map[string]any); typeOk {
			return cached
		}
	}
	m := spans.At(k).Attributes().AsRaw()
	t.spanAttrCache.Store(key, m)
	return m
}

func (t *TracesTraverser) Traverse(ctx context.Context, mappingVisitor MappingVisitor) {
	resourceSpans := t.traces.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		resourceAttrs := t.resourceAttrs(i, resourceSpans)
		resourceCtx := NewSpanEvalContext(nil, nil, resourceAttrs)

		if mappingVisitor.VisitResource(ctx, resourceCtx) == VisitSkip {
			continue
		}

		scopeSpans := rs.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			ss := scopeSpans.At(j)
			scopeAttrs := t.scopeAttrs(i, j, scopeSpans)
			scopeCtx := NewSpanEvalContext(nil, scopeAttrs, resourceAttrs)
			if mappingVisitor.VisitScope(ctx, scopeCtx) == VisitSkip {
				continue
			}

			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				spanAttrs := t.spanAttrs(i, j, k, spans)
				spanCtx := NewSpanEvalContext(spanAttrs, scopeAttrs, resourceAttrs)
				mappingVisitor.VisitSpan(ctx, spanCtx)
			}
		}
	}
}
