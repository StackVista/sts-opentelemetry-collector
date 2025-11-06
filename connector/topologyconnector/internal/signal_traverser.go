package internal

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// ------------------------------------------------
// Typed wrapper for attribute "cache"
// ------------------------------------------------

type attrCache[K comparable] struct {
	m sync.Map // map[K]map[string]any
}

func (a *attrCache[K]) Load(key K) (map[string]any, bool) {
	v, ok := a.m.Load(key)
	if !ok {
		return nil, false
	}
	cached, ok := v.(map[string]any)
	return cached, ok
}

func (a *attrCache[K]) Store(key K, m map[string]any) {
	a.m.Store(key, m)
}

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
//	metricAttrCache    - caches metric-level metadata by [resourceIndex, scopeIndex, metricIndex]
//	dpAttrCache        - caches datapoint-level attributes by [resourceIndex, scopeIndex, metricIndex, datapointIndex]
type MetricsTraverser struct {
	metrics pmetric.Metrics

	resourceAttrCache attrCache[int]    // map[int]map[string]any
	scopeAttrCache    attrCache[[2]int] // map[[2]int]map[string]any
	metricAttrCache   attrCache[[3]int] // map[[3]int]map[string]any
	dpAttrCache       attrCache[[4]int] // map[[4]int]map[string]any
}

type SignalTraverser interface {
	Traverse(ctx context.Context, mappingVisitor MappingVisitor)
}

func NewMetricsTraverser(metrics pmetric.Metrics) *MetricsTraverser {
	return &MetricsTraverser{
		metrics: metrics,
	}
}

func (t *MetricsTraverser) resourceAttrs(resourceIdx int, rm pmetric.ResourceMetricsSlice) map[string]any {
	if cached, ok := t.resourceAttrCache.Load(resourceIdx); ok {
		return cached
	}
	m := rm.At(resourceIdx).Resource().Attributes().AsRaw()
	t.resourceAttrCache.Store(resourceIdx, m)
	return m
}

func (t *MetricsTraverser) scopeAttrs(resourceIdx, scopeIdx int, sm pmetric.ScopeMetricsSlice) map[string]any {
	key := [2]int{resourceIdx, scopeIdx}
	if cached, ok := t.scopeAttrCache.Load(key); ok {
		return cached
	}
	scope := sm.At(scopeIdx).Scope()
	attrs := scope.Attributes().AsRaw()

	// TEMPORARY: Enrich scope attributes with name and version
	if attrs == nil {
		attrs = make(map[string]any)
	}
	attrs["name"] = scope.Name()
	attrs["version"] = scope.Version()

	t.scopeAttrCache.Store(key, attrs)
	return attrs
}

func (t *MetricsTraverser) metricAttrs(resourceIdx, scopeIdx, metricIdx int, m pmetric.Metric) map[string]any {
	key := [3]int{resourceIdx, scopeIdx, metricIdx}
	if cached, ok := t.metricAttrCache.Load(key); ok {
		return cached
	}
	attrs := map[string]any{
		"name":        m.Name(),
		"unit":        m.Unit(),
		"description": m.Description(),
	}
	t.metricAttrCache.Store(key, attrs)
	return attrs
}

type attrProvider interface {
	Attributes() pcommon.Map
}

func (t *MetricsTraverser) datapointAttrs(
	resourceIdx, scopeIdx, metricIdx, datapointIdx int, dp attrProvider,
) map[string]any {
	key := [4]int{resourceIdx, scopeIdx, metricIdx, datapointIdx}
	if cached, ok := t.dpAttrCache.Load(key); ok {
		return cached
	}
	m := dp.Attributes().AsRaw()
	t.dpAttrCache.Store(key, m)
	return m
}

func (t *MetricsTraverser) Traverse(ctx context.Context, mappingVisitor MappingVisitor) {
	rmSlice := t.metrics.ResourceMetrics()
	for resourceIdx := 0; resourceIdx < rmSlice.Len(); resourceIdx++ {
		resourceAttrs := t.resourceAttrs(resourceIdx, rmSlice)
		rm := rmSlice.At(resourceIdx)

		resourceCtx := NewMetricEvalContext(nil, nil, nil, resourceAttrs)
		if mappingVisitor.VisitResource(ctx, resourceCtx) == VisitSkip {
			continue
		}

		smSlice := rm.ScopeMetrics()
		for scopeIdx := 0; scopeIdx < smSlice.Len(); scopeIdx++ {
			scopeAttrs := t.scopeAttrs(resourceIdx, scopeIdx, smSlice)
			sm := smSlice.At(scopeIdx)

			scopeCtx := NewMetricEvalContext(nil, nil, scopeAttrs, resourceAttrs)
			if mappingVisitor.VisitScope(ctx, scopeCtx) == VisitSkip {
				continue
			}

			msSlice := sm.Metrics()
			for metricIdx := 0; metricIdx < msSlice.Len(); metricIdx++ {
				m := msSlice.At(metricIdx)
				metricAttrs := t.metricAttrs(resourceIdx, scopeIdx, metricIdx, m)

				if mappingVisitor.VisitMetric(
					ctx, NewMetricEvalContext(nil, metricAttrs, scopeAttrs, resourceAttrs)) == VisitSkip {
					continue
				}

				switch m.Type() {
				case pmetric.MetricTypeEmpty:
					continue
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					for datapointIdx := dps.Len() - 1; datapointIdx >= 0; datapointIdx-- {
						dp := dps.At(datapointIdx)
						dpAttrs := t.datapointAttrs(resourceIdx, scopeIdx, metricIdx, datapointIdx, dp)
						dpCtx := NewMetricEvalContext(dpAttrs, metricAttrs, scopeAttrs, resourceAttrs)
						mappingVisitor.VisitDatapoint(ctx, dpCtx)
					}
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					for datapointIdx := dps.Len() - 1; datapointIdx >= 0; datapointIdx-- {
						dp := dps.At(datapointIdx)
						dpAttrs := t.datapointAttrs(resourceIdx, scopeIdx, metricIdx, datapointIdx, dp)
						dpCtx := NewMetricEvalContext(dpAttrs, metricAttrs, scopeAttrs, resourceAttrs)
						mappingVisitor.VisitDatapoint(ctx, dpCtx)
					}
				case pmetric.MetricTypeHistogram:
					dps := m.Histogram().DataPoints()
					for datapointIdx := dps.Len() - 1; datapointIdx >= 0; datapointIdx-- {
						dp := dps.At(datapointIdx)
						dpAttrs := t.datapointAttrs(resourceIdx, scopeIdx, metricIdx, datapointIdx, dp)
						dpCtx := NewMetricEvalContext(dpAttrs, metricAttrs, scopeAttrs, resourceAttrs)
						mappingVisitor.VisitDatapoint(ctx, dpCtx)
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := m.ExponentialHistogram().DataPoints()
					for datapointIdx := dps.Len() - 1; datapointIdx >= 0; datapointIdx-- {
						dp := dps.At(datapointIdx)
						dpAttrs := t.datapointAttrs(resourceIdx, scopeIdx, metricIdx, datapointIdx, dp)
						dpCtx := NewMetricEvalContext(dpAttrs, metricAttrs, scopeAttrs, resourceAttrs)
						mappingVisitor.VisitDatapoint(ctx, dpCtx)
					}
				case pmetric.MetricTypeSummary:
					dps := m.Summary().DataPoints()
					for datapointIdx := dps.Len() - 1; datapointIdx >= 0; datapointIdx-- {
						dp := dps.At(datapointIdx)
						dpAttrs := t.datapointAttrs(resourceIdx, scopeIdx, metricIdx, datapointIdx, dp)
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

	resourceAttrCache attrCache[int]    // map[int]map[string]any
	scopeAttrCache    attrCache[[2]int] // map[[2]int]map[string]any
	spanAttrCache     attrCache[[3]int] // map[[3]int]map[string]any
}

func NewTracesTraverser(traces ptrace.Traces) *TracesTraverser {
	return &TracesTraverser{
		traces: traces,
	}
}

func (t *TracesTraverser) resourceAttrs(resourceIdx int, resources ptrace.ResourceSpansSlice) map[string]any {
	if cached, ok := t.resourceAttrCache.Load(resourceIdx); ok {
		return cached
	}
	m := resources.At(resourceIdx).Resource().Attributes().AsRaw()
	t.resourceAttrCache.Store(resourceIdx, m)
	return m
}

func (t *TracesTraverser) scopeAttrs(resourceIdx, scopeIdx int, scopes ptrace.ScopeSpansSlice) map[string]any {
	key := [2]int{resourceIdx, scopeIdx}
	if cached, ok := t.scopeAttrCache.Load(key); ok {
		return cached
	}
	m := scopes.At(scopeIdx).Scope().Attributes().AsRaw()
	t.scopeAttrCache.Store(key, m)
	return m
}

func (t *TracesTraverser) spanAttrs(resourceIdx, scopeIdx, spanIdx int, spans ptrace.SpanSlice) map[string]any {
	key := [3]int{resourceIdx, scopeIdx, spanIdx}
	if cached, ok := t.spanAttrCache.Load(key); ok {
		return cached
	}
	m := spans.At(spanIdx).Attributes().AsRaw()
	t.spanAttrCache.Store(key, m)
	return m
}

func (t *TracesTraverser) Traverse(ctx context.Context, mappingVisitor MappingVisitor) {
	resourceSpans := t.traces.ResourceSpans()
	for resourceIdx := 0; resourceIdx < resourceSpans.Len(); resourceIdx++ {
		rs := resourceSpans.At(resourceIdx)
		resourceAttrs := t.resourceAttrs(resourceIdx, resourceSpans)
		resourceCtx := NewSpanEvalContext(nil, nil, resourceAttrs)

		if mappingVisitor.VisitResource(ctx, resourceCtx) == VisitSkip {
			continue
		}

		scopeSpans := rs.ScopeSpans()
		for scopeIdx := 0; scopeIdx < scopeSpans.Len(); scopeIdx++ {
			ss := scopeSpans.At(scopeIdx)
			scopeAttrs := t.scopeAttrs(resourceIdx, scopeIdx, scopeSpans)
			scopeCtx := NewSpanEvalContext(nil, scopeAttrs, resourceAttrs)
			if mappingVisitor.VisitScope(ctx, scopeCtx) == VisitSkip {
				continue
			}

			spans := ss.Spans()
			for spanIdx := 0; spanIdx < spans.Len(); spanIdx++ {
				spanAttrs := t.spanAttrs(resourceIdx, scopeIdx, spanIdx, spans)
				spanCtx := NewSpanEvalContext(spanAttrs, scopeAttrs, resourceAttrs)
				mappingVisitor.VisitSpan(ctx, spanCtx)
			}
		}
	}
}
