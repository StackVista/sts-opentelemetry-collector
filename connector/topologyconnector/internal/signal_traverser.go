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

type objectCache[K comparable, V OtelDataType] struct {
	m sync.Map // map[K]V
}

func (c *objectCache[K, V]) Load(key K) (V, bool) {
	v, ok := c.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}

	val, ok := v.(V)
	if !ok { // Should never happen unless the cache is misused
		var zero V
		return zero, false
	}

	return val, true
}

func (c *objectCache[K, V]) Store(key K, val V) {
	c.m.Store(key, val)
}

// ------------------------
// Metric traversal
// ------------------------

// MetricsTraverser provides a visitor-based traversal over OpenTelemetry metric data.
// It caches models/structs resembling the Otel data at various levels to avoid repeated conversions and reduce
// allocation overhead during traversal.
//
// Fields:
//
//	metricData - the raw pmetric.Metrics to traverse
//	resources  - caches resources by resourceIndex
//	scopes     - caches scopes by [resourceIndex, scopeIndex]
//	metrics    - caches metrics by [resourceIndex, scopeIndex, metricIndex]
//	datapoints - caches datapoints by [resourceIndex, scopeIndex, metricIndex, datapointIndex]
type MetricsTraverser struct {
	metricData pmetric.Metrics

	resources  objectCache[int, *Resource]     // map[int]Resource
	scopes     objectCache[[2]int, *Scope]     // map[[2]int]Scope
	metrics    objectCache[[3]int, *Metric]    // map[[3]int]Metric
	datapoints objectCache[[4]int, *Datapoint] // map[[4]int]Datapoint
}

type SignalTraverser interface {
	Traverse(ctx context.Context, mappingVisitor MappingVisitor)
}

func NewMetricsTraverser(metricData pmetric.Metrics) *MetricsTraverser {
	return &MetricsTraverser{
		metricData: metricData,
	}
}

func (t *MetricsTraverser) resource(resourceIdx int, rm pmetric.ResourceMetricsSlice) *Resource {
	if cached, ok := t.resources.Load(resourceIdx); ok {
		return cached
	}
	attrs := rm.At(resourceIdx).Resource().Attributes().AsRaw()
	r := NewResource(attrs)
	t.resources.Store(resourceIdx, r)
	return r
}

func (t *MetricsTraverser) scope(resourceIdx, scopeIdx int, sm pmetric.ScopeMetricsSlice) *Scope {
	key := [2]int{resourceIdx, scopeIdx}
	if cached, ok := t.scopes.Load(key); ok {
		return cached
	}
	scope := sm.At(scopeIdx).Scope()
	s := NewScope(scope.Name(), scope.Version(), scope.Attributes().AsRaw())
	t.scopes.Store(key, s)
	return s
}

func (t *MetricsTraverser) metric(resourceIdx, scopeIdx, metricIdx int, m pmetric.Metric) *Metric {
	key := [3]int{resourceIdx, scopeIdx, metricIdx}
	if cached, ok := t.metrics.Load(key); ok {
		return cached
	}
	metric := NewMetric(m.Name(), m.Description(), m.Unit())
	t.metrics.Store(key, metric)
	return metric
}

type attrProvider interface {
	Attributes() pcommon.Map
}

func (t *MetricsTraverser) datapoint(
	resourceIdx, scopeIdx, metricIdx, datapointIdx int, dp attrProvider,
) *Datapoint {
	key := [4]int{resourceIdx, scopeIdx, metricIdx, datapointIdx}
	if cached, ok := t.datapoints.Load(key); ok {
		return cached
	}
	d := NewDatapoint(dp.Attributes().AsRaw())
	t.datapoints.Store(key, d)
	return d
}

func (t *MetricsTraverser) Traverse(ctx context.Context, mappingVisitor MappingVisitor) {
	rmSlice := t.metricData.ResourceMetrics()
	for resourceIdx := 0; resourceIdx < rmSlice.Len(); resourceIdx++ {
		resource := t.resource(resourceIdx, rmSlice)
		rm := rmSlice.At(resourceIdx)

		resourceCtx := NewMetricEvalContext(nil, nil, nil, resource)
		if mappingVisitor.VisitResource(ctx, resourceCtx) == VisitSkip {
			continue
		}

		smSlice := rm.ScopeMetrics()
		for scopeIdx := 0; scopeIdx < smSlice.Len(); scopeIdx++ {
			scopeAttrs := t.scope(resourceIdx, scopeIdx, smSlice)
			sm := smSlice.At(scopeIdx)

			scope := NewMetricEvalContext(nil, nil, scopeAttrs, resource)
			if mappingVisitor.VisitScope(ctx, scope) == VisitSkip {
				continue
			}

			msSlice := sm.Metrics()
			for metricIdx := 0; metricIdx < msSlice.Len(); metricIdx++ {
				m := msSlice.At(metricIdx)
				metric := t.metric(resourceIdx, scopeIdx, metricIdx, m)

				if mappingVisitor.VisitMetric(
					ctx, NewMetricEvalContext(nil, metric, scopeAttrs, resource)) == VisitSkip {
					continue
				}

				switch m.Type() {
				case pmetric.MetricTypeEmpty:
					continue
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					for datapointIdx := dps.Len() - 1; datapointIdx >= 0; datapointIdx-- {
						dp := dps.At(datapointIdx)
						datapoint := t.datapoint(resourceIdx, scopeIdx, metricIdx, datapointIdx, dp)
						dpCtx := NewMetricEvalContext(datapoint, metric, scopeAttrs, resource)
						mappingVisitor.VisitDatapoint(ctx, dpCtx)
					}
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					for datapointIdx := dps.Len() - 1; datapointIdx >= 0; datapointIdx-- {
						dp := dps.At(datapointIdx)
						datapoint := t.datapoint(resourceIdx, scopeIdx, metricIdx, datapointIdx, dp)
						dpCtx := NewMetricEvalContext(datapoint, metric, scopeAttrs, resource)
						mappingVisitor.VisitDatapoint(ctx, dpCtx)
					}
				case pmetric.MetricTypeHistogram:
					dps := m.Histogram().DataPoints()
					for datapointIdx := dps.Len() - 1; datapointIdx >= 0; datapointIdx-- {
						dp := dps.At(datapointIdx)
						datapoint := t.datapoint(resourceIdx, scopeIdx, metricIdx, datapointIdx, dp)
						dpCtx := NewMetricEvalContext(datapoint, metric, scopeAttrs, resource)
						mappingVisitor.VisitDatapoint(ctx, dpCtx)
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := m.ExponentialHistogram().DataPoints()
					for datapointIdx := dps.Len() - 1; datapointIdx >= 0; datapointIdx-- {
						dp := dps.At(datapointIdx)
						datapoint := t.datapoint(resourceIdx, scopeIdx, metricIdx, datapointIdx, dp)
						dpCtx := NewMetricEvalContext(datapoint, metric, scopeAttrs, resource)
						mappingVisitor.VisitDatapoint(ctx, dpCtx)
					}
				case pmetric.MetricTypeSummary:
					dps := m.Summary().DataPoints()
					for datapointIdx := dps.Len() - 1; datapointIdx >= 0; datapointIdx-- {
						dp := dps.At(datapointIdx)
						datapoint := t.datapoint(resourceIdx, scopeIdx, metricIdx, datapointIdx, dp)
						dpCtx := NewMetricEvalContext(datapoint, metric, scopeAttrs, resource)
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
//	resources - caches resources by resourceIndex
//	scopes    - caches scopes by [resourceIndex, scopeIndex]
//	spans     - caches spans by [resourceIndex, scopeIndex, spanIndex]
type TracesTraverser struct {
	traces ptrace.Traces

	resources objectCache[int, *Resource] // map[int]Resource
	scopes    objectCache[[2]int, *Scope] // map[[2]int]Scope
	spans     objectCache[[3]int, *Span]  // map[[3]int]Span
}

func NewTracesTraverser(traces ptrace.Traces) *TracesTraverser {
	return &TracesTraverser{
		traces: traces,
	}
}

func (t *TracesTraverser) resource(resourceIdx int, resources ptrace.ResourceSpansSlice) *Resource {
	if cached, ok := t.resources.Load(resourceIdx); ok {
		return cached
	}
	attrs := resources.At(resourceIdx).Resource().Attributes().AsRaw()
	r := NewResource(attrs)
	t.resources.Store(resourceIdx, r)
	return r
}

func (t *TracesTraverser) scope(resourceIdx, scopeIdx int, scopes ptrace.ScopeSpansSlice) *Scope {
	key := [2]int{resourceIdx, scopeIdx}
	if cached, ok := t.scopes.Load(key); ok {
		return cached
	}
	scope := scopes.At(scopeIdx).Scope()
	s := NewScope(scope.Name(), scope.Version(), scope.Attributes().AsRaw())
	t.scopes.Store(key, s)
	return s
}

func (t *TracesTraverser) span(resourceIdx, scopeIdx, spanIdx int, spans ptrace.SpanSlice) *Span {
	key := [3]int{resourceIdx, scopeIdx, spanIdx}
	if cached, ok := t.spans.Load(key); ok {
		return cached
	}
	span := spans.At(spanIdx)
	s := NewSpan(
		span.Name(),
		span.Kind().String(),
		span.Status().Code().String(),
		span.Status().Message(),
		span.Attributes().AsRaw(),
	)
	t.spans.Store(key, s)
	return s
}

func (t *TracesTraverser) Traverse(ctx context.Context, mappingVisitor MappingVisitor) {
	resourceSpans := t.traces.ResourceSpans()
	for resourceIdx := 0; resourceIdx < resourceSpans.Len(); resourceIdx++ {
		rs := resourceSpans.At(resourceIdx)
		resource := t.resource(resourceIdx, resourceSpans)
		resourceCtx := NewSpanEvalContext(nil, nil, resource)

		if mappingVisitor.VisitResource(ctx, resourceCtx) == VisitSkip {
			continue
		}

		scopeSpans := rs.ScopeSpans()
		for scopeIdx := 0; scopeIdx < scopeSpans.Len(); scopeIdx++ {
			ss := scopeSpans.At(scopeIdx)
			scope := t.scope(resourceIdx, scopeIdx, scopeSpans)
			scopeCtx := NewSpanEvalContext(nil, scope, resource)
			if mappingVisitor.VisitScope(ctx, scopeCtx) == VisitSkip {
				continue
			}

			spans := ss.Spans()
			for spanIdx := 0; spanIdx < spans.Len(); spanIdx++ {
				span := t.span(resourceIdx, scopeIdx, spanIdx, spans)
				spanCtx := NewSpanEvalContext(span, scope, resource)
				mappingVisitor.VisitSpan(ctx, spanCtx)
			}
		}
	}
}
