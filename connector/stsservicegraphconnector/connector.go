// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package servicegraphconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/servicegraphconnector"

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/stackvista/sts-opentelemetry-collector/connector/stsservicegraphconnector/internal/metadata"
	"github.com/stackvista/sts-opentelemetry-collector/connector/stsservicegraphconnector/internal/store"
)

const (
	metricKeySeparator = string(byte(0))
	clientKind         = "client"
	serverKind         = "server"
	peerKind           = "peer"
)

var (
	defaultLatencyHistogramBuckets = []float64{
		0.002, 0.004, 0.006, 0.008, 0.01, 0.05, 0.1, 0.2, 0.4, 0.8, 1, 1.4, 2, 5, 10, 15,
	}
)

type metricSeries struct {
	dimensions  pcommon.Map
	lastUpdated int64 // Used to remove stale series
}

var _ processor.Traces = (*serviceGraphConnector)(nil)

type serviceGraphConnector struct {
	config          *Config
	logger          *zap.Logger
	metricsConsumer consumer.Metrics

	store *store.Store

	startTime time.Time

	seriesMutex                          sync.Mutex
	reqTotal                             map[string]int64
	reqFailedTotal                       map[string]int64
	reqClientDurationSecondsCount        map[string]uint64
	reqClientDurationSecondsSum          map[string]float64
	reqClientDurationSecondsBucketCounts map[string][]uint64
	reqServerDurationSecondsCount        map[string]uint64
	reqServerDurationSecondsSum          map[string]float64
	reqServerDurationSecondsBucketCounts map[string][]uint64
	reqDurationBounds                    []float64

	metricMutex sync.RWMutex
	keyToMetric map[string]metricSeries

	statDroppedSpans     metric.Int64Counter
	statTotalEdges       metric.Int64Counter
	statExpiredEdges     metric.Int64Counter
	statRescheduledEdges metric.Int64Counter

	shutdownCh chan any
}

func customMetricName(name string) string {
	return "connector/" + metadata.Type.String() + "/" + name
}

func newConnector(set component.TelemetrySettings, config component.Config) *serviceGraphConnector {
	pConfig := config.(*Config)

	bounds := defaultLatencyHistogramBuckets
	if pConfig.LatencyHistogramBuckets != nil {
		bounds = mapDurationsToFloat(pConfig.LatencyHistogramBuckets)
	}

	if pConfig.CacheLoop <= 0 {
		pConfig.CacheLoop = time.Minute
	}

	if pConfig.StoreExpirationLoop <= 0 {
		pConfig.StoreExpirationLoop = 2 * time.Second
	}

	meter := metadata.Meter(set)

	droppedSpan, _ := meter.Int64Counter(
		customMetricName("dropped_spans"),
		metric.WithDescription("Number of spans dropped when trying to add edges"),
		metric.WithUnit("1"),
	)
	totalEdges, _ := meter.Int64Counter(
		customMetricName("total_edges"),
		metric.WithDescription("Total number of unique edges"),
		metric.WithUnit("1"),
	)
	expiredEdges, _ := meter.Int64Counter(
		customMetricName("expired_edges"),
		metric.WithDescription("Number of edges that expired before finding its matching span"),
		metric.WithUnit("1"),
	)
	rescheduledEdges, _ := meter.Int64Counter(
		customMetricName("rescheduled_edges"),
		metric.WithDescription("Number of edges that were rescheduled before finding its matching span"),
		metric.WithUnit("1"),
	)

	return &serviceGraphConnector{
		config:                               pConfig,
		logger:                               set.Logger,
		startTime:                            time.Now(),
		reqTotal:                             make(map[string]int64),
		reqFailedTotal:                       make(map[string]int64),
		reqClientDurationSecondsCount:        make(map[string]uint64),
		reqClientDurationSecondsSum:          make(map[string]float64),
		reqClientDurationSecondsBucketCounts: make(map[string][]uint64),
		reqServerDurationSecondsCount:        make(map[string]uint64),
		reqServerDurationSecondsSum:          make(map[string]float64),
		reqServerDurationSecondsBucketCounts: make(map[string][]uint64),
		reqDurationBounds:                    bounds,
		keyToMetric:                          make(map[string]metricSeries),
		shutdownCh:                           make(chan any),
		statDroppedSpans:                     droppedSpan,
		statTotalEdges:                       totalEdges,
		statExpiredEdges:                     expiredEdges,
		statRescheduledEdges:                 rescheduledEdges,
	}
}

type getExporters interface {
	GetExporters() map[component.DataType]map[component.ID]component.Component
}

func (p *serviceGraphConnector) Start(_ context.Context, host component.Host) error {
	p.store = store.NewStore(p.config.Store.TTL, p.config.Store.MaxItems, p.onComplete, p.onExpire, p.onReschedule)

	if p.metricsConsumer == nil {
		ge, ok := host.(getExporters)
		if !ok {
			return fmt.Errorf("unable to get exporters")
		}
		exporters := ge.GetExporters()

		// The available list of exporters come from any configured metrics pipelines' exporters.
		for k, exp := range exporters[component.DataTypeMetrics] {
			metricsExp, ok := exp.(exporter.Metrics)
			if k.String() == p.config.MetricsExporter && ok {
				p.metricsConsumer = metricsExp
				break
			}
		}

		if p.metricsConsumer == nil {
			return fmt.Errorf("failed to find metrics exporter: %s",
				p.config.MetricsExporter)
		}
	}

	go p.metricFlushLoop(p.config.MetricsFlushInterval)

	go p.cacheLoop(p.config.CacheLoop)

	go p.storeExpirationLoop(p.config.StoreExpirationLoop)

	p.logger.Info("Started servicegraphconnector")
	return nil
}

func (p *serviceGraphConnector) metricFlushLoop(flushInterval time.Duration) {
	if flushInterval <= 0 {
		return
	}

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := p.flushMetrics(context.Background()); err != nil {
				p.logger.Error("failed to flush metrics", zap.Error(err))
			}
		case <-p.shutdownCh:
			return
		}
	}
}

func (p *serviceGraphConnector) flushMetrics(ctx context.Context) error {
	md, err := p.buildMetrics()
	if err != nil {
		return fmt.Errorf("failed to build metrics: %w", err)
	}

	// Skip empty metrics.
	if md.MetricCount() == 0 {
		return nil
	}

	// Firstly, export md to avoid being impacted by downstream trace serviceGraphConnector errors/latency.
	return p.metricsConsumer.ConsumeMetrics(ctx, md)
}

func (p *serviceGraphConnector) Shutdown(_ context.Context) error {
	p.logger.Info("Shutting down servicegraphconnector")
	close(p.shutdownCh)
	return nil
}

func (p *serviceGraphConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *serviceGraphConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	if err := p.aggregateMetrics(ctx, td); err != nil {
		return fmt.Errorf("failed to aggregate metrics: %w", err)
	}

	// If metricsFlushInterval is not set, flush metrics immediately.
	if p.config.MetricsFlushInterval <= 0 {
		if err := p.flushMetrics(ctx); err != nil {
			// Not return error here to avoid impacting traces.
			p.logger.Error("failed to flush metrics", zap.Error(err))
		}
	}

	return nil
}

func (p *serviceGraphConnector) aggregateMetrics(ctx context.Context, td ptrace.Traces) (err error) {
	var (
		isNew bool
	)

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rSpans := rss.At(i)

		rAttributes := rSpans.Resource().Attributes()

		serviceName, ok := findServiceName(rAttributes)
		if !ok {
			// If service.name doesn't exist, skip processing this trace
			continue
		}

		scopeSpans := rSpans.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			spans := scopeSpans.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)

				connectionType := store.Unknown

				switch span.Kind() {
				case ptrace.SpanKindProducer:
					// override connection type and continue processing as span kind client
					connectionType = store.MessagingSystem
					fallthrough
				case ptrace.SpanKindClient:
					traceID := span.TraceID()
					key := store.NewKey(traceID, span.SpanID())
					isNew, err = p.store.UpsertEdge(key, func(e *store.Edge) {
						e.TraceID = traceID
						e.ConnectionType = connectionType
						e.ClientService = serviceName
						e.ClientLatencySec = spanDuration(span)
						e.Failed = e.Failed || span.Status().Code() == ptrace.StatusCodeError
						p.upsertDimensions(clientKind, e.Dimensions, rAttributes, span.Attributes())

						// A database request will only have one span, we don't wait for the server
						// span but just copy details from the client span
						if db, ok := findDatabase(span.Attributes()); ok {
							e.ConnectionType = store.Database
							e.ServerService = *db
							e.ServerLatencySec = spanDuration(span)
						}
					})
				case ptrace.SpanKindConsumer:
					// override connection type and continue processing as span kind server
					connectionType = store.MessagingSystem
					fallthrough
				case ptrace.SpanKindServer:
					traceID := span.TraceID()
					key := store.NewKey(traceID, span.ParentSpanID())
					isNew, err = p.store.UpsertEdge(key, func(e *store.Edge) {
						e.TraceID = traceID
						e.ConnectionType = connectionType
						e.ServerService = serviceName
						e.ServerLatencySec = spanDuration(span)
						e.Failed = e.Failed || span.Status().Code() == ptrace.StatusCodeError
						p.upsertDimensions(serverKind, e.Dimensions, rAttributes, span.Attributes())
					})
				default:
					// this span is not part of an edge
					continue
				}

				if errors.Is(err, store.ErrTooManyItems) {
					p.statDroppedSpans.Add(ctx, 1)
					continue
				}

				// UpsertEdge will only return ErrTooManyItems
				if err != nil {
					return err
				}

				if isNew {
					p.statTotalEdges.Add(ctx, 1)
				}
			}
		}
	}
	return nil
}

func (p *serviceGraphConnector) upsertDimensions(kind string, m map[string]string, resourceAttr pcommon.Map, spanAttr pcommon.Map) {
	for _, dim := range p.config.Dimensions {
		if v, ok := findAttributeValue(dim, resourceAttr, spanAttr); ok {
			m[kind+"_"+dim] = v
		}
	}
}

func (p *serviceGraphConnector) onComplete(e *store.Edge, logp float64) {
	p.logger.Debug(
		"edge completed",
		zap.String("client_service", e.ClientService),
		zap.String("server_service", e.ServerService),
		zap.String("connection_type", string(e.ConnectionType)),
		zap.Stringer("trace_id", e.TraceID),
	)
	p.aggregateMetricsForEdge(e, logp)
}

func (p *serviceGraphConnector) onExpire(e *store.Edge) {
	p.logger.Debug(
		"edge expired",
		zap.String("client_service", e.ClientService),
		zap.String("server_service", e.ServerService),
		zap.String("connection_type", string(e.ConnectionType)),
		zap.Stringer("trace_id", e.TraceID),
	)

	p.statExpiredEdges.Add(context.Background(), 1)
}

func (p *serviceGraphConnector) onReschedule(e *store.Edge) {
	p.logger.Debug(
		"edge rescheduled",
		zap.String("client_service", e.ClientService),
		zap.String("server_service", e.ServerService),
		zap.String("connection_type", string(e.ConnectionType)),
		zap.Stringer("trace_id", e.TraceID),
	)
	p.statRescheduledEdges.Add(context.Background(), 1)
}

func (p *serviceGraphConnector) aggregateMetricsForEdge(e *store.Edge, logp float64) {
	metricKey := p.buildMetricKey(e.ClientService, e.ServerService, string(e.ConnectionType), e.Dimensions)
	dimensions := buildDimensions(e)

	// logp is the (log) probability that the edge survived the wait for a matching span.
	// Compensate the metrics for the culling of equivalent edges.
	n := math.Exp(-logp)
	fraction := n - math.Floor(n)
	var count uint64
	if rand.Float64() < fraction {
		count = uint64(math.Floor(n)) + 1
	} else {
		count = uint64(math.Floor(n))
	}

	p.seriesMutex.Lock()
	defer p.seriesMutex.Unlock()
	p.updateSeries(metricKey, dimensions)
	p.updateCountMetrics(metricKey, count)
	if e.Failed {
		p.updateErrorMetrics(metricKey, count)
	}
	p.updateDurationMetrics(metricKey, e.ServerLatencySec, e.ClientLatencySec, count)
}

func (p *serviceGraphConnector) updateSeries(key string, dimensions pcommon.Map) {
	p.metricMutex.Lock()
	defer p.metricMutex.Unlock()
	// Overwrite the series if it already exists
	p.keyToMetric[key] = metricSeries{
		dimensions:  dimensions,
		lastUpdated: time.Now().UnixMilli(),
	}
}

func (p *serviceGraphConnector) dimensionsForSeries(key string) (pcommon.Map, bool) {
	p.metricMutex.RLock()
	defer p.metricMutex.RUnlock()
	if series, ok := p.keyToMetric[key]; ok {
		return series.dimensions, true
	}

	return pcommon.Map{}, false
}

func (p *serviceGraphConnector) updateCountMetrics(key string, count uint64) {
	p.reqTotal[key] += int64(count)
}

func (p *serviceGraphConnector) updateErrorMetrics(key string, count uint64) {
	p.reqFailedTotal[key] += int64(count)
}

func (p *serviceGraphConnector) updateDurationMetrics(key string, serverDuration, clientDuration float64, count uint64) {
	p.updateServerDurationMetrics(key, serverDuration, count)
	p.updateClientDurationMetrics(key, clientDuration, count)
}

func (p *serviceGraphConnector) updateServerDurationMetrics(key string, duration float64, count uint64) {
	index := sort.SearchFloat64s(p.reqDurationBounds, duration) // Search bucket index
	if _, ok := p.reqServerDurationSecondsBucketCounts[key]; !ok {
		p.reqServerDurationSecondsBucketCounts[key] = make([]uint64, len(p.reqDurationBounds)+1)
	}
	p.reqServerDurationSecondsSum[key] += duration * float64(count)
	p.reqServerDurationSecondsCount[key] += count
	p.reqServerDurationSecondsBucketCounts[key][index] += count
}

func (p *serviceGraphConnector) updateClientDurationMetrics(key string, duration float64, count uint64) {
	index := sort.SearchFloat64s(p.reqDurationBounds, duration) // Search bucket index
	if _, ok := p.reqClientDurationSecondsBucketCounts[key]; !ok {
		p.reqClientDurationSecondsBucketCounts[key] = make([]uint64, len(p.reqDurationBounds)+1)
	}
	p.reqClientDurationSecondsSum[key] += duration * float64(count)
	p.reqClientDurationSecondsCount[key] += count
	p.reqClientDurationSecondsBucketCounts[key][index] += count
}

func buildDimensions(e *store.Edge) pcommon.Map {
	dims := pcommon.NewMap()
	dims.PutStr("client", e.ClientService)
	dims.PutStr("server", e.ServerService)
	dims.PutStr("connection_type", string(e.ConnectionType))
	dims.PutBool("failed", e.Failed)
	for k, v := range e.Dimensions {
		dims.PutStr(k, v)
	}
	return dims
}

func (p *serviceGraphConnector) buildMetrics() (pmetric.Metrics, error) {
	m := pmetric.NewMetrics()
	ilm := m.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	ilm.Scope().SetName("traces_service_graph")

	// Obtain write lock to reset data
	p.seriesMutex.Lock()
	defer p.seriesMutex.Unlock()

	if err := p.collectCountMetrics(ilm); err != nil {
		return m, err
	}

	if err := p.collectLatencyMetrics(ilm); err != nil {
		return m, err
	}

	return m, nil
}

func (p *serviceGraphConnector) collectCountMetrics(ilm pmetric.ScopeMetrics) error {
	for key, c := range p.reqTotal {
		mCount := ilm.Metrics().AppendEmpty()
		mCount.SetName("traces_service_graph_request_total")
		mCount.SetEmptySum().SetIsMonotonic(true)
		// TODO: Support other aggregation temporalities
		mCount.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		dpCalls := mCount.Sum().DataPoints().AppendEmpty()
		dpCalls.SetStartTimestamp(pcommon.NewTimestampFromTime(p.startTime))
		dpCalls.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dpCalls.SetIntValue(c)

		dimensions, ok := p.dimensionsForSeries(key)
		if !ok {
			return fmt.Errorf("failed to find dimensions for key %s", key)
		}

		dimensions.CopyTo(dpCalls.Attributes())
	}

	for key, c := range p.reqFailedTotal {
		mCount := ilm.Metrics().AppendEmpty()
		mCount.SetName("traces_service_graph_request_failed_total")
		mCount.SetEmptySum().SetIsMonotonic(true)
		// TODO: Support other aggregation temporalities
		mCount.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		dpCalls := mCount.Sum().DataPoints().AppendEmpty()
		dpCalls.SetStartTimestamp(pcommon.NewTimestampFromTime(p.startTime))
		dpCalls.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dpCalls.SetIntValue(c)

		dimensions, ok := p.dimensionsForSeries(key)
		if !ok {
			return fmt.Errorf("failed to find dimensions for key %s", key)
		}

		dimensions.CopyTo(dpCalls.Attributes())
	}

	return nil
}

func (p *serviceGraphConnector) collectLatencyMetrics(ilm pmetric.ScopeMetrics) error {
	if err := p.collectServerLatencyMetrics(ilm, "traces_service_graph_request_server_seconds"); err != nil {
		return err
	}

	return p.collectClientLatencyMetrics(ilm)
}

func (p *serviceGraphConnector) collectClientLatencyMetrics(ilm pmetric.ScopeMetrics) error {
	for key := range p.reqServerDurationSecondsCount {
		mDuration := ilm.Metrics().AppendEmpty()
		mDuration.SetName("traces_service_graph_request_client_seconds")
		// TODO: Support other aggregation temporalities
		mDuration.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		timestamp := pcommon.NewTimestampFromTime(time.Now())

		dpDuration := mDuration.Histogram().DataPoints().AppendEmpty()
		dpDuration.SetStartTimestamp(pcommon.NewTimestampFromTime(p.startTime))
		dpDuration.SetTimestamp(timestamp)
		dpDuration.ExplicitBounds().FromRaw(p.reqDurationBounds)
		dpDuration.BucketCounts().FromRaw(p.reqServerDurationSecondsBucketCounts[key])
		dpDuration.SetCount(p.reqServerDurationSecondsCount[key])
		dpDuration.SetSum(p.reqServerDurationSecondsSum[key])

		// TODO: Support exemplars
		dimensions, ok := p.dimensionsForSeries(key)
		if !ok {
			return fmt.Errorf("failed to find dimensions for key %s", key)
		}

		dimensions.CopyTo(dpDuration.Attributes())
	}
	return nil
}

func (p *serviceGraphConnector) collectServerLatencyMetrics(ilm pmetric.ScopeMetrics, mName string) error {
	for key := range p.reqServerDurationSecondsCount {
		mDuration := ilm.Metrics().AppendEmpty()
		mDuration.SetName(mName)
		// TODO: Support other aggregation temporalities
		mDuration.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		timestamp := pcommon.NewTimestampFromTime(time.Now())

		dpDuration := mDuration.Histogram().DataPoints().AppendEmpty()
		dpDuration.SetStartTimestamp(pcommon.NewTimestampFromTime(p.startTime))
		dpDuration.SetTimestamp(timestamp)
		dpDuration.ExplicitBounds().FromRaw(p.reqDurationBounds)
		dpDuration.BucketCounts().FromRaw(p.reqClientDurationSecondsBucketCounts[key])
		dpDuration.SetCount(p.reqClientDurationSecondsCount[key])
		dpDuration.SetSum(p.reqClientDurationSecondsSum[key])

		// TODO: Support exemplars
		dimensions, ok := p.dimensionsForSeries(key)
		if !ok {
			return fmt.Errorf("failed to find dimensions for key %s", key)
		}

		dimensions.CopyTo(dpDuration.Attributes())
	}
	return nil
}

func (p *serviceGraphConnector) buildMetricKey(clientName, serverName, connectionType string, edgeDimensions map[string]string) string {
	var metricKey strings.Builder
	metricKey.WriteString(clientName + metricKeySeparator + serverName + metricKeySeparator + connectionType)

	metricKey.WriteString(metricKeySeparator + "client")
	for _, dimName := range p.config.Dimensions {
		dim, ok := edgeDimensions[clientKind+"_"+dimName]
		if !ok {
			continue
		}
		metricKey.WriteString(metricKeySeparator + dim)
	}
	metricKey.WriteString(metricKeySeparator + "server")
	for _, dimName := range p.config.Dimensions {
		dim, ok := edgeDimensions[serverName+"_"+dimName]
		if !ok {
			continue
		}
		metricKey.WriteString(metricKeySeparator + dim)
	}

	return metricKey.String()
}

// storeExpirationLoop periodically expires old entries from the store.
func (p *serviceGraphConnector) storeExpirationLoop(d time.Duration) {
	t := time.NewTicker(d)
	for {
		select {
		case <-t.C:
			p.store.Expire(time.Now())
		case <-p.shutdownCh:
			return
		}
	}
}

// cacheLoop periodically cleans the cache
func (p *serviceGraphConnector) cacheLoop(d time.Duration) {
	t := time.NewTicker(d)
	for {
		select {
		case <-t.C:
			p.cleanCache()
		case <-p.shutdownCh:
			return
		}
	}

}

// cleanCache removes series that have not been updated in 15 minutes
func (p *serviceGraphConnector) cleanCache() {
	var staleSeries []string
	p.metricMutex.RLock()
	for key, series := range p.keyToMetric {
		if series.lastUpdated+15*time.Minute.Milliseconds() < time.Now().UnixMilli() {
			staleSeries = append(staleSeries, key)
		}
	}
	p.metricMutex.RUnlock()

	p.seriesMutex.Lock()
	for _, key := range staleSeries {
		delete(p.reqTotal, key)
		delete(p.reqFailedTotal, key)
		delete(p.reqClientDurationSecondsCount, key)
		delete(p.reqClientDurationSecondsSum, key)
		delete(p.reqClientDurationSecondsBucketCounts, key)
		delete(p.reqServerDurationSecondsCount, key)
		delete(p.reqServerDurationSecondsSum, key)
		delete(p.reqServerDurationSecondsBucketCounts, key)
	}
	p.seriesMutex.Unlock()

	p.metricMutex.Lock()
	for _, key := range staleSeries {
		delete(p.keyToMetric, key)
	}
	p.metricMutex.Unlock()
}

// spanDuration returns the duration of the given span in seconds (legacy ms).
func spanDuration(span ptrace.Span) float64 {
	return float64(span.EndTimestamp()-span.StartTimestamp()) / float64(time.Second.Nanoseconds())
}

// durationToFloat converts the given duration to the number of seconds (legacy ms) it represents.
func durationToFloat(d time.Duration) float64 {
	return d.Seconds()
}

func mapDurationsToFloat(vs []time.Duration) []float64 {
	vsm := make([]float64, len(vs))
	for i, v := range vs {
		vsm[i] = durationToFloat(v)
	}
	return vsm
}

func findDatabase(attrs pcommon.Map) (*string, bool) {
	dbSystem, dbSystemOk := findAttributeValue("db.system", attrs)
	if !dbSystemOk {
		return nil, false
	}
	if peerService, peerOk := findAttributeValue("peer.service", attrs); peerOk {
		return &peerService, true
	} else {
		if dbName, dbNameOk := findAttributeValue("db.name", attrs); dbNameOk {
			return &dbName, true
		} else {
			dbName := dbSystem
			if dbSystem == "redis" {
				if dbIndex, ok := findAttributeValue("db.redis.database_index", attrs); ok {
					dbName = fmt.Sprintf("redis %s", dbIndex)
				}
			}
			return &dbName, true
		}
	}
}
