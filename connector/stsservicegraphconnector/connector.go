// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:lll
package servicegraphconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/servicegraphconnector"

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
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

//nolint:gochecknoglobals
var (
	DefaultLatencyHistogramBuckets = []float64{
		0.002, 0.004, 0.006, 0.008, 0.01, 0.05, 0.1, 0.2, 0.4, 0.8, 1, 1.4, 2, 5, 10, 15,
	}
)

type metricSeries struct {
	dimensions  pcommon.Map
	lastUpdated int64 // Used to remove stale series
}

func (m *metricSeries) SetLastUpdated(lastUpdated int64) {
	m.lastUpdated = lastUpdated
}

var _ processor.Traces = (*ServiceGraphConnector)(nil)

type ServiceGraphConnector struct {
	config          *Config
	logger          *zap.Logger
	metricsConsumer consumer.Metrics

	store *store.Store

	startTime time.Time

	// This mutex really shouldn't be public but it's leveraged in tests
	SeriesMutex                          sync.Mutex
	reqTotal                             map[string]int64
	reqFailedTotal                       map[string]int64
	reqClientDurationSecondsCount        map[string]uint64
	reqClientDurationSecondsSum          map[string]float64
	reqClientDurationSecondsBucketCounts map[string][]uint64
	reqServerDurationSecondsCount        map[string]uint64
	reqServerDurationSecondsSum          map[string]float64
	reqServerDurationSecondsBucketCounts map[string][]uint64
	reqDurationBounds                    []float64

	// This mutex really shouldn't be public but it's leveraged in tests
	MetricMutex sync.RWMutex
	keyToMetric map[string]metricSeries

	statDroppedSpans     metric.Int64Counter
	statTotalEdges       metric.Int64Counter
	statExpiredEdges     metric.Int64Counter
	statRescheduledEdges metric.Int64Counter

	shutdownCh chan any
}

func (s *ServiceGraphConnector) GetReqDurationBounds() []float64 {
	return s.reqDurationBounds
}

func (s *ServiceGraphConnector) GetReqTotal() map[string]int64 {
	return s.reqTotal
}

func (s *ServiceGraphConnector) SetExpire(t time.Time) {
	s.store.Expire(t)
}

func (s *ServiceGraphConnector) GetKeyToMetric() map[string]metricSeries { //nolint:revive
	return s.keyToMetric
}
func (s *ServiceGraphConnector) SetKeyToMetric(key string, metric metricSeries) {
	s.keyToMetric[key] = metric
}

func customMetricName(name string) string {
	return "connector/" + metadata.Type.String() + "/" + name
}

func NewConnector(set component.TelemetrySettings, config component.Config, nextConsumer consumer.Metrics) *ServiceGraphConnector {
	pConfig, ok := config.(*Config)

	if !ok {
		pConfig = &Config{}
	}

	bounds := DefaultLatencyHistogramBuckets
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

	return &ServiceGraphConnector{
		metricsConsumer:                      nextConsumer,
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

func (s *ServiceGraphConnector) Start(_ context.Context, host component.Host) error {
	s.store = store.NewStore(s.config.Store.TTL, s.config.Store.MaxItems, s.onComplete, s.onExpire, s.onReschedule)

	go s.metricFlushLoop(s.config.MetricsFlushInterval)

	go s.cacheLoop(s.config.CacheLoop)

	go s.storeExpirationLoop(s.config.StoreExpirationLoop)

	s.logger.Info("Started servicegraphconnector")
	return nil
}

func (s *ServiceGraphConnector) metricFlushLoop(flushInterval time.Duration) {
	if flushInterval <= 0 {
		return
	}

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.flushMetrics(context.Background()); err != nil {
				s.logger.Error("failed to flush metrics", zap.Error(err))
			}
		case <-s.shutdownCh:
			return
		}
	}
}

func (s *ServiceGraphConnector) flushMetrics(ctx context.Context) error {
	md, err := s.BuildMetrics()
	if err != nil {
		return fmt.Errorf("failed to build metrics: %w", err)
	}

	// Skip empty metrics.
	if md.MetricCount() == 0 {
		return nil
	}

	// Firstly, export md to avoid being impacted by downstream trace ServiceGraphConnector errors/latency.
	return s.metricsConsumer.ConsumeMetrics(ctx, md)
}

func (s *ServiceGraphConnector) Shutdown(_ context.Context) error {
	s.logger.Info("Shutting down servicegraphconnector")
	close(s.shutdownCh)
	return nil
}

func (s *ServiceGraphConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (s *ServiceGraphConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	if err := s.aggregateMetrics(ctx, td); err != nil {
		return fmt.Errorf("failed to aggregate metrics: %w", err)
	}

	// If metricsFlushInterval is not set, flush metrics immediately.
	if s.config.MetricsFlushInterval <= 0 {
		if err := s.flushMetrics(ctx); err != nil {
			// Not return error here to avoid impacting traces.
			s.logger.Error("failed to flush metrics", zap.Error(err))
		}
	}

	return nil
}

func (s *ServiceGraphConnector) aggregateMetrics(ctx context.Context, td ptrace.Traces) (err error) { //nolint:nonamedreturns
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
					isNew, err = s.store.UpsertEdge(key, func(e *store.Edge) {
						e.TraceID = traceID
						e.ConnectionType = connectionType
						e.ClientService = serviceName
						e.ClientLatencySec = spanDuration(span)
						e.Failed = e.Failed || span.Status().Code() == ptrace.StatusCodeError
						s.upsertDimensions(clientKind, e.Dimensions, rAttributes, span.Attributes())

						// A database request will only have one span, we don't wait for the server
						// span but just copy details from the client span
						if db, ok := FindDatabase(span.Attributes()); ok {
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
					isNew, err = s.store.UpsertEdge(key, func(e *store.Edge) {
						e.TraceID = traceID
						e.ConnectionType = connectionType
						e.ServerService = serviceName
						e.ServerLatencySec = spanDuration(span)
						e.Failed = e.Failed || span.Status().Code() == ptrace.StatusCodeError
						s.upsertDimensions(serverKind, e.Dimensions, rAttributes, span.Attributes())
					})
				case ptrace.SpanKindUnspecified:
					continue
				case ptrace.SpanKindInternal:
					continue
				default:
					// this span is not part of an edge
					continue
				}

				if errors.Is(err, store.ErrTooManyItems) {
					s.statDroppedSpans.Add(ctx, 1)
					continue
				}

				// UpsertEdge will only return ErrTooManyItems
				if err != nil {
					return err
				}

				if isNew {
					s.statTotalEdges.Add(ctx, 1)
				}
			}
		}
	}
	return nil
}

func (s *ServiceGraphConnector) upsertDimensions(kind string, m map[string]string, resourceAttr pcommon.Map, spanAttr pcommon.Map) {
	for _, dim := range s.config.Dimensions {
		if v, ok := findAttributeValue(dim, resourceAttr, spanAttr); ok {
			m[kind+"_"+dim] = v
		}
	}
}

func (s *ServiceGraphConnector) onComplete(e *store.Edge, logp float64) {
	s.logger.Debug(
		"edge completed",
		zap.String("client_service", e.ClientService),
		zap.String("server_service", e.ServerService),
		zap.String("connection_type", string(e.ConnectionType)),
		zap.Stringer("trace_id", e.TraceID),
	)
	s.aggregateMetricsForEdge(e, logp)
}

func (s *ServiceGraphConnector) onExpire(e *store.Edge) {
	s.logger.Debug(
		"edge expired",
		zap.String("client_service", e.ClientService),
		zap.String("server_service", e.ServerService),
		zap.String("connection_type", string(e.ConnectionType)),
		zap.Stringer("trace_id", e.TraceID),
	)

	s.statExpiredEdges.Add(context.Background(), 1)
}

func (s *ServiceGraphConnector) onReschedule(e *store.Edge) {
	s.logger.Debug(
		"edge rescheduled",
		zap.String("client_service", e.ClientService),
		zap.String("server_service", e.ServerService),
		zap.String("connection_type", string(e.ConnectionType)),
		zap.Stringer("trace_id", e.TraceID),
	)
	s.statRescheduledEdges.Add(context.Background(), 1)
}

func (s *ServiceGraphConnector) aggregateMetricsForEdge(e *store.Edge, logp float64) {
	metricKey := s.BuildMetricKey(e.ClientService, e.ServerService, string(e.ConnectionType), e.Dimensions)
	dimensions := buildDimensions(e)

	// logp is the (log) probability that the edge survived the wait for a matching span.
	// Compensate the metrics for the culling of equivalent edges.
	n := math.Exp(-logp)
	fraction := n - math.Floor(n)
	var count uint64
	if randFloat64() < fraction {
		count = uint64(math.Floor(n)) + 1
	} else {
		count = uint64(math.Floor(n))
	}

	s.SeriesMutex.Lock()
	defer s.SeriesMutex.Unlock()
	s.updateSeries(metricKey, dimensions)
	s.updateCountMetrics(metricKey, count)
	if e.Failed {
		s.updateErrorMetrics(metricKey, count)
	}
	s.UpdateDurationMetrics(metricKey, e.ServerLatencySec, e.ClientLatencySec, count)
}

func intn(maximum int64) int64 {
	nBig, err := rand.Int(rand.Reader, big.NewInt(maximum))
	if err != nil {
		panic(err)
	}
	return nBig.Int64()
}

func randFloat64() float64 {
	return float64(intn(1<<53)) / (1 << 53)
}

func (s *ServiceGraphConnector) updateSeries(key string, dimensions pcommon.Map) {
	s.MetricMutex.Lock()
	defer s.MetricMutex.Unlock()
	// Overwrite the series if it already exists
	s.keyToMetric[key] = metricSeries{
		dimensions:  dimensions,
		lastUpdated: time.Now().UnixMilli(),
	}
}

func (s *ServiceGraphConnector) dimensionsForSeries(key string) (pcommon.Map, bool) {
	s.MetricMutex.RLock()
	defer s.MetricMutex.RUnlock()
	if series, ok := s.keyToMetric[key]; ok {
		return series.dimensions, true
	}

	return pcommon.Map{}, false
}

func (s *ServiceGraphConnector) updateCountMetrics(key string, count uint64) {
	if count < math.MaxInt64 {
		s.reqTotal[key] += int64(count)
	}
}

func (s *ServiceGraphConnector) updateErrorMetrics(key string, count uint64) {
	if count < math.MaxInt64 {
		s.reqFailedTotal[key] += int64(count)
	}
}

func (s *ServiceGraphConnector) UpdateDurationMetrics(key string, serverDuration, clientDuration float64, count uint64) {
	s.updateServerDurationMetrics(key, serverDuration, count)
	s.updateClientDurationMetrics(key, clientDuration, count)
}

func (s *ServiceGraphConnector) updateServerDurationMetrics(key string, duration float64, count uint64) {
	index := sort.SearchFloat64s(s.reqDurationBounds, duration) // Search bucket index
	if _, ok := s.reqServerDurationSecondsBucketCounts[key]; !ok {
		s.reqServerDurationSecondsBucketCounts[key] = make([]uint64, len(s.reqDurationBounds)+1)
	}
	s.reqServerDurationSecondsSum[key] += duration * float64(count)
	s.reqServerDurationSecondsCount[key] += count
	s.reqServerDurationSecondsBucketCounts[key][index] += count
}

func (s *ServiceGraphConnector) updateClientDurationMetrics(key string, duration float64, count uint64) {
	index := sort.SearchFloat64s(s.reqDurationBounds, duration) // Search bucket index
	if _, ok := s.reqClientDurationSecondsBucketCounts[key]; !ok {
		s.reqClientDurationSecondsBucketCounts[key] = make([]uint64, len(s.reqDurationBounds)+1)
	}
	s.reqClientDurationSecondsSum[key] += duration * float64(count)
	s.reqClientDurationSecondsCount[key] += count
	s.reqClientDurationSecondsBucketCounts[key][index] += count
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

func (s *ServiceGraphConnector) BuildMetrics() (pmetric.Metrics, error) {
	m := pmetric.NewMetrics()
	ilm := m.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	ilm.Scope().SetName("traces_service_graph")

	// Obtain write lock to reset data
	s.SeriesMutex.Lock()
	defer s.SeriesMutex.Unlock()

	if err := s.collectCountMetrics(ilm); err != nil {
		return m, err
	}

	if err := s.collectLatencyMetrics(ilm); err != nil {
		return m, err
	}

	return m, nil
}

func (s *ServiceGraphConnector) collectCountMetrics(ilm pmetric.ScopeMetrics) error {
	for key, c := range s.reqTotal {
		mCount := ilm.Metrics().AppendEmpty()
		mCount.SetName("traces_service_graph_request_total")
		mCount.SetEmptySum().SetIsMonotonic(true)
		// TODO: Support other aggregation temporalities
		mCount.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		dpCalls := mCount.Sum().DataPoints().AppendEmpty()
		dpCalls.SetStartTimestamp(pcommon.NewTimestampFromTime(s.startTime))
		dpCalls.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dpCalls.SetIntValue(c)

		dimensions, ok := s.dimensionsForSeries(key)
		if !ok {
			return fmt.Errorf("failed to find dimensions for key %s", key)
		}

		dimensions.CopyTo(dpCalls.Attributes())
	}

	for key, c := range s.reqFailedTotal {
		mCount := ilm.Metrics().AppendEmpty()
		mCount.SetName("traces_service_graph_request_failed_total")
		mCount.SetEmptySum().SetIsMonotonic(true)
		// TODO: Support other aggregation temporalities
		mCount.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		dpCalls := mCount.Sum().DataPoints().AppendEmpty()
		dpCalls.SetStartTimestamp(pcommon.NewTimestampFromTime(s.startTime))
		dpCalls.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dpCalls.SetIntValue(c)

		dimensions, ok := s.dimensionsForSeries(key)
		if !ok {
			return fmt.Errorf("failed to find dimensions for key %s", key)
		}

		dimensions.CopyTo(dpCalls.Attributes())
	}

	return nil
}

func (s *ServiceGraphConnector) collectLatencyMetrics(ilm pmetric.ScopeMetrics) error {
	if err := s.collectServerLatencyMetrics(ilm, "traces_service_graph_request_server_seconds"); err != nil {
		return err
	}

	return s.collectClientLatencyMetrics(ilm)
}

func (s *ServiceGraphConnector) collectClientLatencyMetrics(ilm pmetric.ScopeMetrics) error {
	for key := range s.reqServerDurationSecondsCount {
		mDuration := ilm.Metrics().AppendEmpty()
		mDuration.SetName("traces_service_graph_request_client_seconds")
		// TODO: Support other aggregation temporalities
		mDuration.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		timestamp := pcommon.NewTimestampFromTime(time.Now())

		dpDuration := mDuration.Histogram().DataPoints().AppendEmpty()
		dpDuration.SetStartTimestamp(pcommon.NewTimestampFromTime(s.startTime))
		dpDuration.SetTimestamp(timestamp)
		dpDuration.ExplicitBounds().FromRaw(s.reqDurationBounds)
		dpDuration.BucketCounts().FromRaw(s.reqServerDurationSecondsBucketCounts[key])
		dpDuration.SetCount(s.reqServerDurationSecondsCount[key])
		dpDuration.SetSum(s.reqServerDurationSecondsSum[key])

		// TODO: Support exemplars
		dimensions, ok := s.dimensionsForSeries(key)
		if !ok {
			return fmt.Errorf("failed to find dimensions for key %s", key)
		}

		dimensions.CopyTo(dpDuration.Attributes())
	}
	return nil
}

func (s *ServiceGraphConnector) collectServerLatencyMetrics(ilm pmetric.ScopeMetrics, mName string) error {
	for key := range s.reqServerDurationSecondsCount {
		mDuration := ilm.Metrics().AppendEmpty()
		mDuration.SetName(mName)
		// TODO: Support other aggregation temporalities
		mDuration.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		timestamp := pcommon.NewTimestampFromTime(time.Now())

		dpDuration := mDuration.Histogram().DataPoints().AppendEmpty()
		dpDuration.SetStartTimestamp(pcommon.NewTimestampFromTime(s.startTime))
		dpDuration.SetTimestamp(timestamp)
		dpDuration.ExplicitBounds().FromRaw(s.reqDurationBounds)
		dpDuration.BucketCounts().FromRaw(s.reqClientDurationSecondsBucketCounts[key])
		dpDuration.SetCount(s.reqClientDurationSecondsCount[key])
		dpDuration.SetSum(s.reqClientDurationSecondsSum[key])

		// TODO: Support exemplars
		dimensions, ok := s.dimensionsForSeries(key)
		if !ok {
			return fmt.Errorf("failed to find dimensions for key %s", key)
		}

		dimensions.CopyTo(dpDuration.Attributes())
	}
	return nil
}

func (s *ServiceGraphConnector) BuildMetricKey(clientName, serverName, connectionType string, edgeDimensions map[string]string) string {
	var metricKey strings.Builder
	metricKey.WriteString(clientName + metricKeySeparator + serverName + metricKeySeparator + connectionType)

	metricKey.WriteString(metricKeySeparator + "client")
	for _, dimName := range s.config.Dimensions {
		dim, ok := edgeDimensions[clientKind+"_"+dimName]
		if !ok {
			continue
		}
		metricKey.WriteString(metricKeySeparator + dim)
	}
	metricKey.WriteString(metricKeySeparator + "server")
	for _, dimName := range s.config.Dimensions {
		dim, ok := edgeDimensions[serverName+"_"+dimName]
		if !ok {
			continue
		}
		metricKey.WriteString(metricKeySeparator + dim)
	}

	return metricKey.String()
}

// storeExpirationLoop periodically expires old entries from the store.
func (s *ServiceGraphConnector) storeExpirationLoop(d time.Duration) {
	t := time.NewTicker(d)
	for {
		select {
		case <-t.C:
			s.store.Expire(time.Now())
		case <-s.shutdownCh:
			return
		}
	}
}

// cacheLoop periodically cleans the cache
func (s *ServiceGraphConnector) cacheLoop(d time.Duration) {
	t := time.NewTicker(d)
	for {
		select {
		case <-t.C:
			s.CleanCache()
		case <-s.shutdownCh:
			return
		}
	}

}

// cleanCache removes series that have not been updated in 15 minutes
func (s *ServiceGraphConnector) CleanCache() {
	var staleSeries []string
	s.MetricMutex.RLock()
	for key, series := range s.keyToMetric {
		if series.lastUpdated+15*time.Minute.Milliseconds() < time.Now().UnixMilli() {
			staleSeries = append(staleSeries, key)
		}
	}
	s.MetricMutex.RUnlock()

	s.SeriesMutex.Lock()
	for _, key := range staleSeries {
		delete(s.reqTotal, key)
		delete(s.reqFailedTotal, key)
		delete(s.reqClientDurationSecondsCount, key)
		delete(s.reqClientDurationSecondsSum, key)
		delete(s.reqClientDurationSecondsBucketCounts, key)
		delete(s.reqServerDurationSecondsCount, key)
		delete(s.reqServerDurationSecondsSum, key)
		delete(s.reqServerDurationSecondsBucketCounts, key)
	}
	s.SeriesMutex.Unlock()

	s.MetricMutex.Lock()
	for _, key := range staleSeries {
		delete(s.keyToMetric, key)
	}
	s.MetricMutex.Unlock()
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

func FindDatabase(attrs pcommon.Map) (*string, bool) {
	dbSystem, dbSystemOk := findAttributeValue("db.system", attrs)

	if !dbSystemOk {
		return nil, false
	}

	if peerService, peerOk := findAttributeValue("peer.service", attrs); peerOk {
		return &peerService, true
	}

	if dbName, dbNameOk := findAttributeValue("db.name", attrs); dbNameOk {
		return &dbName, true
	}

	dbName := dbSystem
	if dbSystem == "redis" {
		if dbIndex, ok := findAttributeValue("db.redis.database_index", attrs); ok {
			dbName = fmt.Sprintf("redis %s", dbIndex)
		}
	}
	return &dbName, true
}

func BuildTestService() *ServiceGraphConnector {
	return &ServiceGraphConnector{
		reqTotal:                             make(map[string]int64),
		reqFailedTotal:                       make(map[string]int64),
		reqServerDurationSecondsSum:          make(map[string]float64),
		reqServerDurationSecondsCount:        make(map[string]uint64),
		reqServerDurationSecondsBucketCounts: make(map[string][]uint64),
		reqClientDurationSecondsSum:          make(map[string]float64),
		reqClientDurationSecondsCount:        make(map[string]uint64),
		reqClientDurationSecondsBucketCounts: make(map[string][]uint64),
		reqDurationBounds:                    DefaultLatencyHistogramBuckets,
		config: &Config{
			Dimensions: []string{},
		},
	}
}
