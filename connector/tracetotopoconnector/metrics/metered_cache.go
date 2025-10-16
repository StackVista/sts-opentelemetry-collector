package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric"
)

type MeteredCacheMetricsRecorder interface {
	IncHit()
	IncMiss()
	IncAdd()
	IncEvict()
	RecordSize(size int)
	Close()
}

type noopMeter struct{}

func (noopMeter) IncHit()        {}
func (noopMeter) IncMiss()       {}
func (noopMeter) IncAdd()        {}
func (noopMeter) IncEvict()      {}
func (noopMeter) RecordSize(int) {}
func (noopMeter) Close()         {}

type otelMeter struct {
	//nolint:containedctx
	ctx    context.Context
	cancel context.CancelFunc

	hitCounter   metric.Int64Counter
	missCounter  metric.Int64Counter
	addCounter   metric.Int64Counter
	evictCounter metric.Int64Counter

	sizeCounter metric.Int64UpDownCounter
}

func newOtelMeter(
	ctx context.Context,
	settings component.TelemetrySettings,
	name string,
	reportInterval time.Duration,
	cacheLenFunc func() int,
) *otelMeter {
	ctx, cancel := context.WithCancel(ctx)

	metricBaseName := fmt.Sprintf("connector.tracetotopo.%s", name)
	meter := settings.MeterProvider.Meter("meteredcache")

	hit, _ := meter.Int64Counter(metricBaseName + ".hits")
	miss, _ := meter.Int64Counter(metricBaseName + ".misses")
	add, _ := meter.Int64Counter(metricBaseName + ".adds")
	evict, _ := meter.Int64Counter(metricBaseName + ".evictions")
	size, _ := meter.Int64UpDownCounter(metricBaseName + ".size")

	om := &otelMeter{
		ctx:          ctx,
		cancel:       cancel,
		hitCounter:   hit,
		missCounter:  miss,
		addCounter:   add,
		evictCounter: evict,
		sizeCounter:  size,
	}

	if reportInterval > 0 {
		go func() {
			ticker := time.NewTicker(reportInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					om.RecordSize(cacheLenFunc())
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	return om
}

func (m *otelMeter) IncHit()   { m.hitCounter.Add(m.ctx, 1) }
func (m *otelMeter) IncMiss()  { m.missCounter.Add(m.ctx, 1) }
func (m *otelMeter) IncAdd()   { m.addCounter.Add(m.ctx, 1) }
func (m *otelMeter) IncEvict() { m.evictCounter.Add(m.ctx, 1) }
func (m *otelMeter) RecordSize(size int) {
	m.sizeCounter.Add(m.ctx, int64(size))
}
func (m *otelMeter) Close() { m.cancel() }

type MeteredCache[K comparable, V any] struct {
	cache           *expirable.LRU[K, V]
	metricsRecorder MeteredCacheMetricsRecorder
}

type MeteredCacheSettings struct {
	Name          string // metric name prefix
	EnableMetrics bool   // if false, metrics are disabled

	Size               int
	TTL                time.Duration // 0 disables eviction
	ReportSizeInterval time.Duration // 0 disables periodic reporting

	TelemetrySettings component.TelemetrySettings
}

// NewCache creates a new MeteredCache with the given settings and optional eviction callback.
//
// This is the standard constructor for production use. If metrics are enabled via
// MeteredCacheSettings.EnableMetrics, the cache will automatically register OpenTelemetry
// instruments (hits, misses, adds, evictions, and size).
//
// If metrics are disabled, a lightweight no-op meter is used to minimize performance overhead.
func NewCache[K comparable, V any](
	ctx context.Context,
	settings MeteredCacheSettings,
	onEvict func(K, V),
) *MeteredCache[K, V] {
	return NewCacheWithMeter(ctx, settings, onEvict, nil)
}

// NewCacheWithMeter creates a new MeteredCache with an explicitly provided metrics recorder.
//
// This is useful primarily for testing.
func NewCacheWithMeter[K comparable, V any](
	ctx context.Context,
	settings MeteredCacheSettings,
	onEvict func(K, V),
	recorder MeteredCacheMetricsRecorder,
) *MeteredCache[K, V] {
	var cache *expirable.LRU[K, V]
	var meter MeteredCacheMetricsRecorder

	// lazily create a reference to the cache size
	cacheLenRef := func() int {
		if cache == nil {
			return 0
		}
		return cache.Len()
	}

	switch {
	case recorder != nil:
		meter = recorder
	case settings.EnableMetrics:
		meter = newOtelMeter(ctx, settings.TelemetrySettings, settings.Name, settings.ReportSizeInterval, cacheLenRef)
	default:
		meter = noopMeter{}
	}

	cache = expirable.NewLRU[K, V](
		settings.Size,
		func(k K, v V) {
			if onEvict != nil {
				onEvict(k, v)
			}
			meter.IncEvict()
		},
		settings.TTL,
	)

	return &MeteredCache[K, V]{
		cache:           cache,
		metricsRecorder: meter,
	}
}

func (m *MeteredCache[K, V]) Add(key K, value V) {
	m.cache.Add(key, value)
	m.metricsRecorder.IncAdd()
}

func (m *MeteredCache[K, V]) Get(key K) (V, bool) {
	val, ok := m.cache.Get(key)
	if ok {
		m.metricsRecorder.IncHit()
	} else {
		m.metricsRecorder.IncMiss()
	}
	return val, ok
}

func (m *MeteredCache[K, V]) Close() {
	m.metricsRecorder.Close()
}

func (m *MeteredCache[K, V]) Len() int {
	return m.cache.Len()
}
