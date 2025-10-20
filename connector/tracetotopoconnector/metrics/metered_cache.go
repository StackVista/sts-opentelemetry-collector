package metrics

import (
	"context"
	"fmt"
	"sync"
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
}

type noopMeter struct{}

func (noopMeter) IncHit()   {}
func (noopMeter) IncMiss()  {}
func (noopMeter) IncAdd()   {}
func (noopMeter) IncEvict() {}

type otelMeter struct {
	//nolint:containedctx
	ctx    context.Context
	cancel context.CancelFunc

	hitCounter   metric.Int64Counter
	missCounter  metric.Int64Counter
	addCounter   metric.Int64Counter
	evictCounter metric.Int64Counter
}

func newOtelMeter(
	ctx context.Context,
	settings component.TelemetrySettings,
	name string,
) *otelMeter {
	ctx, cancel := context.WithCancel(ctx)

	metricBaseName := fmt.Sprintf("connector.tracetotopo.%s", name)
	meter := settings.MeterProvider.Meter("meteredcache")

	hit, _ := meter.Int64Counter(metricBaseName + ".hits")
	miss, _ := meter.Int64Counter(metricBaseName + ".misses")
	add, _ := meter.Int64Counter(metricBaseName + ".adds")
	evict, _ := meter.Int64Counter(metricBaseName + ".evictions")

	om := &otelMeter{
		ctx:          ctx,
		cancel:       cancel,
		hitCounter:   hit,
		missCounter:  miss,
		addCounter:   add,
		evictCounter: evict,
	}

	return om
}

func (m *otelMeter) IncHit()   { m.hitCounter.Add(m.ctx, 1) }
func (m *otelMeter) IncMiss()  { m.missCounter.Add(m.ctx, 1) }
func (m *otelMeter) IncAdd()   { m.addCounter.Add(m.ctx, 1) }
func (m *otelMeter) IncEvict() { m.evictCounter.Add(m.ctx, 1) }
func (m *otelMeter) Close()    { m.cancel() }

type MeteredCache[K comparable, V any] struct {
	cache *expirable.LRU[K, V]

	metricsLock     sync.Mutex // protects metrics consistency
	metricsRecorder MeteredCacheMetricsRecorder
}

type MeteredCacheSettings struct {
	Name          string // metric name prefix
	EnableMetrics bool   // if false, metrics are disabled

	Size int
	TTL  time.Duration // 0 disables eviction

	TelemetrySettings component.TelemetrySettings
}

// NewCache creates a new MeteredCache with the given settings and optional eviction callback.
//
// This is the standard constructor for production use. If metrics are enabled via
// MeteredCacheSettings.EnableMetrics, the cache will automatically register OpenTelemetry
// instruments (hits, misses, adds and evictions).
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

	switch {
	case recorder != nil:
		meter = recorder
	case settings.EnableMetrics:
		meter = newOtelMeter(ctx, settings.TelemetrySettings, settings.Name)
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
		metricsLock:     sync.Mutex{},
	}
}

func (m *MeteredCache[K, V]) Add(key K, value V) {
	m.metricsLock.Lock()
	defer m.metricsLock.Unlock()

	_, existed := m.cache.Peek(key)
	m.cache.Add(key, value) //

	if !existed {
		m.metricsRecorder.IncAdd()
	}
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

func (m *MeteredCache[K, V]) Len() int {
	return m.cache.Len()
}
