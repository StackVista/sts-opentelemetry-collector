// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package servicegraphconnector_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	servicegraphconnector "github.com/stackvista/sts-opentelemetry-collector/connector/stsservicegraphconnector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/collector/semconv/v1.25.0"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	"go.uber.org/zap/zaptest"
)

func TestConnectorStart(t *testing.T) {
	// Create servicegraph connector
	factory := servicegraphconnector.NewFactory()
	cfg, ok := factory.CreateDefaultConfig().(*servicegraphconnector.Config)
	assert.True(t, ok)

	procCreationParams := connectortest.NewNopCreateSettings()
	traceConnector, err := factory.CreateTracesToMetrics(context.Background(), procCreationParams, cfg, consumertest.NewNop())
	require.NoError(t, err)

	// Test
	smp, ok := traceConnector.(*servicegraphconnector.ServiceGraphConnector)
	assert.True(t, ok)

	err = smp.Start(context.Background(), componenttest.NewNopHost())
	defer require.NoError(t, smp.Shutdown(context.Background()))

	// Verify
	assert.NoError(t, err)
}

func TestConnectorShutdown(t *testing.T) {
	// Prepare
	factory := servicegraphconnector.NewFactory()
	cfg, ok := factory.CreateDefaultConfig().(*servicegraphconnector.Config)
	assert.True(t, ok)

	// Test
	next := new(consumertest.MetricsSink)
	set := componenttest.NewNopTelemetrySettings()
	set.Logger = zaptest.NewLogger(t)
	p := servicegraphconnector.NewConnector(set, cfg)
	p.SetMetricsConsumer(next)
	err := p.Shutdown(context.Background())

	// Verify
	assert.NoError(t, err)
}

func TestConnectorConsume(t *testing.T) {
	// Prepare
	cfg := &servicegraphconnector.Config{
		Dimensions: []string{"some-attribute", "non-existing-attribute"},
		Store:      servicegraphconnector.StoreConfig{MaxItems: 10},
	}

	set := componenttest.NewNopTelemetrySettings()
	set.Logger = zaptest.NewLogger(t)
	conn := servicegraphconnector.NewConnector(set, cfg)
	conn.SetMetricsConsumer(newMockMetricsExporter())

	assert.NoError(t, conn.Start(context.Background(), componenttest.NewNopHost()))

	// Test & verify
	td := buildSampleTrace(t, "val")
	// The assertion is part of verifyHappyCaseMetrics func.
	assert.NoError(t, conn.ConsumeTraces(context.Background(), td))

	// Force collection
	conn.SetExpire(time.Now())
	md, err := conn.BuildMetrics()
	assert.NoError(t, err)
	verifyHappyCaseMetrics(t, md)

	// Shutdown the connector
	assert.NoError(t, conn.Shutdown(context.Background()))
}

func verifyHappyCaseMetrics(t *testing.T, md pmetric.Metrics) {
	verifyHappyCaseMetricsWithDuration(1)(t, md)
}

func verifyHappyCaseMetricsWithDuration(durationSum float64) func(t *testing.T, md pmetric.Metrics) {
	return func(t *testing.T, md pmetric.Metrics) {
		assert.Equal(t, 3, md.MetricCount())

		rms := md.ResourceMetrics()
		assert.Equal(t, 1, rms.Len())

		sms := rms.At(0).ScopeMetrics()
		assert.Equal(t, 1, sms.Len())

		ms := sms.At(0).Metrics()
		assert.Equal(t, 3, ms.Len())

		mCount := ms.At(0)
		verifyCount(t, mCount)

		mServerDuration := ms.At(1)
		assert.Equal(t, "traces_service_graph_request_server_seconds", mServerDuration.Name())
		verifyDuration(t, mServerDuration, durationSum)

		mClientDuration := ms.At(2)
		assert.Equal(t, "traces_service_graph_request_client_seconds", mClientDuration.Name())
		verifyDuration(t, mClientDuration, durationSum)
	}
}

func verifyCount(t *testing.T, m pmetric.Metric) {
	assert.Equal(t, "traces_service_graph_request_total", m.Name())

	assert.Equal(t, pmetric.MetricTypeSum, m.Type())
	dps := m.Sum().DataPoints()
	assert.Equal(t, 1, dps.Len())

	dp := dps.At(0)
	assert.Equal(t, pmetric.NumberDataPointValueTypeInt, dp.ValueType())
	assert.Equal(t, int64(1), dp.IntValue())

	attributes := dp.Attributes()
	assert.Equal(t, 5, attributes.Len())
	verifyAttr(t, attributes, "client", "some-service")
	verifyAttr(t, attributes, "server", "some-service")
	verifyAttr(t, attributes, "connection_type", "")
	verifyAttr(t, attributes, "failed", "false")
	verifyAttr(t, attributes, "client_some-attribute", "val")
}

func verifyDuration(t *testing.T, m pmetric.Metric, durationSum float64) {
	assert.Equal(t, pmetric.MetricTypeHistogram, m.Type())
	dps := m.Histogram().DataPoints()
	assert.Equal(t, 1, dps.Len())

	dp := dps.At(0)
	assert.Equal(t, durationSum, dp.Sum()) // Duration: 1sec
	assert.Equal(t, uint64(1), dp.Count())
	buckets := pcommon.NewUInt64Slice()
	buckets.FromRaw([]uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0})
	assert.Equal(t, buckets, dp.BucketCounts())

	attributes := dp.Attributes()
	assert.Equal(t, 5, attributes.Len())
	verifyAttr(t, attributes, "client", "some-service")
	verifyAttr(t, attributes, "server", "some-service")
	verifyAttr(t, attributes, "connection_type", "")
	verifyAttr(t, attributes, "client_some-attribute", "val")
}

func verifyAttr(t *testing.T, attrs pcommon.Map, k, expected string) {
	v, ok := attrs.Get(k)
	assert.True(t, ok)
	assert.Equal(t, expected, v.AsString())
}

func buildSampleTrace(t *testing.T, attrValue string) ptrace.Traces {
	tStart := time.Date(2022, 1, 2, 3, 4, 5, 6, time.UTC)
	tEnd := time.Date(2022, 1, 2, 3, 4, 6, 6, time.UTC)

	traces := ptrace.NewTraces()

	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr(semconv.AttributeServiceName, "some-service")

	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()

	var traceID pcommon.TraceID
	_, err := rand.Read(traceID[:])
	assert.NoError(t, err)

	var clientSpanID, serverSpanID pcommon.SpanID
	_, err = rand.Read(clientSpanID[:])
	assert.NoError(t, err)
	_, err = rand.Read(serverSpanID[:])
	assert.NoError(t, err)

	clientSpan := scopeSpans.Spans().AppendEmpty()
	clientSpan.SetName("client span")
	clientSpan.SetSpanID(clientSpanID)
	clientSpan.SetTraceID(traceID)
	clientSpan.SetKind(ptrace.SpanKindClient)
	clientSpan.SetStartTimestamp(pcommon.NewTimestampFromTime(tStart))
	clientSpan.SetEndTimestamp(pcommon.NewTimestampFromTime(tEnd))
	clientSpan.Attributes().PutStr("some-attribute", attrValue) // Attribute selected as dimension for metrics

	serverSpan := scopeSpans.Spans().AppendEmpty()
	serverSpan.SetName("server span")
	serverSpan.SetSpanID(serverSpanID)
	serverSpan.SetTraceID(traceID)
	serverSpan.SetParentSpanID(clientSpanID)
	serverSpan.SetKind(ptrace.SpanKindServer)
	serverSpan.SetStartTimestamp(pcommon.NewTimestampFromTime(tStart))
	serverSpan.SetEndTimestamp(pcommon.NewTimestampFromTime(tEnd))

	return traces
}

type mockHost struct {
	component.Host
	exps map[component.DataType]map[component.ID]component.Component
}

func newMockHost(exps map[component.DataType]map[component.ID]component.Component) component.Host {
	return &mockHost{
		Host: componenttest.NewNopHost(),
		exps: exps,
	}
}

func (m *mockHost) GetExporters() map[component.DataType]map[component.ID]component.Component {
	return m.exps
}

var _ exporter.Metrics = (*mockMetricsExporter)(nil)

func newMockMetricsExporter() *mockMetricsExporter {
	return &mockMetricsExporter{}
}

type mockMetricsExporter struct {
	mtx sync.Mutex
	md  []pmetric.Metrics
}

func (m *mockMetricsExporter) Start(context.Context, component.Host) error { return nil }

func (m *mockMetricsExporter) Shutdown(context.Context) error { return nil }

func (m *mockMetricsExporter) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }

func (m *mockMetricsExporter) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.md = append(m.md, md)
	return nil
}

func TestUpdateDurationMetrics(t *testing.T) {
	p := servicegraphconnector.BuildTestService()

	metricKey := p.BuildMetricKey("foo", "bar", "", map[string]string{})

	testCases := []struct {
		caseStr  string
		duration float64
	}{

		{
			caseStr:  "index 0 latency",
			duration: 0,
		},
		{
			caseStr:  "out-of-range latency 1",
			duration: 25_000,
		},
		{
			caseStr:  "out-of-range latency 2",
			duration: 125_000,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseStr, func(_ *testing.T) {
			p.UpdateDurationMetrics(metricKey, tc.duration, tc.duration, 1)
		})
	}
}

func TestCorrectForExpiredEdges(t *testing.T) {
	testSize := 1000

	// Prepare
	cfg := &servicegraphconnector.Config{
		MetricsExporter: "mock",
		Dimensions:      []string{"some-attribute", "non-existing-attribute"},
		Store: servicegraphconnector.StoreConfig{
			MaxItems: testSize,
			TTL:      time.Millisecond,
		},
	}

	mockMetricsExporter := newMockMetricsExporter()

	set := componenttest.NewNopTelemetrySettings()
	set.Logger = zaptest.NewLogger(t)
	p := servicegraphconnector.NewConnector(set, cfg)

	mHost := newMockHost(map[component.DataType]map[component.ID]component.Component{
		component.DataTypeMetrics: {
			component.MustNewID("mock"): mockMetricsExporter,
		},
	})

	assert.NoError(t, p.Start(context.Background(), mHost))

	td := ptrace.NewTraces()
	for i := 0; i < testSize; i++ {
		trace := buildSampleTrace(t, "first")
		trace.ResourceSpans().MoveAndAppendTo(td.ResourceSpans())
	}
	td.MarkReadOnly()

	clientTraces := ptrace.NewTraces()
	td.CopyTo(clientTraces)
	for rsi := 0; rsi < clientTraces.ResourceSpans().Len(); rsi++ {
		rs := clientTraces.ResourceSpans().At(rsi)
		for ssi := 0; ssi < rs.ScopeSpans().Len(); ssi++ {
			ss := rs.ScopeSpans().At(ssi)
			ss.Spans().RemoveIf(func(span ptrace.Span) bool {
				return span.Kind() != ptrace.SpanKindClient
			})
		}
	}
	assert.NoError(t, p.ConsumeTraces(context.Background(), clientTraces))

	p.SetExpire(time.Now().Add(2 * time.Millisecond))

	serverTraces := ptrace.NewTraces()
	td.CopyTo(serverTraces)
	for rsi := 0; rsi < serverTraces.ResourceSpans().Len(); rsi++ {
		rs := serverTraces.ResourceSpans().At(rsi)
		for ssi := 0; ssi < rs.ScopeSpans().Len(); ssi++ {
			ss := rs.ScopeSpans().At(ssi)
			ss.Spans().RemoveIf(func(span ptrace.Span) bool {
				return span.Kind() != ptrace.SpanKindServer
			})
		}
	}
	assert.NoError(t, p.ConsumeTraces(context.Background(), serverTraces))

	total := mockMetricsExporter.md[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	assert.Equal(t, "traces_service_graph_request_total", total.Name())
	n := total.Sum().DataPoints().At(0).IntValue()

	// variance is determined by size of store - after expiration only 37% of the edges remained
	// the rescheduled edges try to compensate
	stderr := int(math.Sqrt(float64(testSize) / 0.37))

	//nolint:forbidigo
	println(fmt.Sprintf("Testing %d close to %d (within %d)", n, testSize, 4*stderr))
	assert.Less(t, testSize-4*stderr, int(n))
	assert.Greater(t, testSize+4*stderr, int(n))

	// Shutdown the connector
	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestStaleSeriesCleanup(t *testing.T) {
	// Prepare
	cfg := &servicegraphconnector.Config{
		MetricsExporter: "mock",
		Dimensions:      []string{"some-attribute", "non-existing-attribute"},
		Store: servicegraphconnector.StoreConfig{
			MaxItems: 10,
			TTL:      time.Second,
		},
	}

	mockMetricsExporter := newMockMetricsExporter()

	set := componenttest.NewNopTelemetrySettings()
	set.Logger = zaptest.NewLogger(t)
	p := servicegraphconnector.NewConnector(set, cfg)

	mHost := newMockHost(map[component.DataType]map[component.ID]component.Component{
		component.DataTypeMetrics: {
			component.MustNewID("mock"): mockMetricsExporter,
		},
	})

	assert.NoError(t, p.Start(context.Background(), mHost))

	// ConsumeTraces
	td := buildSampleTrace(t, "first")
	assert.NoError(t, p.ConsumeTraces(context.Background(), td))

	// Make series stale and force a cache cleanup
	for key, metric := range p.GetKeyToMetric() {
		metric.SetLastUpdated(0)
		p.SetKeyToMetric(key, metric)
	}
	p.CleanCache()
	assert.Equal(t, 0, len(p.GetKeyToMetric()))

	// ConsumeTraces with a trace with different attribute value
	td = buildSampleTrace(t, "second")
	assert.NoError(t, p.ConsumeTraces(context.Background(), td))

	// Shutdown the connector
	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestMapsAreConsistentDuringCleanup(t *testing.T) {
	// Prepare
	cfg := &servicegraphconnector.Config{
		MetricsExporter: "mock",
		Dimensions:      []string{"some-attribute", "non-existing-attribute"},
		Store: servicegraphconnector.StoreConfig{
			MaxItems: 10,
			TTL:      time.Second,
		},
	}

	mockMetricsExporter := newMockMetricsExporter()

	set := componenttest.NewNopTelemetrySettings()
	set.Logger = zaptest.NewLogger(t)
	p := servicegraphconnector.NewConnector(set, cfg)

	mHost := newMockHost(map[component.DataType]map[component.ID]component.Component{
		component.DataTypeMetrics: {
			component.MustNewID("mock"): mockMetricsExporter,
		},
	})

	assert.NoError(t, p.Start(context.Background(), mHost))

	// ConsumeTraces
	td := buildSampleTrace(t, "first")
	assert.NoError(t, p.ConsumeTraces(context.Background(), td))

	// Make series stale and force a cache cleanup
	for key, metric := range p.GetKeyToMetric() {
		metric.SetLastUpdated(0)
		p.SetKeyToMetric(key, metric)
	}

	// Start cleanup, but use locks to pretend that we are:
	// - currently collecting metrics (so seriesMutex is locked)
	// - currently getting dimensions for that series (so metricMutex is locked)
	p.SeriesMutex.Lock()
	p.MetricMutex.RLock()
	go p.CleanCache()

	// Since everything is locked, nothing has happened, so both should still have length 1
	assert.Equal(t, 1, len(p.GetReqTotal()))
	assert.Equal(t, 1, len(p.GetKeyToMetric()))

	// Now we pretend that we have stopped collecting metrics, by unlocking seriesMutex
	p.SeriesMutex.Unlock()

	// Make sure cleanupCache has continued to the next mutex
	time.Sleep(time.Millisecond)
	p.SeriesMutex.Lock()

	// The expired series should have been removed. The metrics collector now won't look
	// for dimensions from that series. It's important that it happens this way around,
	// instead of deleting it from `keyToMetric`, otherwise the metrics collector will try
	// and fail to find dimensions for a series that is about to be removed.
	assert.Equal(t, 0, len(p.GetReqTotal()))
	assert.Equal(t, 1, len(p.GetKeyToMetric()))

	p.MetricMutex.RUnlock()
	p.SeriesMutex.Unlock()

	// Shutdown the connector
	assert.NoError(t, p.Shutdown(context.Background()))
}

func setupTelemetry(reader *sdkmetric.ManualReader) component.TelemetrySettings {
	settings := componenttest.NewNopTelemetrySettings()
	settings.MetricsLevel = configtelemetry.LevelNormal

	settings.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return settings
}

func TestValidateOwnTelemetry(t *testing.T) {
	cfg := &servicegraphconnector.Config{
		MetricsExporter: "mock",
		Dimensions:      []string{"some-attribute", "non-existing-attribute"},
		Store: servicegraphconnector.StoreConfig{
			MaxItems: 10,
			TTL:      time.Second,
		},
	}

	mockMetricsExporter := newMockMetricsExporter()

	reader := sdkmetric.NewManualReader()
	set := setupTelemetry(reader)
	p := servicegraphconnector.NewConnector(set, cfg)

	mHost := newMockHost(map[component.DataType]map[component.ID]component.Component{
		component.DataTypeMetrics: {
			component.MustNewID("mock"): mockMetricsExporter,
		},
	})

	assert.NoError(t, p.Start(context.Background(), mHost))

	// ConsumeTraces
	td := buildSampleTrace(t, "first")
	assert.NoError(t, p.ConsumeTraces(context.Background(), td))

	// Make series stale and force a cache cleanup
	for key, metric := range p.GetKeyToMetric() {
		metric.SetLastUpdated(0)
		p.SetKeyToMetric(key, metric)
	}
	p.CleanCache()
	assert.Equal(t, 0, len(p.GetKeyToMetric()))

	// ConsumeTraces with a trace with different attribute value
	td = buildSampleTrace(t, "second")
	assert.NoError(t, p.ConsumeTraces(context.Background(), td))

	// Shutdown the connector
	assert.NoError(t, p.Shutdown(context.Background()))

	rm := metricdata.ResourceMetrics{}
	assert.NoError(t, reader.Collect(context.Background(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	sm := rm.ScopeMetrics[0]
	require.Len(t, sm.Metrics, 1)
	got := sm.Metrics[0]
	want := metricdata.Metrics{
		Name:        "connector/stsservicegraph/total_edges",
		Description: "Total number of unique edges",
		Unit:        "1",
		Data: metricdata.Sum[int64]{
			Temporality: metricdata.CumulativeTemporality,
			IsMonotonic: true,
			DataPoints: []metricdata.DataPoint[int64]{
				{Value: 2},
			},
		},
	}
	metricdatatest.AssertEqual(t, want, got, metricdatatest.IgnoreTimestamp())
}

func TestFindDatabaseUsesPeer(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("db.system", "postgres")
	attrs.PutStr("db.name", "the-db")
	attrs.PutStr("peer.service", "the-peer")
	db, found := servicegraphconnector.FindDatabase(attrs)
	assert.True(t, found)
	assert.Equal(t, "the-peer", *db)
}

func TestFindDatabaseUsesDbnameWithoutPeer(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("db.system", "postgres")
	attrs.PutStr("db.name", "the-db")
	db, found := servicegraphconnector.FindDatabase(attrs)
	assert.True(t, found)
	assert.Equal(t, "the-db", *db)
}

func TestFindDatabaseUsesIndexForRedis(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("db.system", "redis")
	db, found := servicegraphconnector.FindDatabase(attrs)
	assert.True(t, found)
	assert.Equal(t, "redis", *db)
}
