package metrics_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/metrics"
	"go.opentelemetry.io/collector/component/componenttest"
)

type mockMeter struct {
	hits, misses, adds, evicts atomic.Int64
}

func (m *mockMeter) IncHit()        { m.hits.Add(1) }
func (m *mockMeter) IncMiss()       { m.misses.Add(1) }
func (m *mockMeter) IncAdd()        { m.adds.Add(1) }
func (m *mockMeter) IncEvict()      { m.evicts.Add(1) }
func (m *mockMeter) RecordSize(int) {}
func (m *mockMeter) Close()         {}

func TestMeteredCache_BasicMetrics(t *testing.T) {
	ctx := context.Background()
	meter := &mockMeter{}

	mc := metrics.NewCacheWithMeter[string, string](
		ctx,
		metrics.MeteredCacheSettings{
			Name:              "test",
			Size:              5,
			TTL:               0,
			EnableMetrics:     false, // using a custom meter makes this flag irrelevant
			TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		},
		nil,
		meter,
	)
	defer mc.Close()

	mc.Add("a", "1") // add
	mc.Get("a")      // hit
	mc.Get("b")      // miss

	if got := meter.adds.Load(); got != 1 {
		t.Errorf("expected 1 add, got %d", got)
	}
	if got := meter.hits.Load(); got != 1 {
		t.Errorf("expected 1 hit, got %d", got)
	}
	if got := meter.misses.Load(); got != 1 {
		t.Errorf("expected 1 miss, got %d", got)
	}
}
