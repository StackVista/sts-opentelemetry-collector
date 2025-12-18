//nolint:testpackage
package internal

import (
	"context"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/types"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap/zaptest"
)

func TestProjectionHash_DeterministicOverAttributeOrder(t *testing.T) {
	ref := types.NewExpressionRefSummary(
		types.EntityRefSummary{AttributeKeys: []string{"x", "y"}},
		types.EntityRefSummary{},
		types.EntityRefSummary{},
	)
	refSummaries := newExpressionRefSummaryForSignal(settings.METRICS, "m1", *ref)
	d := newDedup(t, 0.5, refSummaries)
	ctx1 := &ExpressionEvalContext{
		Resource: NewResource(
			map[string]any{
				"a": 1,
				"b": 2,
				"labels": map[string]any{
					"env":  "prod",
					"tier": []any{"frontend", "api"},
				},
			},
		),
		Datapoint: NewDatapoint(map[string]any{
			"x": 1, "y": 2,
		}),
	}
	ctx2 := &ExpressionEvalContext{
		Resource: NewResource(
			map[string]any{
				"b": 2, // swapped order of a and b
				"a": 1,
				"labels": map[string]any{
					"tier": []any{"frontend", "api"},
					"env":  "prod", // swapped map order
				},
			},
		),
		Datapoint: NewDatapoint(map[string]any{
			"y": 2, "x": 1, // swapped order
		}),
	}

	h1 := d.hasher.ProjectionHash("m1", settings.METRICS, ctx1, ref)
	h2 := d.hasher.ProjectionHash("m1", settings.METRICS, ctx2, ref)
	require.Equal(t, h1, h2, "Hash should be stable regardless of map iteration order")
}

func TestProjectionHash_SliceOrderMatters(t *testing.T) {
	refSummaries := newExpressionRefSummaryForSignal(
		settings.METRICS, "m1", types.ExpressionRefSummary{},
	)
	d := newDedup(t, 0.5, refSummaries)

	ctx1 := &ExpressionEvalContext{
		Resource: NewResource(map[string]any{
			"roles": []any{"db", "cache"},
		}),
	}
	ctx2 := &ExpressionEvalContext{
		Resource: NewResource(map[string]any{
			"roles": []any{"cache", "db"}, // reversed
		}),
	}

	h1 := d.hasher.ProjectionHash("m1", settings.METRICS, ctx1, nil)
	h2 := d.hasher.ProjectionHash("m1", settings.METRICS, ctx2, nil)

	require.NotEqual(t, h1, h2, "slice order must affect hash")
}

func TestProjectionHash_MappingIdentifierIsolation(t *testing.T) {
	refSummary1 := newExpressionRefSummaryForSignal(
		settings.TRACES, "mapping-A", types.ExpressionRefSummary{})
	refSummary2 := newExpressionRefSummaryForSignal(
		settings.TRACES, "mapping-B", types.ExpressionRefSummary{})
	d := newDedup(t, 0.5, mergeExpressionRefSummaries(refSummary1, refSummary2))

	ctx := &ExpressionEvalContext{
		Resource: NewResource(map[string]any{"a": 1}),
	}

	h1 := d.hasher.ProjectionHash("mapping-A", settings.TRACES, ctx, nil)
	h2 := d.hasher.ProjectionHash("mapping-B", settings.TRACES, ctx, nil)

	require.NotEqual(t, h1, h2)
}

// {"a": nil} hashes differently from {}
func TestProjectionHash_NilVsMissing(t *testing.T) {
	refSummaries := newExpressionRefSummaryForSignal(settings.METRICS, "m1", types.ExpressionRefSummary{})
	d := newDedup(t, 0.5, refSummaries)

	ctx1 := &ExpressionEvalContext{
		Resource: NewResource(map[string]any{"a": nil}),
	}
	ctx2 := &ExpressionEvalContext{
		Resource: NewResource(map[string]any{}),
	}

	h1 := d.hasher.ProjectionHash("m1", settings.METRICS, ctx1, nil)
	h2 := d.hasher.ProjectionHash("m1", settings.METRICS, ctx2, nil)

	require.NotEqual(t, h1, h2)
}

func TestShouldSend_KeyChangesWhenReferencedInputChanges(t *testing.T) {
	ref := types.NewExpressionRefSummary(
		types.EntityRefSummary{AttributeKeys: []string{"kind"}},
		types.EntityRefSummary{},
		types.EntityRefSummary{AttributeKeys: []string{"unit"}},
	)
	refSummaries := newExpressionRefSummaryForSignal(settings.METRICS, "m1", *ref)
	d := newDedup(t, 0.25, refSummaries)
	ctx := &ExpressionEvalContext{
		Resource: NewResource(map[string]any{"service.name": "cart"}),
		Metric:   NewMetric("m1", "desc", "unit"),
		Datapoint: NewDatapoint(map[string]any{
			"kind": "db",
		}),
	}

	send1 := d.ShouldSend("m1", settings.METRICS, ctx, time.Minute)
	require.True(t, send1)

	// Second call with same inputs -> should not Send (refresh window not reached)
	send2 := d.ShouldSend("m1", settings.METRICS, ctx, time.Minute)
	require.False(t, send2)

	// Change a referenced input -> Key should change and Send
	ctx.Datapoint = NewDatapoint(map[string]any{"kind": "messaging"})
	send3 := d.ShouldSend("m1", settings.METRICS, ctx, time.Minute)
	require.True(t, send3)
}

func TestShouldSend_UnreferencedInputDoesNotChangeKey(t *testing.T) {
	ref := types.NewExpressionRefSummary(
		types.EntityRefSummary{},
		types.EntityRefSummary{},
		types.EntityRefSummary{},
	)
	refSummaries := newExpressionRefSummaryForSignal(settings.METRICS, "m1", *ref)
	d := newDedup(t, 0.5, refSummaries)

	ctx := &ExpressionEvalContext{
		Resource: NewResource(map[string]any{"service.name": "cart"}),
		Metric:   NewMetric("ignored", "ignored", "ignored"),
	}

	send1 := d.ShouldSend("m1", settings.METRICS, ctx, time.Minute)
	require.True(t, send1)

	send2 := d.ShouldSend("m1", settings.METRICS, ctx, time.Minute)
	require.False(t, send2)

	// Change unreferenced var
	ctx.Metric.cachedMap["name"] = "changed"

	send3 := d.ShouldSend("m1", settings.METRICS, ctx, time.Minute)
	require.False(t, send3, "unreferenced input must not trigger send")
}

func TestShouldSend_RefreshAfterThreshold(t *testing.T) {
	// Set refreshFraction small to trigger quickly
	ttl := 200 * time.Millisecond
	refSummaries := newExpressionRefSummaryForSignal(settings.TRACES, "m1", types.ExpressionRefSummary{})
	d := newDedup(t, 0.5, refSummaries)
	ctx := &ExpressionEvalContext{Resource: NewResource(map[string]any{"a": 1})}

	send1 := d.ShouldSend("m1", settings.TRACES, ctx, ttl)
	require.True(t, send1)

	// Immediately should not Send again
	send2 := d.ShouldSend("m1", settings.TRACES, ctx, ttl)
	require.False(t, send2)

	// Wait past refresh threshold (cacheTTL*refreshFraction)
	time.Sleep(ttl/2 + 20*time.Millisecond)

	send3 := d.ShouldSend("m1", settings.TRACES, ctx, ttl)
	require.True(t, send3)
}

func testCacheSettings() metrics.MeteredCacheSettings {
	return metrics.MeteredCacheSettings{
		Size:              100,
		EnableMetrics:     false,
		TTL:               time.Minute,
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}
}

func newExpressionRefSummaryForSignal(
	signal settings.OtelInputSignal, mappingIdentifier string, summary types.ExpressionRefSummary,
) map[settings.OtelInputSignal]map[string]*types.ExpressionRefSummary {
	return map[settings.OtelInputSignal]map[string]*types.ExpressionRefSummary{
		signal: {
			mappingIdentifier: &summary,
		},
	}
}

func mergeExpressionRefSummaries(
	summaries map[settings.OtelInputSignal]map[string]*types.ExpressionRefSummary,
	rest ...map[settings.OtelInputSignal]map[string]*types.ExpressionRefSummary,
) map[settings.OtelInputSignal]map[string]*types.ExpressionRefSummary {
	result := make(map[settings.OtelInputSignal]map[string]*types.ExpressionRefSummary)
	for k, v := range summaries {
		result[k] = v
	}
	for _, summary := range rest {
		for k, v := range summary {
			result[k] = v
		}
	}
	return result
}

func newDedup(
	t *testing.T,
	refresh float64,
	expressionRefSummaries map[settings.OtelInputSignal]map[string]*types.ExpressionRefSummary,
) *TopologyDeduplicator {
	t.Helper()
	return NewTopologyDeduplicator(
		context.Background(),
		zaptest.NewLogger(t),
		DeduplicationConfig{
			Enabled:         true,
			RefreshFraction: refresh,
			CacheConfig:     testCacheSettings(),
		},
		&MockExpressionRefReader{expressionRefSummaries},
	)
}

type MockExpressionRefReader struct {
	// signal -> mappingIdentifier -> summary
	expressionRefSummaries map[settings.OtelInputSignal]map[string]*types.ExpressionRefSummary
}

func (m *MockExpressionRefReader) Current(
	signal settings.OtelInputSignal, mappingIdentifier string,
) *types.ExpressionRefSummary {
	return m.expressionRefSummaries[signal][mappingIdentifier]
}
