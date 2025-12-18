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
	d := newDedup(t, 0.5)
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

	ref := types.NewExpressionRefSummary(
		types.EntityRefSummary{AttributeKeys: []string{"x", "y"}},
		types.EntityRefSummary{},
		types.EntityRefSummary{},
	)

	h1 := d.hasher.ProjectionHash("m1", settings.METRICS, ctx1, ref)
	h2 := d.hasher.ProjectionHash("m1", settings.METRICS, ctx2, ref)
	require.Equal(t, h1, h2, "Hash should be stable regardless of map iteration order")
}

func TestProjectionHash_SliceOrderMatters(t *testing.T) {
	d := newDedup(t, 0.5)

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
	d := newDedup(t, 0.5)

	ctx := &ExpressionEvalContext{
		Resource: NewResource(map[string]any{"a": 1}),
	}

	h1 := d.hasher.ProjectionHash("mapping-A", settings.TRACES, ctx, nil)
	h2 := d.hasher.ProjectionHash("mapping-B", settings.TRACES, ctx, nil)

	require.NotEqual(t, h1, h2)
}

// {"a": nil} hashes differently from {}
func TestProjectionHash_NilVsMissing(t *testing.T) {
	d := newDedup(t, 0.5)

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
	d := newDedup(t, 0.25)
	ctx := &ExpressionEvalContext{
		Resource: NewResource(map[string]any{"service.name": "cart"}),
		Metric:   NewMetric("m1", "desc", "unit"),
		Datapoint: NewDatapoint(map[string]any{
			"kind": "db",
		}),
	}
	ref := types.NewExpressionRefSummary(
		types.EntityRefSummary{AttributeKeys: []string{"kind"}},
		types.EntityRefSummary{},
		types.EntityRefSummary{AttributeKeys: []string{"unit"}},
	)

	send1 := d.ShouldSend("m1", settings.METRICS, ctx, ref, time.Minute)
	require.True(t, send1)

	// Second call with same inputs -> should not Send (refresh window not reached)
	send2 := d.ShouldSend("m1", settings.METRICS, ctx, ref, time.Minute)
	require.False(t, send2)

	// Change a referenced input -> Key should change and Send
	ctx.Datapoint = NewDatapoint(map[string]any{"kind": "messaging"})
	send3 := d.ShouldSend("m1", settings.METRICS, ctx, ref, time.Minute)
	require.True(t, send3)
}

func TestShouldSend_UnreferencedInputDoesNotChangeKey(t *testing.T) {
	d := newDedup(t, 0.5)

	ctx := &ExpressionEvalContext{
		Resource: NewResource(map[string]any{"service.name": "cart"}),
		Metric:   NewMetric("ignored", "ignored", "ignored"),
	}
	ref := types.NewExpressionRefSummary(
		types.EntityRefSummary{},
		types.EntityRefSummary{},
		types.EntityRefSummary{},
	)

	send1 := d.ShouldSend("m1", settings.METRICS, ctx, ref, time.Minute)
	require.True(t, send1)

	send2 := d.ShouldSend("m1", settings.METRICS, ctx, ref, time.Minute)
	require.False(t, send2)

	// Change unreferenced var
	ctx.Metric.cachedMap["name"] = "changed"

	send3 := d.ShouldSend("m1", settings.METRICS, ctx, ref, time.Minute)
	require.False(t, send3, "unreferenced input must not trigger send")
}

func TestShouldSend_RefreshAfterThreshold(t *testing.T) {
	// Set refreshFraction small to trigger quickly
	ttl := 200 * time.Millisecond
	d := newDedup(t, 0.5)
	ctx := &ExpressionEvalContext{Resource: NewResource(map[string]any{"a": 1})}
	ref := &types.ExpressionRefSummary{}

	send1 := d.ShouldSend("m1", settings.TRACES, ctx, ref, ttl)
	require.True(t, send1)

	// Immediately should not Send again
	send2 := d.ShouldSend("m1", settings.TRACES, ctx, ref, ttl)
	require.False(t, send2)

	// Wait past refresh threshold (cacheTTL*refreshFraction)
	time.Sleep(ttl/2 + 20*time.Millisecond)

	send3 := d.ShouldSend("m1", settings.TRACES, ctx, ref, ttl)
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

func newDedup(t *testing.T, refresh float64) *TopologyDeduplicator {
	t.Helper()
	return NewTopologyDeduplicator(
		context.Background(),
		zaptest.NewLogger(t),
		DeduplicationConfig{
			Enabled:         true,
			RefreshFraction: refresh,
			CacheConfig:     testCacheSettings(),
		})
}
