//nolint:testpackage
package internal

import (
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/stretchr/testify/assert"
)

// newCtxForTimestamp builds a MappingContext whose BaseContext carries the given fallback (collector receive
// time) so we can verify effectiveCollectionTimestamp's preference rules.
func newCtxForTimestamp(fallback int64) *MappingContext[settingsproto.OtelComponentMapping] {
	return &MappingContext[settingsproto.OtelComponentMapping]{
		BaseCtx: BaseContext{
			CollectionTimestamp: fallback,
			MetricsRecorder:     &metrics.NoopConnectorMetricsRecorder{},
		},
	}
}

func TestEffectiveCollectionTimestamp_PrefersElementTimestamp(t *testing.T) {
	ctx := newCtxForTimestamp(999)
	evalCtx := NewLogEvalContext(NewLog("e", "b", nil).WithCollectionTimestamp(1234), nil, nil)

	assert.Equal(t, int64(1234), ctx.effectiveCollectionTimestamp(evalCtx))
}

func TestEffectiveCollectionTimestamp_FallsBackWhenElementTimestampZero(t *testing.T) {
	ctx := newCtxForTimestamp(999)
	// Resource/scope-level contexts have no leaf element, hence a zero collection timestamp.
	evalCtx := NewLogEvalContext(nil, nil, nil)

	assert.Equal(t, int64(999), ctx.effectiveCollectionTimestamp(evalCtx))
}

func TestEvalContext_CollectionTimestampSetFromLeafModels(t *testing.T) {
	logCtx := NewLogEvalContext(NewLog("e", "b", nil).WithCollectionTimestamp(11), nil, nil)
	spanCtx := NewSpanEvalContext(NewSpan("n", "k", "ok", "", nil).WithCollectionTimestamp(22), nil, nil)
	dpCtx := NewMetricEvalContext(NewDatapoint(nil).WithCollectionTimestamp(33), nil, nil, nil)

	assert.Equal(t, int64(11), logCtx.CollectionTimestampMs)
	assert.Equal(t, int64(22), spanCtx.CollectionTimestampMs)
	assert.Equal(t, int64(33), dpCtx.CollectionTimestampMs)
}

func TestEvalContext_CloneWithVariablesPreservesCollectionTimestamp(t *testing.T) {
	evalCtx := NewLogEvalContext(NewLog("e", "b", nil).WithCollectionTimestamp(77), nil, nil)
	cloned := evalCtx.CloneWithVariables(map[string]any{"a": 1})
	assert.Equal(t, int64(77), cloned.CollectionTimestampMs)
}

func TestTimestampToMillis(t *testing.T) {
	assert.Equal(t, int64(0), timestampToMillis(0))
	// 1_500_000_000 ns = 1500 ms
	assert.Equal(t, int64(1500), timestampToMillis(1_500_000_000))
}
