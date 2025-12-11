package topologyconnector_test

import (
	"context"
	"testing"

	topologyConnector "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap/zaptest"
)

func TestExpressionRefManager_UpdateAndCurrent_ComponentAndRelation(t *testing.T) {
	eval := newTestCELEvaluator(t)
	logger := zaptest.NewLogger(t)
	refManager := topologyConnector.NewExpressionRefManager(logger, eval)

	// Build a component mapping with vars and outputs referencing various inputs
	comp := stsSettingsModel.OtelComponentMapping{
		Identifier: "comp-1",
		Input: stsSettingsModel.OtelInput{
			Signal: stsSettingsModel.OtelInputSignalList{stsSettingsModel.METRICS},
		},
		Output: stsSettingsModel.OtelComponentMappingOutput{
			Identifier: sExpr("id-${resource.attributes['service.name']}-${vars.ns}"),
			Name:       sExpr("name-${vars.ns}"),
			TypeName:   sExpr("type"),
			LayerName:  sExpr("layer-${datapoint.attributes['kind']}"),
		},
		Vars: &[]stsSettingsModel.OtelVariableMapping{
			{Name: "ns", Value: aExpr("${scope.name}")},
		},
	}

	// Relation mapping referencing span attributes
	rel := stsSettingsModel.OtelRelationMapping{
		Identifier: "rel-1",
		Input: stsSettingsModel.OtelInput{
			Signal: stsSettingsModel.OtelInputSignalList{stsSettingsModel.TRACES},
		},
		Output: stsSettingsModel.OtelRelationMappingOutput{
			SourceId: sExpr("${span.attributes['src']}"),
			TargetId: sExpr("${span.attributes['dst']}"),
			TypeName: sExpr("rel"),
		},
	}

	signals := []stsSettingsModel.OtelInputSignal{stsSettingsModel.METRICS, stsSettingsModel.TRACES}
	compBySig := map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelComponentMapping{
		stsSettingsModel.METRICS: {comp},
		stsSettingsModel.TRACES:  {},
	}
	relBySig := map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelRelationMapping{
		stsSettingsModel.METRICS: {},
		stsSettingsModel.TRACES:  {rel},
	}

	// Exercise Update and then verify Current for each signal
	refManager.Update(signals, compBySig, relBySig)

	// Metrics signal -> component expressionRefSummaries
	m := refManager.Current(stsSettingsModel.METRICS)
	require.NotNil(t, m)
	compRefs, ok := m["comp-1"]
	require.True(t, ok)
	require.ElementsMatch(t, []string{"ns"}, compRefs.Vars())
	require.ElementsMatch(t, []string{"kind"}, compRefs.DatapointKeys())
	// resource and scope are implicitly included in projection; we only assert tracked keys here

	// Traces signal -> relation expressionRefSummaries
	tr := refManager.Current(stsSettingsModel.TRACES)
	require.NotNil(t, tr)
	relRefs, ok := tr["rel-1"]
	require.True(t, ok)
	require.ElementsMatch(t, []string{"src", "dst"}, relRefs.SpanKeys())
}

func TestExpressionRefManager_Current_NilForUnknownSignal(t *testing.T) {
	eval := newTestCELEvaluator(t)
	logger := zaptest.NewLogger(t)
	refManager := topologyConnector.NewExpressionRefManager(logger, eval)

	// No update yet
	cur := refManager.Current(stsSettingsModel.TRACES)
	require.Nil(t, cur)
}

func TestExpressionRefManager_InvalidExpressionsAreIgnored(t *testing.T) {
	eval := newTestCELEvaluator(t)
	logger := zaptest.NewLogger(t)
	refManager := topologyConnector.NewExpressionRefManager(logger, eval)

	comp := stsSettingsModel.OtelComponentMapping{
		Identifier: "bad-comp",
		Input: stsSettingsModel.OtelInput{
			Signal: stsSettingsModel.OtelInputSignalList{stsSettingsModel.METRICS},
		},
		Output: stsSettingsModel.OtelComponentMappingOutput{
			Identifier: sExpr("${this is not valid CEL"),
			Name:       sExpr("${also bad"),
		},
	}

	refManager.Update(
		[]stsSettingsModel.OtelInputSignal{stsSettingsModel.METRICS},
		map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelComponentMapping{
			stsSettingsModel.METRICS: {comp},
		},
		nil,
	)

	cur := refManager.Current(stsSettingsModel.METRICS)
	require.Nil(t, cur, "invalid expressions should produce no summaries")
}

func TestExpressionRefManager_OptionalFieldsAreLenient(t *testing.T) {
	eval := newTestCELEvaluator(t)
	logger := zaptest.NewLogger(t)
	refManager := topologyConnector.NewExpressionRefManager(logger, eval)

	comp := stsSettingsModel.OtelComponentMapping{
		Identifier: "comp",
		Input: stsSettingsModel.OtelInput{
			Signal: stsSettingsModel.OtelInputSignalList{stsSettingsModel.METRICS},
		},
		Output: stsSettingsModel.OtelComponentMappingOutput{
			Identifier: sExpr("${resource.attributes['ok']}"),
			Optional: &stsSettingsModel.OtelComponentMappingFieldMapping{
				AdditionalIdentifiers: &[]stsSettingsModel.OtelStringExpression{sExpr("${invalid")},
			},
		},
		Vars: &[]stsSettingsModel.OtelVariableMapping{
			{Name: "ns", Value: aExpr("${span.attributes['ns']}")},
		},
	}

	refManager.Update(
		[]stsSettingsModel.OtelInputSignal{stsSettingsModel.METRICS},
		map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelComponentMapping{
			stsSettingsModel.METRICS: {comp},
		},
		nil,
	)

	cur := refManager.Current(stsSettingsModel.METRICS)
	require.NotNil(t, cur)
	compRefs, ok := cur["comp"]
	require.True(t, ok)
	require.ElementsMatch(t, []string{"ns"}, compRefs.SpanKeys())
}

func TestExpressionRefManager_UpdateReplacesState(t *testing.T) {
	eval := newTestCELEvaluator(t)
	logger := zaptest.NewLogger(t)
	refManager := topologyConnector.NewExpressionRefManager(logger, eval)

	refManager.Update(
		[]stsSettingsModel.OtelInputSignal{stsSettingsModel.METRICS},
		nil,
		nil,
	)

	refManager.Update(
		[]stsSettingsModel.OtelInputSignal{},
		nil,
		nil,
	)

	require.Nil(t, refManager.Current(stsSettingsModel.METRICS))
}

func newTestCELEvaluator(t *testing.T) internal.ExpressionEvaluator {
	t.Helper()
	eval, err := internal.NewCELEvaluator(
		context.Background(),
		metrics.MeteredCacheSettings{
			Name:              "expression_cache_test",
			EnableMetrics:     false,
			TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		},
	)
	require.NoError(t, err)
	return eval
}

func sExpr(s string) stsSettingsModel.OtelStringExpression {
	return stsSettingsModel.OtelStringExpression{Expression: s}
}

func aExpr(s string) stsSettingsModel.OtelAnyExpression {
	return stsSettingsModel.OtelAnyExpression{Expression: s}
}
