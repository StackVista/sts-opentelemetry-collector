package topologyconnector_test

import (
	"context"
	"reflect"
	sort "sort"
	"testing"

	topologyConnector "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap/zaptest"
)

/*
Expected*Fields document which model fields are intentionally walked (and skipped) by the
expression reference collectors (collectRefsForComponent / collectRefsForRelation).

These lists are enforced by reflection-based tests to ensure that when new fields
are added to the mapping models, they are either:
  - explicitly walked, or
  - consciously documented as intentionally skipped.

This acts as a safety net against silently missing new expression-bearing fields
during schema evolution.
*/

//nolint:gochecknoglobals
var ExpectedComponentMappingFields = struct {
	TopLevelWalked  []string
	TopLevelSkipped []string
	Output          []string
	FieldMapping    []string
}{
	// Top-level fields of OtelComponentMapping that are walked
	TopLevelWalked: []string{
		"Vars",
		"Output",
	},

	// Fields deliberately NOT walked by collectRefsForComponent
	TopLevelSkipped: []string{
		"CreatedTimeStamp",
		"ExpireAfterMs",
		"Id",
		"Identifier",
		"Input",
		"Name",
		"Shard",
		"Type",
	},

	// Fields of OtelComponentMappingOutput that are walked
	Output: []string{
		"Identifier",
		"Name",
		"TypeName",
		"TypeIdentifier",
		"LayerName",
		"LayerIdentifier",
		"DomainName",
		"DomainIdentifier",
		"Optional",
		"Required",
	},

	// Fields of OtelComponentMappingFieldMapping (Optional / Required)
	FieldMapping: []string{
		"AdditionalIdentifiers",
		"Tags",
		"Version",
	},
}

//nolint:gochecknoglobals
var ExpectedRelationMappingFields = struct {
	TopLevelWalked  []string
	TopLevelSkipped []string
	Output          []string
}{
	// Top-level fields of OtelRelationMapping that are walked
	TopLevelWalked: []string{
		"Vars",
		"Output",
	},

	// Fields deliberately NOT walked by collectRefsForRelation
	TopLevelSkipped: []string{
		"CreatedTimeStamp",
		"ExpireAfterMs",
		"Id",
		"Identifier",
		"Input",
		"Name",
		"Shard",
		"Type",
	},

	// Fields of OtelRelationMappingOutput that are walked
	Output: []string{
		"SourceId",
		"TargetId",
		"TypeName",
		"TypeIdentifier",
	},
}

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

			Required: &stsSettingsModel.OtelComponentMappingFieldMapping{
				Tags: &[]stsSettingsModel.OtelTagMapping{
					{
						Source:  aExpr("${span.attributes}"),
						Pattern: ptr("service.\\(.*)"),
						Target:  "service.${1}",
					},
				},
			},
		},
		Vars: &[]stsSettingsModel.OtelVariableMapping{
			{Name: "ns", Value: aExpr("${span.name}")},
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
	compRefs := refManager.Current(stsSettingsModel.METRICS, "comp-1")
	require.NotNil(t, compRefs)
	require.ElementsMatch(t, []string{"kind"}, compRefs.Datapoint.AttributeKeys)
	require.True(t, compRefs.Span.AllAttributes)
	require.ElementsMatch(t, []string{"name"}, compRefs.Span.FieldKeys)
	// resource and scope are implicitly included in projection; we only assert tracked keys here

	// Traces signal -> relation expressionRefSummaries
	relRefs := refManager.Current(stsSettingsModel.TRACES, "rel-1")
	require.NotNil(t, relRefs)
	require.ElementsMatch(t, []string{"src", "dst"}, relRefs.Span.AttributeKeys)
}

// Verify that a mapping without datapoint, span or metric expressions still returns an "empty" ExpressionRefSummary
func TestExpressionRefManager_UpdateAndCurrent_ComponentWithResourceOnlyExpressions(t *testing.T) {
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
			Identifier: sExpr("id-${resource.attributes['service.name']}"),
			Name:       sExpr("name"),
			TypeName:   sExpr("type"),
			LayerName:  sExpr("layer"),
		},
	}

	signals := []stsSettingsModel.OtelInputSignal{stsSettingsModel.METRICS}
	compBySig := map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelComponentMapping{
		stsSettingsModel.METRICS: {comp},
	}
	relBySig := make(map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelRelationMapping)

	refManager.Update(signals, compBySig, relBySig)

	compRefs := refManager.Current(stsSettingsModel.METRICS, "comp-1")
	require.NotNil(t, compRefs)
	require.Empty(t, compRefs.Datapoint)
	require.Empty(t, compRefs.Span)
	require.Empty(t, compRefs.Metric)
}

func TestExpressionRefManager_Current_NilForUnknownSignal(t *testing.T) {
	eval := newTestCELEvaluator(t)
	logger := zaptest.NewLogger(t)
	refManager := topologyConnector.NewExpressionRefManager(logger, eval)

	cur := refManager.Current(stsSettingsModel.TRACES, "")
	require.Nil(t, cur)
}

func TestExpressionRefManager_Current_NilForUnknownComp(t *testing.T) {
	eval := newTestCELEvaluator(t)
	logger := zaptest.NewLogger(t)
	refManager := topologyConnector.NewExpressionRefManager(logger, eval)

	signals := []stsSettingsModel.OtelInputSignal{stsSettingsModel.METRICS}
	compBySig := map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelComponentMapping{
		stsSettingsModel.METRICS: {},
	}
	relBySig := make(map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelRelationMapping)
	refManager.Update(signals, compBySig, relBySig)

	cur := refManager.Current(stsSettingsModel.METRICS, "comp-1")
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

	cur := refManager.Current(stsSettingsModel.METRICS, "bad-comp")
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

	compRefs := refManager.Current(stsSettingsModel.METRICS, "comp")
	require.NotNil(t, compRefs)
	require.ElementsMatch(t, []string{"ns"}, compRefs.Span.AttributeKeys)
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

	require.Nil(t, refManager.Current(stsSettingsModel.METRICS, ""))
}

func TestComponentMappingFieldsCoverage(t *testing.T) {
	t.Run("OtelComponentMapping top-level coverage", func(t *testing.T) {
		assertStructFieldCoverage(
			t,
			"OtelComponentMapping",
			reflect.TypeOf(stsSettingsModel.OtelComponentMapping{}),
			append(
				ExpectedComponentMappingFields.TopLevelWalked,
				ExpectedComponentMappingFields.TopLevelSkipped...,
			),
			"Add walking logic in collectRefsForComponent or document why this field is intentionally skipped.",
		)
	})

	t.Run("OtelComponentMappingOutput coverage", func(t *testing.T) {
		assertStructFieldCoverage(
			t,
			"OtelComponentMappingOutput",
			reflect.TypeOf(stsSettingsModel.OtelComponentMappingOutput{}),
			ExpectedComponentMappingFields.Output,
			"Add walking logic in collectRefsForComponent and update ExpectedComponentMappingFields.Output,\n"+
				"or document why this field should be intentionally skipped.",
		)
	})

	t.Run("OtelComponentMappingFieldMapping coverage", func(t *testing.T) {
		assertStructFieldCoverage(
			t,
			"OtelComponentMappingFieldMapping",
			reflect.TypeOf(stsSettingsModel.OtelComponentMappingFieldMapping{}),
			ExpectedComponentMappingFields.FieldMapping,
			"Add walking logic for Optional/Required fields in collectRefsForComponent\n"+
				"or document why this field should be intentionally skipped.",
		)
	})
}

func TestRelationMappingFieldsCoverage(t *testing.T) {
	t.Run("OtelRelationMapping top-level coverage", func(t *testing.T) {
		assertStructFieldCoverage(
			t,
			"OtelRelationMapping",
			reflect.TypeOf(stsSettingsModel.OtelRelationMapping{}),
			append(
				ExpectedRelationMappingFields.TopLevelWalked,
				ExpectedRelationMappingFields.TopLevelSkipped...,
			),
			"Add walking logic in collectRefsForRelation or document why this field is intentionally skipped.",
		)
	})

	assertStructFieldCoverage(
		t,
		"OtelRelationMappingOutput",
		reflect.TypeOf(stsSettingsModel.OtelRelationMappingOutput{}),
		ExpectedRelationMappingFields.Output,
		"Add walking logic in collectRefsForRelation or update the expected field list.",
	)
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

func assertStructFieldCoverage(
	t *testing.T,
	structName string,
	structType reflect.Type,
	expectedFields []string,
	contextHint string,
) {
	t.Helper()

	actualFields := getStructFieldNames(structType)

	expected := make(map[string]struct{}, len(expectedFields))
	for _, f := range expectedFields {
		expected[f] = struct{}{}
	}

	actual := make(map[string]struct{}, len(actualFields))
	for _, f := range actualFields {
		actual[f] = struct{}{}
	}

	var missing []string
	for field := range actual {
		if _, ok := expected[field]; !ok {
			missing = append(missing, field)
		}
	}

	var obsolete []string
	for field := range expected {
		if _, ok := actual[field]; !ok {
			obsolete = append(obsolete, field)
		}
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf(
			"New fields detected in %s that are not covered by expression reference walking: %v\n%s",
			structName,
			missing,
			contextHint,
		)
	}

	if len(obsolete) > 0 {
		sort.Strings(obsolete)
		t.Errorf(
			"Fields documented for %s no longer exist: %v\nUpdate the test expectations.",
			structName,
			obsolete,
		)
	}
}

func getStructFieldNames(t reflect.Type) []string {
	require.Equal(nil, t.Kind(), reflect.Struct)

	var fields []string
	for i := 0; i < t.NumField(); i++ {
		fields = append(fields, t.Field(i).Name)
	}
	return fields
}

func sExpr(s string) stsSettingsModel.OtelStringExpression {
	return stsSettingsModel.OtelStringExpression{Expression: s}
}

func aExpr(s string) stsSettingsModel.OtelAnyExpression {
	return stsSettingsModel.OtelAnyExpression{Expression: s}
}

func ptr[T any](v T) *T { return &v }
