package internal_test

import (
	"context"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
)

func TestStringExpressionAstWalker_Walk(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		wantRefs []internal.Reference
	}{
		{
			name: "simple field access",
			expr: `${scope.name}`,
			wantRefs: []internal.Reference{
				{Root: "scope", Path: []string{"name"}},
			},
		},
		{
			name: "mixed resource/vars/datapoint attrs usage",
			expr: `ns-${resource.attributes["service.name"]}-${vars.namespace}-${datapoint.attributes["connection_type"]}`,
			wantRefs: []internal.Reference{
				{Root: "resource", Path: []string{"attributes", "service.name"}},
				{Root: "vars", Path: []string{"namespace"}},
				{Root: "datapoint", Path: []string{"attributes", "connection_type"}},
			},
		},
		{
			name: "span attribute select",
			expr: `${span.attributes["http.status_code"]}`,
			wantRefs: []internal.Reference{
				{Root: "span", Path: []string{"attributes", "http.status_code"}},
			},
		},
	}

	eval := newTestEvaluator(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := eval.GetStringExpressionAST(settings.OtelStringExpression{Expression: tt.expr})
			require.NoError(t, err)
			walker := internal.NewExpressionAstWalker()
			walker.Walk(res.CheckedAST.NativeRep().Expr())

			requireReferencesEqual(t, walker.GetReferences(), tt.wantRefs)
		})
	}
}

func TestBooleanExpressionAstWalker_Walk(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		wantRefs []internal.Reference
	}{
		{
			name: "simple field equality",
			expr: `scope.name == "foo"`,
			wantRefs: []internal.Reference{
				{Root: "scope", Path: []string{"name"}},
			},
		},
		{
			name: "chained field equality (with duplicate `connection_type` reference)",
			expr: `'client' in datapoint.attributes &&
                'server' in datapoint.attributes &&
                'client_service.namespace' in datapoint.attributes &&
                'connection_type' in datapoint.attributes && datapoint.attributes['connection_type'] == 'database'`,
			wantRefs: []internal.Reference{
				{Root: "datapoint", Path: []string{"attributes", "client"}},
				{Root: "datapoint", Path: []string{"attributes", "server"}},
				{Root: "datapoint", Path: []string{"attributes", "client_service.namespace"}},
				{Root: "datapoint", Path: []string{"attributes", "connection_type"}},
			},
		},
	}

	eval := newTestEvaluator(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := eval.GetBooleanExpressionAST(settings.OtelBooleanExpression{Expression: tt.expr})
			require.NoError(t, err)
			walker := internal.NewExpressionAstWalker()
			walker.Walk(res.CheckedAST.NativeRep().Expr())

			requireReferencesEqual(t, walker.GetReferences(), tt.wantRefs)
		})
	}
}

func TestMapExpressionAstWalker_Walk(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		wantRefs []internal.Reference
	}{
		{
			name: "map field access",
			expr: `${resource.attributes}`,
			wantRefs: []internal.Reference{
				{Root: "resource", Path: []string{"attributes"}},
			},
		},
	}

	eval := newTestEvaluator(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := eval.GetMapExpressionAST(settings.OtelAnyExpression{Expression: tt.expr})
			require.NoError(t, err)
			walker := internal.NewExpressionAstWalker()
			walker.Walk(res.CheckedAST.NativeRep().Expr())

			requireReferencesEqual(t, walker.GetReferences(), tt.wantRefs)
		})
	}
}

func TestAnyExpressionAstWalker_Walk(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		wantRefs []internal.Reference
	}{
		{
			name: "simple field equality",
			expr: `${scope.name == "foo"}`,
			wantRefs: []internal.Reference{
				{Root: "scope", Path: []string{"name"}},
			},
		},
		{
			name: "simple field access",
			expr: `${scope.name}`,
			wantRefs: []internal.Reference{
				{Root: "scope", Path: []string{"name"}},
			},
		},
	}

	eval := newTestEvaluator(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := eval.GetAnyExpressionAST(settings.OtelAnyExpression{Expression: tt.expr})
			require.NoError(t, err)
			walker := internal.NewExpressionAstWalker()
			walker.Walk(res.CheckedAST.NativeRep().Expr())

			requireReferencesEqual(t, walker.GetReferences(), tt.wantRefs)
		})
	}
}

func newTestEvaluator(t *testing.T) *internal.CelEvaluator {
	t.Helper()

	eval, err := internal.NewCELEvaluator(
		context.Background(),
		metrics.MeteredCacheSettings{
			Size:              10,
			EnableMetrics:     false,
			TTL:               5 * time.Second,
			TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		},
	)
	require.NoError(t, err)
	return eval
}

func requireReferencesEqual(
	t *testing.T,
	got []internal.Reference,
	want []internal.Reference,
) {
	t.Helper()

	gotSet := make(map[string]internal.Reference)
	for _, r := range got {
		gotSet[r.Key()] = r
	}

	require.Len(t, gotSet, len(want), "unexpected extra or duplicate references")

	for _, r := range want {
		key := r.Key()
		gr, ok := gotSet[key]
		require.True(t, ok, "missing expected ref: %s", key)
		require.Equal(t, r, gr)
	}
}
