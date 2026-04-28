package internal_test

import (
	"context"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
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
			expr: `scope.name`,
			wantRefs: []internal.Reference{
				{Root: "scope", Path: []string{"name"}},
			},
		},
		{
			name: "mixed resource/vars/datapoint attrs usage",
			expr: `"ns-" + resource.attributes["service.name"] + "-" + vars.namespace + "-" + datapoint.attributes["connection_type"]`,
			wantRefs: []internal.Reference{
				{Root: "resource", Path: []string{"attributes", "service.name"}},
				{Root: "vars", Path: []string{"namespace"}},
				{Root: "datapoint", Path: []string{"attributes", "connection_type"}},
			},
		},
		{
			name: "span attribute select",
			expr: `span.attributes["http.status_code"]`,
			wantRefs: []internal.Reference{
				{Root: "span", Path: []string{"attributes", "http.status_code"}},
			},
		},
	}

	eval := newTestEvaluator(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := eval.GetStringExpressionAST(settingsproto.OtelStringExpression{Expression: tt.expr})
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
			res, err := eval.GetBooleanExpressionAST(settingsproto.OtelBooleanExpression{Expression: tt.expr})
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
			expr: `resource.attributes`,
			wantRefs: []internal.Reference{
				{Root: "resource", Path: []string{"attributes"}},
			},
		},
	}

	eval := newTestEvaluator(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := eval.GetMapExpressionAST(settingsproto.OtelAnyExpression{Expression: tt.expr})
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
			expr: `scope.name == "foo"`,
			wantRefs: []internal.Reference{
				{Root: "scope", Path: []string{"name"}},
			},
		},
		{
			name: "simple field access",
			expr: `scope.name`,
			wantRefs: []internal.Reference{
				{Root: "scope", Path: []string{"name"}},
			},
		},
	}

	eval := newTestEvaluator(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := eval.GetAnyExpressionAST(settingsproto.OtelAnyExpression{Expression: tt.expr})
			require.NoError(t, err)
			walker := internal.NewExpressionAstWalker()
			walker.Walk(res.CheckedAST.NativeRep().Expr())

			requireReferencesEqual(t, walker.GetReferences(), tt.wantRefs)
		})
	}
}

func TestPickOmitExpressionAstWalker_Walk(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		wantRefs []internal.Reference
	}{
		{
			name: "pick(log.body, [...]) extracts log.body reference",
			expr: "pick(log.body, ['metadata', 'spec'])",
			wantRefs: []internal.Reference{
				{Root: "log", Path: []string{"body"}},
			},
		},
		{
			name: "omit(log.body, [...]) extracts log.body reference",
			expr: "omit(log.body, ['status'])",
			wantRefs: []internal.Reference{
				{Root: "log", Path: []string{"body"}},
			},
		},
		{
			name: "pick(resource.attributes, [...]) extracts resource.attributes reference",
			expr: "pick(resource.attributes, ['service.name'])",
			wantRefs: []internal.Reference{
				{Root: "resource", Path: []string{"attributes"}},
			},
		},
		{
			name: "omit(span.attributes, [...]) extracts span.attributes reference",
			expr: "omit(span.attributes, ['http.method'])",
			wantRefs: []internal.Reference{
				{Root: "span", Path: []string{"attributes"}},
			},
		},
	}

	eval := newTestEvaluator(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := eval.GetAnyExpressionAST(settingsproto.OtelAnyExpression{Expression: tt.expr})
			require.NoError(t, err)
			walker := internal.NewExpressionAstWalker()
			walker.Walk(res.CheckedAST.NativeRep().Expr())

			requireReferencesEqual(t, walker.GetReferences(), tt.wantRefs)
		})
	}
}

func TestExpressionAstWalker_GetVarReferences(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		wantVars []string
	}{
		{
			name:     "no vars referenced",
			expr:     `resource.attributes["service.name"]`,
			wantVars: nil,
		},
		{
			name:     "single var",
			expr:     `"urn:server/" + vars.serverName`,
			wantVars: []string{"serverName"},
		},
		{
			name:     "multiple vars",
			expr:     `"urn:" + vars.namespace + "/" + vars.serverName + "/" + vars.kind`,
			wantVars: []string{"namespace", "serverName", "kind"},
		},
		{
			name:     "ternary with vars on both branches",
			expr:     `vars.kind == "X" ? vars.a : vars.b`,
			wantVars: []string{"kind", "a", "b"},
		},
		{
			name:     "nested var path collapses to root name",
			expr:     `vars.config.host`,
			wantVars: []string{"config"},
		},
		{
			name:     "var alongside other roots",
			expr:     `resource.attributes["k"] + vars.serverName + log.attributes["x"]`,
			wantVars: []string{"serverName"},
		},
	}

	eval := newTestEvaluator(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := eval.GetStringExpressionAST(settingsproto.OtelStringExpression{Expression: tt.expr})
			require.NoError(t, err)
			walker := internal.NewExpressionAstWalker()
			walker.Walk(res.CheckedAST.NativeRep().Expr())

			got := walker.GetVarReferences()
			require.Len(t, got, len(tt.wantVars))
			for _, name := range tt.wantVars {
				_, ok := got[name]
				require.True(t, ok, "missing var: %s", name)
			}
		})
	}
}

func TestCollectVarReferences_UnionAcrossExpressions(t *testing.T) {
	eval := newTestEvaluator(t)

	got := internal.CollectVarReferences(
		eval,
		settingsproto.OtelStringExpression{Expression: `"urn:" + vars.a + "/" + vars.b`},
		settingsproto.OtelStringExpression{Expression: `"urn:" + vars.b + "/" + vars.c`},
	)

	require.Len(t, got, 3)
	for _, name := range []string{"a", "b", "c"} {
		_, ok := got[name]
		require.True(t, ok, "missing var: %s", name)
	}
}

func TestCollectVarReferences_NoExpressions(t *testing.T) {
	eval := newTestEvaluator(t)
	got := internal.CollectVarReferences(eval)
	require.NotNil(t, got)
	require.Empty(t, got)
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
