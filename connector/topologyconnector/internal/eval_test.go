//nolint:testpackage
package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/stretchr/testify/assert"
)

type mockEvalExpressionEvaluator struct {
	varExpressionLookup map[string]any
	varErrsLookup       map[string]error
}

func (f *mockEvalExpressionEvaluator) EvalStringExpression(_ settingsproto.OtelStringExpression, _ *ExpressionEvalContext) (string, error) {
	return "", nil
}

func (f *mockEvalExpressionEvaluator) EvalAnyExpression(expr settingsproto.OtelAnyExpression, _ *ExpressionEvalContext) (any, error) {
	if err, ok := f.varErrsLookup[expr.Expression]; ok {
		return "", err
	}
	if res, ok := f.varExpressionLookup[expr.Expression]; ok {
		return res, nil
	}
	return "", nil
}

func (f *mockEvalExpressionEvaluator) EvalOptionalStringExpression(_ *settingsproto.OtelStringExpression, _ *ExpressionEvalContext) (*string, error) {
	//nolint:nilnil
	return nil, nil
}

func (f *mockEvalExpressionEvaluator) EvalBooleanExpression(_ settingsproto.OtelBooleanExpression, _ *ExpressionEvalContext) (bool, error) {
	return false, nil
}

func (f *mockEvalExpressionEvaluator) EvalMapExpression(_ settingsproto.OtelAnyExpression, _ *ExpressionEvalContext) (map[string]any, error) {
	//nolint:nilnil
	return nil, nil
}

func (f *mockEvalExpressionEvaluator) GetStringExpressionAST(_ settingsproto.OtelStringExpression) (*GetASTResult, error) {
	//nolint:nilnil
	return nil, nil
}

func (f *mockEvalExpressionEvaluator) GetBooleanExpressionAST(_ settingsproto.OtelBooleanExpression) (*GetASTResult, error) {
	//nolint:nilnil
	return nil, nil
}

func (f *mockEvalExpressionEvaluator) GetMapExpressionAST(_ settingsproto.OtelAnyExpression) (*GetASTResult, error) {
	//nolint:nilnil
	return nil, nil
}

func (f *mockEvalExpressionEvaluator) GetAnyExpressionAST(_ settingsproto.OtelAnyExpression) (*GetASTResult, error) {
	//nolint:nilnil
	return nil, nil
}

type pair struct {
	name, expr string
}

func varMappings(pairs ...pair) []settingsproto.OtelVariableMapping {
	mappings := make([]settingsproto.OtelVariableMapping, len(pairs))
	for i, p := range pairs {
		mappings[i] = settingsproto.OtelVariableMapping{
			Name:  p.name,
			Value: settingsproto.OtelAnyExpression{Expression: p.expr},
		}
	}
	return mappings
}

func TestEvalVariables(t *testing.T) {
	t.Parallel()

	notFoundErr := errors.New("not found")
	notFoundVarErr := errors.New("not found variable with name: not-existing-variable")

	tests := []struct {
		name                string
		vars                []settingsproto.OtelVariableMapping
		varExpressionLookup map[string]any
		varErrsLookup       map[string]error
		want                map[string]any
		expectErr           error
	}{
		{
			name:      "nil variables",
			vars:      nil,
			want:      map[string]any{},
			expectErr: nil,
		},
		{
			name:      "empty variables",
			vars:      []settingsproto.OtelVariableMapping{},
			want:      map[string]any{},
			expectErr: nil,
		},
		{
			name: "literal variable",
			vars: varMappings(
				pair{"variable1", "static"},
			),
			varExpressionLookup: map[string]any{"static": "it is static value"},
			want:                map[string]any{"variable1": "it is static value"},
		},
		{
			name: "refer to span attribute",
			vars: varMappings(
				pair{"variable1", "spanAttributes.str-attr"},
				pair{"variable2", "spanAttributes.bool-attr"},
			),
			varExpressionLookup: map[string]any{
				"spanAttributes.str-attr":  "Hello",
				"spanAttributes.bool-attr": true,
			},
			want: map[string]any{
				"variable1": "Hello",
				"variable2": true,
			},
		},
		{
			name: "evaluate variables from span, scope and resource",
			vars: varMappings(
				pair{"spanVariable", "spanAttributes.str-attr"},
				pair{"scopeVariable", "scopeAttributes.str-attr"},
				pair{"resourceVariable", "resourceAttributes.str-attr"},
			),
			varExpressionLookup: map[string]any{
				"spanAttributes.str-attr":     "Hello",
				"scopeAttributes.str-attr":    "it is scope attribute",
				"resourceAttributes.str-attr": "it is resource attribute",
			},
			want: map[string]any{
				"spanVariable":     "Hello",
				"scopeVariable":    "it is scope attribute",
				"resourceVariable": "it is resource attribute",
			},
		},
		{
			name: "error when attribute not found",
			vars: varMappings(
				pair{"variable1", "spanAttributes.not-existing"},
			),
			varErrsLookup: map[string]error{
				"spanAttributes.not-existing": notFoundErr,
			},
			want:      map[string]any{},
			expectErr: fmt.Errorf("variable \"variable1\" evaluation failed: %w", notFoundErr),
		},
		{
			name: "error not found related variable",
			vars: varMappings(
				pair{"variable1", "spanAttributes.str-attr"},
				pair{"variable2", "vars.not-existing-variable"},
			),
			varExpressionLookup: map[string]any{"spanAttributes.str-attr": "Hello"},
			varErrsLookup:       map[string]error{"vars.not-existing-variable": notFoundVarErr},
			want: map[string]any{
				"variable1": "Hello",
			},
			expectErr: fmt.Errorf("variable \"variable2\" evaluation failed: %w", notFoundVarErr),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := tt.vars
			got, errs := EvalVariables(
				&mockEvalExpressionEvaluator{
					varExpressionLookup: tt.varExpressionLookup,
					varErrsLookup:       tt.varErrsLookup,
				},
				NewSpanEvalContext(nil, nil, nil), &vars,
			)

			if tt.expectErr != nil {
				assert.Equal(t, tt.expectErr.Error(), errs[0].Error(), "errors mismatch")
				require.Nil(t, got)
			} else {
				assert.Equal(t, tt.want, got, "resolved variables mismatch")
				require.Nil(t, errs)
			}
		})
	}
}

func TestEvalVariables_withRealContext(t *testing.T) {
	span := ptrace.NewSpan()
	span.Attributes().PutStr("str-attr", "hello")
	span.Attributes().PutBool("bool-attr", true)

	scope := ptrace.NewScopeSpans()
	scope.Scope().Attributes().PutStr("scope-attr", "world")

	resource := ptrace.NewResourceSpans()
	resource.Resource().Attributes().PutStr("res-attr", "infra")

	vars := varMappings(
		pair{"var1", "spanAttributes.str-attr"},
		pair{"var2", "spanAttributes.bool-attr"},
		pair{"var3", "scopeAttributes.scope-attr"},
		pair{"var4", "resourceAttributes.res-attr"},
	)

	fakeEval := &mockEvalExpressionEvaluator{
		varExpressionLookup: map[string]any{
			"spanAttributes.str-attr":     "hello",
			"spanAttributes.bool-attr":    true,
			"scopeAttributes.scope-attr":  "world",
			"resourceAttributes.res-attr": "infra",
		},
	}

	// Pass along constructed span, scope and resource to validate plumbing works as expected
	got, errs := EvalVariables(
		fakeEval,
		NewSpanEvalContext(
			NewSpan("name", "client", "ok", "", span.Attributes().AsRaw()),
			NewScope("name", "version", scope.Scope().Attributes().AsRaw()),
			NewResource(resource.Resource().Attributes().AsRaw()),
		),
		&vars,
	)

	require.Nil(t, errs)
	require.Equal(t, map[string]any{
		"var1": "hello",
		"var2": true,
		"var3": "world",
		"var4": "infra",
	}, got)
}

func TestFilterVarsByName(t *testing.T) {
	t.Parallel()

	all := varMappings(
		pair{"a", "x"},
		pair{"b", "y"},
		pair{"c", "z"},
	)

	t.Run("nil names returns the original slice (no filtering)", func(t *testing.T) {
		got := FilterVarsByName(&all, nil)
		require.Equal(t, all, got)
	})

	t.Run("nil names with nil vars returns nil", func(t *testing.T) {
		got := FilterVarsByName(nil, nil)
		require.Nil(t, got)
	})

	t.Run("non-nil names with nil vars returns nil", func(t *testing.T) {
		got := FilterVarsByName(nil, map[string]struct{}{"a": {}})
		require.Nil(t, got)
	})

	t.Run("empty names returns empty slice", func(t *testing.T) {
		got := FilterVarsByName(&all, map[string]struct{}{})
		require.Empty(t, got)
	})

	t.Run("filters to the named subset, preserving order", func(t *testing.T) {
		got := FilterVarsByName(&all, map[string]struct{}{"a": {}, "c": {}})
		require.Len(t, got, 2)
		require.Equal(t, "a", got[0].Name)
		require.Equal(t, "c", got[1].Name)
	})

	t.Run("names containing entries not in vars are silently ignored (no false-add)", func(t *testing.T) {
		got := FilterVarsByName(&all, map[string]struct{}{"a": {}, "ghost": {}})
		require.Len(t, got, 1)
		require.Equal(t, "a", got[0].Name)
	})
}

func TestCollectVarReferences(t *testing.T) {
	t.Parallel()

	// Real evaluator: covers the happy paths where the AST is actually fetched.
	realEval := newRealCelEvaluator(t)

	t.Run("no expressions returns an empty (non-nil) map", func(t *testing.T) {
		got := CollectVarReferences(realEval)
		require.NotNil(t, got)
		require.Empty(t, got)
	})

	t.Run("expression with no vars returns an empty map", func(t *testing.T) {
		got := CollectVarReferences(
			realEval,
			settingsproto.OtelStringExpression{Expression: `resource.attributes["service.name"]`},
		)
		require.NotNil(t, got)
		require.Empty(t, got)
	})

	t.Run("single expression with vars returns the referenced names", func(t *testing.T) {
		got := CollectVarReferences(
			realEval,
			settingsproto.OtelStringExpression{Expression: `"urn:" + vars.ns + "/" + vars.name`},
		)
		require.Len(t, got, 2)
		_, hasNs := got["ns"]
		_, hasName := got["name"]
		require.True(t, hasNs)
		require.True(t, hasName)
	})

	t.Run("AST fetch failure returns nil (caller falls back to evaluating all vars)", func(t *testing.T) {
		// mockEvalExpressionEvaluator.GetStringExpressionAST returns (nil, nil), which the
		// helper must treat as "could not statically determine — assume all vars are needed".
		got := CollectVarReferences(
			&mockEvalExpressionEvaluator{},
			settingsproto.OtelStringExpression{Expression: `vars.ns`},
		)
		require.Nil(t, got, "nil result is the safety signal for the DELETE path to fall back to evaluating all vars")
	})

	t.Run("AST fetch error returns nil", func(t *testing.T) {
		got := CollectVarReferences(
			&errorOnGetASTEvaluator{},
			settingsproto.OtelStringExpression{Expression: `vars.ns`},
		)
		require.Nil(t, got)
	})
}

// errorOnGetASTEvaluator returns an error from every Get*ExpressionAST call to exercise the
// nil-fallback contract of CollectVarReferences.
type errorOnGetASTEvaluator struct {
	mockEvalExpressionEvaluator
}

func (e *errorOnGetASTEvaluator) GetStringExpressionAST(_ settingsproto.OtelStringExpression) (*GetASTResult, error) {
	return nil, errors.New("boom")
}

func newRealCelEvaluator(t *testing.T) *CelEvaluator {
	t.Helper()
	eval, err := NewCELEvaluator(
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
