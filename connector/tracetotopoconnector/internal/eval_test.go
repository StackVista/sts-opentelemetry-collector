//nolint:testpackage
package internal

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/stretchr/testify/assert"
)

type mockEvalExpressionEvaluator struct {
	varExpressionLookup map[string]string
	varErrsLookup       map[string]error
}

func (f *mockEvalExpressionEvaluator) EvalStringExpression(expr settings.OtelStringExpression, _ *ExpressionEvalContext) (string, error) {
	if err, ok := f.varErrsLookup[expr.Expression]; ok {
		return "", err
	}
	if res, ok := f.varExpressionLookup[expr.Expression]; ok {
		return res, nil
	}
	return "", nil
}

func (f *mockEvalExpressionEvaluator) EvalOptionalStringExpression(_ *settings.OtelStringExpression, _ *ExpressionEvalContext) (*string, error) {
	//nolint:nilnil
	return nil, nil
}

func (f *mockEvalExpressionEvaluator) EvalBooleanExpression(_ settings.OtelBooleanExpression, _ *ExpressionEvalContext) (bool, error) {
	return false, nil
}

func (f *mockEvalExpressionEvaluator) EvalMapExpression(_ settings.OtelStringExpression, _ *ExpressionEvalContext) (map[string]any, error) {
	//nolint:nilnil
	return nil, nil
}

type pair struct {
	name, expr string
}

func varMappings(pairs ...pair) []settings.OtelVariableMapping {
	mappings := make([]settings.OtelVariableMapping, len(pairs))
	for i, p := range pairs {
		mappings[i] = settings.OtelVariableMapping{
			Name:  p.name,
			Value: settings.OtelStringExpression{Expression: p.expr},
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
		vars                []settings.OtelVariableMapping
		varExpressionLookup map[string]string
		varErrsLookup       map[string]error
		want                map[string]string
		expectErr           error
	}{
		{
			name:      "nil variables",
			vars:      nil,
			want:      map[string]string{},
			expectErr: nil,
		},
		{
			name:      "empty variables",
			vars:      []settings.OtelVariableMapping{},
			want:      map[string]string{},
			expectErr: nil,
		},
		{
			name: "literal variable",
			vars: varMappings(
				pair{"variable1", "static"},
			),
			varExpressionLookup: map[string]string{"static": "it is static value"},
			want:                map[string]string{"variable1": "it is static value"},
		},
		{
			name: "refer to span attribute",
			vars: varMappings(
				pair{"variable1", "spanAttributes.str-attr"},
				pair{"variable2", "spanAttributes.bool-attr"},
			),
			varExpressionLookup: map[string]string{
				"spanAttributes.str-attr":  "Hello",
				"spanAttributes.bool-attr": "true",
			},
			want: map[string]string{
				"variable1": "Hello",
				"variable2": "true",
			},
		},
		{
			name: "evaluate variables from span, scope and resource",
			vars: varMappings(
				pair{"spanVariable", "spanAttributes.str-attr"},
				pair{"scopeVariable", "scopeAttributes.str-attr"},
				pair{"resourceVariable", "resourceAttributes.str-attr"},
			),
			varExpressionLookup: map[string]string{
				"spanAttributes.str-attr":     "Hello",
				"scopeAttributes.str-attr":    "it is scope attribute",
				"resourceAttributes.str-attr": "it is resource attribute",
			},
			want: map[string]string{
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
			want:      map[string]string{},
			expectErr: fmt.Errorf("variable \"variable1\" evaluation failed: %w", notFoundErr),
		},
		{
			name: "error not found related variable",
			vars: varMappings(
				pair{"variable1", "spanAttributes.str-attr"},
				pair{"variable2", "vars.not-existing-variable"},
			),
			varExpressionLookup: map[string]string{"spanAttributes.str-attr": "Hello"},
			varErrsLookup:       map[string]error{"vars.not-existing-variable": notFoundVarErr},
			want: map[string]string{
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
				nil, nil, nil, &vars,
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
		varExpressionLookup: map[string]string{
			"spanAttributes.str-attr":     "hello",
			"spanAttributes.bool-attr":    "true",
			"scopeAttributes.scope-attr":  "world",
			"resourceAttributes.res-attr": "infra",
		},
	}

	// Pass along constructed span, scope and resource to validate plumbing works as expected
	got, errs := EvalVariables(fakeEval, &span, &scope, &resource, &vars)

	require.Nil(t, errs)
	require.Equal(t, map[string]string{
		"var1": "hello",
		"var2": "true",
		"var3": "world",
		"var4": "infra",
	}, got)
}
