package internal

import (
	"errors"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestEvalVariables(t *testing.T) {
	t.Parallel()

	testSpan := ptrace.NewSpan()
	testSpan.Attributes().PutBool("bool-attr", true)
	testSpan.Attributes().PutStr("str-attr", "Hello")

	testScope := ptrace.NewScopeSpans()
	testScope.Scope().Attributes().PutStr("str-attr", "it is scope attribute")

	testResource := ptrace.NewResourceSpans()
	testResource.Resource().Attributes().PutStr("str-attr", "it is resource attribute")

	tests := []struct {
		name      string
		vars      *[]settings.OtelVariableMapping
		span      *ptrace.Span
		scope     *ptrace.ScopeSpans
		resource  *ptrace.ResourceSpans
		want      map[string]string
		expectErr map[string]error
	}{
		{
			name:      "NilVariables",
			vars:      nil,
			span:      &testSpan,
			scope:     &testScope,
			resource:  &testResource,
			want:      map[string]string{},
			expectErr: nil,
		},
		{
			name:      "EmptyVariables",
			vars:      &[]settings.OtelVariableMapping{},
			span:      &testSpan,
			scope:     &testScope,
			resource:  &testResource,
			want:      map[string]string{},
			expectErr: nil,
		},
		{
			name: "single literally variable",
			vars: &[]settings.OtelVariableMapping{
				{
					Name:  "variable1",
					Value: settings.OtelStringExpression{Expression: "it is static value"},
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			want: map[string]string{
				"variable1": "it is static value",
			},
			expectErr: nil,
		},
		{
			name: "refer to span attribute",
			vars: &[]settings.OtelVariableMapping{
				{
					Name:  "variable1",
					Value: settings.OtelStringExpression{Expression: "spanAttributes.str-attr"},
				},
				{
					Name:  "variable2",
					Value: settings.OtelStringExpression{Expression: "spanAttributes.bool-attr"},
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			want: map[string]string{
				"variable1": "Hello",
				"variable2": "true",
			},
			expectErr: nil,
		},
		{
			name: "refers to another variable",
			vars: &[]settings.OtelVariableMapping{
				{
					Name:  "variable1",
					Value: settings.OtelStringExpression{Expression: "spanAttributes.str-attr"},
				},
				{
					Name:  "variable2",
					Value: settings.OtelStringExpression{Expression: "vars.variable1"},
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			want: map[string]string{
				"variable1": "Hello",
				"variable2": "Hello",
			},
			expectErr: nil,
		},
		{
			name: "evaluate variables from span, scope and resource",
			vars: &[]settings.OtelVariableMapping{
				{
					Name:  "spanVariable",
					Value: settings.OtelStringExpression{Expression: "spanAttributes.str-attr"},
				},
				{
					Name:  "scopeVariable",
					Value: settings.OtelStringExpression{Expression: "scopeAttributes.str-attr"},
				},
				{
					Name:  "resourceVariable",
					Value: settings.OtelStringExpression{Expression: "resourceAttributes.str-attr"},
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			want: map[string]string{
				"spanVariable":     "Hello",
				"scopeVariable":    "it is scope attribute",
				"resourceVariable": "it is resource attribute",
			},
			expectErr: nil,
		},
		{
			name: "error, not found attribute",
			vars: &[]settings.OtelVariableMapping{
				{
					Name:  "variable1",
					Value: settings.OtelStringExpression{Expression: "spanAttributes.not-existing-attr"},
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			want:     map[string]string{},
			expectErr: map[string]error{
				"variable1": errors.New("Not found span attribute with name: not-existing-attr"),
			},
		},
		{
			name: "error, not found related variable",
			vars: &[]settings.OtelVariableMapping{
				{
					Name:  "variable1",
					Value: settings.OtelStringExpression{Expression: "spanAttributes.str-attr"},
				},
				{
					Name:  "variable2",
					Value: settings.OtelStringExpression{Expression: "vars.not-existing-variable"},
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			want: map[string]string{
				"variable1": "Hello",
			},
			expectErr: map[string]error{
				"variable2": errors.New("Not found variable with name: not-existing-variable"),
			},
		},
		{
			name: "error, mid variable can't be evaluated",
			vars: &[]settings.OtelVariableMapping{
				{
					Name:  "variable1",
					Value: settings.OtelStringExpression{Expression: "spanAttributes.str-attr"},
				},
				{
					Name:  "variable2",
					Value: settings.OtelStringExpression{Expression: "vars.not-existing-variable"},
				},
				{
					Name:  "variable3",
					Value: settings.OtelStringExpression{Expression: "vars.variable1"},
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			want: map[string]string{
				"variable1": "Hello",
				"variable3": "Hello",
			},
			expectErr: map[string]error{
				"variable2": errors.New("Not found variable with name: not-existing-variable"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EvalVariables(tt.span, tt.scope, tt.resource, tt.vars)
			assert.True(t, equalMaps(got, tt.want))
			assert.Equal(t, tt.expectErr, err)
		})
	}
}

// Helper function to compare maps.
func equalMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
