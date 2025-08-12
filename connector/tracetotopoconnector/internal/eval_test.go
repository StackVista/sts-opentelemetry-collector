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

	tests := []struct {
		name      string
		vars      *[]settings.OtelVariableMapping
		span      *ptrace.Span
		want      map[string]string
		expectErr map[string]error
	}{
		{
			name:      "NilVariables",
			vars:      nil,
			span:      &testSpan,
			want:      map[string]string{},
			expectErr: nil,
		},
		{
			name:      "EmptyVariables",
			vars:      &[]settings.OtelVariableMapping{},
			span:      &testSpan,
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
			span: &testSpan,
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
					Value: settings.OtelStringExpression{Expression: "attributes.str-attr"},
				},
				{
					Name:  "variable2",
					Value: settings.OtelStringExpression{Expression: "attributes.bool-attr"},
				},
			},
			span: &testSpan,
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
					Value: settings.OtelStringExpression{Expression: "attributes.str-attr"},
				},
				{
					Name:  "variable2",
					Value: settings.OtelStringExpression{Expression: "vars.variable1"},
				},
			},
			span: &testSpan,
			want: map[string]string{
				"variable1": "Hello",
				"variable2": "Hello",
			},
			expectErr: nil,
		},
		{
			name: "error, not found attribute",
			vars: &[]settings.OtelVariableMapping{
				{
					Name:  "variable1",
					Value: settings.OtelStringExpression{Expression: "attributes.not-existing-attr"},
				},
			},
			span: &testSpan,
			want: map[string]string{},
			expectErr: map[string]error{
				"variable1": errors.New("Not found attribute with name: not-existing-attr"),
			},
		},
		{
			name: "error, not found related variable",
			vars: &[]settings.OtelVariableMapping{
				{
					Name:  "variable1",
					Value: settings.OtelStringExpression{Expression: "attributes.str-attr"},
				},
				{
					Name:  "variable2",
					Value: settings.OtelStringExpression{Expression: "vars.not-existing-variable"},
				},
			},
			span: &testSpan,
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
					Value: settings.OtelStringExpression{Expression: "attributes.str-attr"},
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
			span: &testSpan,
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
			got, err := EvalVariables(tt.vars, tt.span)
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
