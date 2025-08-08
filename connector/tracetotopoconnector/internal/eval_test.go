package internal

import (
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestEvalVariables(t *testing.T) {
	t.Parallel()

	testSpan := ptrace.NewSpan()
	testSpan.Attributes().PutBool("bool-attr", true)
	testSpan.Attributes().PutStr("str-attr", "Hello")

	tests := []struct {
		name    string
		vars    *[]settings.OtelVariableMapping
		span    *ptrace.Span
		want    map[string]string
		wantErr bool
	}{
		{
			name:    "NilVariables",
			vars:    nil,
			span:    &testSpan,
			want:    map[string]string{},
			wantErr: false,
		},
		{
			name:    "EmptyVariables",
			vars:    &[]settings.OtelVariableMapping{},
			span:    &testSpan,
			want:    map[string]string{},
			wantErr: false,
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
			wantErr: false,
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
			wantErr: false,
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
			wantErr: false,
		},
		{
			name: "error, not found attribute",
			vars: &[]settings.OtelVariableMapping{
				{
					Name:  "variable1",
					Value: settings.OtelStringExpression{Expression: "attributes.not-existing-attr"},
				},
			},
			span:    &testSpan,
			want:    map[string]string{},
			wantErr: true,
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
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EvalVariables(tt.vars, tt.span)
			if (err != nil) != tt.wantErr {
				t.Errorf("EvalVariables() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !equalMaps(got, tt.want) {
				t.Errorf("EvalVariables() = %v, want %v", got, tt.want)
			}
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
