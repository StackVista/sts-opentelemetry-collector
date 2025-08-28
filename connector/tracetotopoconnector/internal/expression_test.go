package internal

import (
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"testing"
)

func makeContext() ExpressionEvalContext {
	span := ptrace.NewSpan()
	span.Attributes().PutStr("http.method", "GET")
	span.Attributes().PutInt("http.status_code", 200)
	span.Attributes().PutStr("user.id", "123")
	span.Attributes().PutStr("retries", "5")

	scope := ptrace.NewScopeSpans()
	scope.Scope().Attributes().PutStr("otel.scope.name", "io.opentelemetry.instrumentation.http")
	scope.Scope().Attributes().PutStr("otel.scope.version", "1.2.3")

	res := ptrace.NewResourceSpans()
	res.Resource().Attributes().PutStr("service.name", "cart-service")
	res.Resource().Attributes().PutStr("cloud.provider", "aws")
	res.Resource().Attributes().PutStr("service.instance.id", "627cc493")
	res.Resource().Attributes().PutStr("service.version", "1.2.3")
	res.Resource().Attributes().PutStr("host.name", "ip-10-1-2-3.ec2.internal")
	res.Resource().Attributes().PutStr("os.type", "linux")
	res.Resource().Attributes().PutStr("process.pid", "12345")
	res.Resource().Attributes().PutStr("env", "dev")

	vars := map[string]string{
		"namespace": "test",
	}

	return ExpressionEvalContext{
		Span:     span,
		Scope:    scope,
		Resource: res,
		Vars:     vars,
	}
}

func TestEvalStringExpression(t *testing.T) {
	eval, err := NewCELEvaluator()
	require.NoError(t, err)

	ctx := makeContext()

	tests := []struct {
		name                string
		expr                string
		expected            string
		expectErrorContains string
	}{
		{
			name:     "support resource attributes",
			expr:     `resourceAttributes["service.name"]`,
			expected: "cart-service",
		},
		{
			name:     "support span attributes",
			expr:     `spanAttributes["http.method"]`,
			expected: "GET",
		},
		{
			name:     "support scope attributes",
			expr:     `scopeAttributes["otel.scope.name"]`,
			expected: "io.opentelemetry.instrumentation.http",
		},
		{
			name:     "support custom vars",
			expr:     `vars.namespace`,
			expected: "test",
		},
		{
			name:                "missing attribute key returns error",
			expr:                `resourceAttributes["not-existing-attr"]`,
			expectErrorContains: "no such key",
		},
		{
			name:     "coerce int to string",
			expr:     `spanAttributes["retries"]`,
			expected: "5",
		},
		{
			name:                "not coerce boolean to string",
			expr:                `true`,
			expectErrorContains: "expression did not evaluate to string",
		},
		{
			name:                "unsupported type returns error",
			expr:                `spanAttributes`, // whole map, not string
			expectErrorContains: "expression did not evaluate to string, got: map",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := eval.EvalStringExpression(
				settings.OtelStringExpression{Expression: tt.expr},
				&ctx,
			)

			if tt.expectErrorContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectErrorContains)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestEvalBooleanExpression(t *testing.T) {
	eval, err := NewCELEvaluator()
	require.NoError(t, err)

	ctx := makeContext()

	tests := []struct {
		name                string
		expr                string
		expected            bool
		expectErrorContains string
	}{
		{
			name:     "simple true condition",
			expr:     `resourceAttributes["cloud.provider"] == "aws"`,
			expected: true,
		},
		{
			name:     "simple false condition",
			expr:     `resourceAttributes["cloud.provider"] == "gcp"`,
			expected: false,
		},
		{
			name:     "complex boolean expression with AND",
			expr:     `resourceAttributes["cloud.provider"] == "aws" && spanAttributes["http.method"] == "GET"`,
			expected: true,
		},
		{
			name:     "complex boolean expression with OR",
			expr:     `resourceAttributes["cloud.provider"] == "gcp" || spanAttributes["http.method"] == "GET"`,
			expected: true,
		},
		{
			name:                "missing attribute key returns error",
			expr:                `resourceAttributes["not-existing-attr"]`,
			expectErrorContains: "no such key",
		},
		{
			name:                "type mismatch returns error",
			expr:                `spanAttributes["retries"]`, // int but expected boolean
			expectErrorContains: "condition did not evaluate to boolean",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := eval.EvalBooleanExpression(
				settings.OtelBooleanExpression{Expression: tt.expr},
				&ctx,
			)

			if tt.expectErrorContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectErrorContains)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestEvalOptionalStringExpression(t *testing.T) {
	eval, err := NewCELEvaluator()
	require.NoError(t, err)

	ctx := makeContext()

	tests := []struct {
		name     string
		expr     *settings.OtelStringExpression
		expected *string
	}{
		{
			name:     "nil expression returns nil",
			expr:     nil,
			expected: nil,
		},
		{
			name:     "non-nil expression returns string",
			expr:     &settings.OtelStringExpression{Expression: `"static-string"`},
			expected: ptr("static-string"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := eval.EvalOptionalStringExpression(tt.expr, &ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestBoolEvalTypeMismatch(t *testing.T) {
	eval, _ := NewCELEvaluator()
	ctx := makeContext()

	// String expression but evaluated with boolean
	_, err := eval.EvalBooleanExpression(settings.OtelBooleanExpression{Expression: `resourceAttributes["cloud.provider"]`}, &ctx)
	require.Error(t, err)
}

func TestStringEvalTypeMismatch(t *testing.T) {
	eval, _ := NewCELEvaluator()
	ctx := makeContext()

	// Bool expression but evaluated with string
	_, err := eval.EvalStringExpression(settings.OtelStringExpression{Expression: `resourceAttributes["cloud.provider"] == "aws"`}, &ctx)
	require.Error(t, err)
}

func TestEvalCacheReuse(t *testing.T) {
	eval, _ := NewCELEvaluator()
	ctx := makeContext()

	expr := settings.OtelBooleanExpression{Expression: `spanAttributes["retries"] == 2`}

	_, err := eval.EvalBooleanExpression(expr, &ctx)
	require.NoError(t, err)
	require.Equal(t, 1, eval.cacheSize()) // compiled once

	_, err = eval.EvalBooleanExpression(expr, &ctx)
	require.NoError(t, err)
	require.Equal(t, 1, eval.cacheSize()) // still one entry

	// different expression should increase cache size
	other := settings.OtelBooleanExpression{Expression: `spanAttributes["retries"] == 10`}
	_, err = eval.EvalBooleanExpression(other, &ctx)
	require.NoError(t, err)
	require.Equal(t, 2, eval.cacheSize())
}
