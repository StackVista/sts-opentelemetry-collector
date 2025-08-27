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

func TestEvalStringExpression_SpanAttr(t *testing.T) {
	eval, err := NewCELEvaluator()
	require.NoError(t, err)

	ctx := makeContext()

	result, err := eval.EvalStringExpression(settings.OtelStringExpression{Expression: `spanAttributes["http.method"]`}, &ctx)
	require.NoError(t, err)
	require.Equal(t, "GET", result)
}

func TestEvalStringExpression_Vars(t *testing.T) {
	eval, _ := NewCELEvaluator()
	ctx := makeContext()

	result, err := eval.EvalStringExpression(settings.OtelStringExpression{Expression: `vars.namespace`}, &ctx)
	require.NoError(t, err)
	require.Equal(t, "test", result)
}

func TestEvalBooleanExpression(t *testing.T) {
	eval, _ := NewCELEvaluator()
	ctx := makeContext()

	result, err := eval.EvalBooleanExpression(settings.OtelBooleanExpression{Expression: `resourceAttributes["cloud.provider"] == "aws"`}, &ctx)
	require.NoError(t, err)
	require.True(t, result)
}

func TestEvalOptionalStringExpression_Nil(t *testing.T) {
	eval, _ := NewCELEvaluator()
	ctx := makeContext()

	result, err := eval.EvalOptionalStringExpression(nil, &ctx)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestEvalOptionalStringExpression_NonNil(t *testing.T) {
	eval, _ := NewCELEvaluator()
	ctx := makeContext()

	expr := settings.OtelStringExpression{Expression: `"static-string"`}
	result, err := eval.EvalOptionalStringExpression(&expr, &ctx)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "static-string", *result)
}

func TestEvalTypeMismatch(t *testing.T) {
	eval, _ := NewCELEvaluator()
	ctx := makeContext()

	// String expression but evaluated with boolean
	_, err := eval.EvalBooleanExpression(settings.OtelBooleanExpression{Expression: `resourceAttributes["cloud.provider"]`}, &ctx)
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
