//nolint:testpackage
package internal

import (
	"context"
	"testing"
	"time"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
)

func makeContext(includeSpanAttributes bool, includeMetricAttributes bool) ExpressionEvalContext {
	spanAttributes := map[string]any{
		"http.method":      "GET",
		"http.status_code": int64(200),
		"user.id":          "123",
		"retries":          "5",
		"sampled":          true,
		"pi":               3.14,
	}

	metricAttributes := map[string]any{
		"http.method":      "GET",
		"http.status_code": int64(200),
		"user.id":          "123",
	}

	scopeAttributes := map[string]any{
		"otel.scope.name":    "io.opentelemetry.instrumentation.http",
		"otel.scope.version": "1.2.3",
	}

	resourceAttributes := map[string]any{
		"service.name":   "cart-service",
		"cloud.provider": "aws",
		"env":            "dev",
		// slice attribute type
		"process.command_args": []any{"java", "-jar", "app.jar"},
		// map attribute type
		"deployment": map[string]any{
			"region": "eu-west-1",
			"env":    "prod",
		},
	}

	vars := map[string]any{
		"namespace": "test",
	}

	context := ExpressionEvalContext{
		ScopeAttributes:    scopeAttributes,
		ResourceAttributes: resourceAttributes,
		Vars:               vars,
	}
	if includeSpanAttributes {
		context.SpanAttributes = spanAttributes
	}
	if includeMetricAttributes {
		context.MetricAttributes = metricAttributes
	}
	return context
}

func makeMeteredCacheSettings(size int, ttl time.Duration) metrics.MeteredCacheSettings {
	return metrics.MeteredCacheSettings{
		Size:              size,
		EnableMetrics:     false,
		TTL:               ttl,
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}
}

func TestEvalStringExpression(t *testing.T) {
	eval, err := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	require.NoError(t, err)

	ctx := makeContext(true, true)

	tests := []struct {
		name        string
		expr        string
		expected    string
		errContains string
	}{
		{
			name:     "simple string",
			expr:     "static string",
			expected: "static string",
		},
		{
			name:     "simple string with quotes",
			expr:     "'static string'",
			expected: "'static string'",
		},
		{
			name:     "simple string with double quotes",
			expr:     `"static string"`,
			expected: `"static string"`,
		},
		{
			name:     "empty string",
			expr:     "",
			expected: "",
		},
		{
			name:     "support resource attributes",
			expr:     `${resourceAttributes["service.name"]}`,
			expected: "cart-service",
		},
		{
			name:     "support attributes access (with single quotes)",
			expr:     "${resourceAttributes['service.name']}",
			expected: "cart-service",
		},
		{
			name:     "support span attributes",
			expr:     `${spanAttributes["http.method"]}`,
			expected: "GET",
		},
		{
			name:     "support metric attributes",
			expr:     `${metricAttributes["http.method"]}`,
			expected: "GET",
		},
		{
			name:     "support scope attributes",
			expr:     `${scopeAttributes["otel.scope.name"]}`,
			expected: "io.opentelemetry.instrumentation.http",
		},
		{
			name:     "support custom vars",
			expr:     `${vars.namespace}`,
			expected: "test",
		},
		{
			name:        "missing attribute key returns error",
			expr:        `${resourceAttributes["not-existing-attr"]}`,
			errContains: "no such key",
		},
		{
			name:     "coerce int to string",
			expr:     `${spanAttributes["retries"]}`,
			expected: "5",
		},
		{
			name:        "not coerce boolean to string",
			expr:        `${true}`,
			errContains: "cannot convert 'bool' to 'string'",
		},
		{
			name:        "unsupported type returns error",
			expr:        `${spanAttributes}`, // whole map, not string
			errContains: "expected string type, got: map(string, dyn), for expression '${spanAttributes}'",
		},
		{
			name:     "support string interpolation with vars",
			expr:     `service-${resourceAttributes["env"]}`,
			expected: "service-dev",
		},
		{
			name:     "support string interpolation with resource attributes",
			expr:     `ns-${resourceAttributes["service.name"]}`,
			expected: "ns-cart-service",
		},
		{
			name:     "multiple interpolations at once",
			expr:     `ns-${resourceAttributes["service.name"]}-${vars.namespace}-${resourceAttributes["service.name"].matches(R'cart-.*') ? 'cart' : 'not-cart'}-end`,
			expected: "ns-cart-service-test-cart-end",
		},
		{
			name:     "another complex expression",
			expr:     `${resourceAttributes["service.name"].matches(R'cart-.*') ? ( spanAttributes["http.status_code"] < 400 ? 'good-cart' : 'bad-cart' ) : 'not-cart'}`,
			expected: "good-cart",
		},
		{
			name:     "support key existence checks",
			expr:     `${'service.name' in resourceAttributes ? 'yes' : 'no'}`,
			expected: "yes",
		},
		{
			name:        "fail on unterminated interpolation",
			expr:        `"foo-${vars["env"]"`, // missing closing }
			errContains: "unterminated interpolation",
		},
		{
			name:        "fail on empty interpolation",
			expr:        `foo-${}`,
			errContains: "empty interpolation",
		},
		{
			name:        "fail with expression marker in wrapped expression",
			expr:        `${ ${spanAttributes["http.status_code"]} }`,
			errContains: "nested interpolation not allowed at pos",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := eval.EvalStringExpression(
				settings.OtelStringExpression{Expression: tt.expr},
				&ctx,
			)

			if tt.errContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestEvalBooleanExpression(t *testing.T) {
	eval, err := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	require.NoError(t, err)

	ctx := makeContext(true, true)

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
			name:     "boolean literal",
			expr:     "true",
			expected: true,
		},
		{
			name:     "boolean literal",
			expr:     `false`,
			expected: false,
		},
		{
			name:                "missing attribute key returns error",
			expr:                `resourceAttributes["not-existing-attr"]`,
			expectErrorContains: "no such key",
		},
		{
			name:                "type mismatch returns error",
			expr:                `spanAttributes["retries"]`, // int but expected boolean, cannot be recognized by type check because type of expression is Dyn
			expectErrorContains: "condition did not evaluate to boolean",
		},
		{
			name:                "unsupported type returns error",
			expr:                `spanAttributes`, // whole map, not a boolean
			expectErrorContains: "expected bool type, got: map(string, dyn), for expression 'spanAttributes'",
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
	eval, err := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	require.NoError(t, err)

	ctx := makeContext(true, true)

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
			expr:     &settings.OtelStringExpression{Expression: "static-string"},
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

func TestEvalMapExpression(t *testing.T) {
	ctx := makeContext(true, true)
	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))

	tests := []struct {
		name        string
		expr        settings.OtelAnyExpression
		want        map[string]any
		expectError string
	}{
		{
			name: "pure map reference spanAttributes",
			expr: settings.OtelAnyExpression{Expression: "${spanAttributes}"},
			want: map[string]any{
				"http.method":      "GET",
				"http.status_code": int64(200),
				"retries":          "5",
				"user.id":          "123",
				"pi":               3.14,
				"sampled":          true,
			},
		},
		{
			name: "pure map reference resourceAttributes",
			expr: settings.OtelAnyExpression{Expression: "${resourceAttributes}"},
			want: map[string]any{
				"cloud.provider": "aws",
				"service.name":   "cart-service",
				"env":            "dev",
				"deployment": map[string]any{
					"env":    "prod",
					"region": "eu-west-1",
				},
				"process.command_args": []interface{}{"java", "-jar", "app.jar"},
			},
		},
		{
			name: "pure map reference scopeAttributes",
			expr: settings.OtelAnyExpression{Expression: "${scopeAttributes}"},
			want: map[string]any{
				"otel.scope.name":    "io.opentelemetry.instrumentation.http",
				"otel.scope.version": "1.2.3",
			},
		},
		{
			name: "Map literal",
			expr: settings.OtelAnyExpression{Expression: "${{'key': 'value'}}"},
			want: map[string]any{
				"key": "value",
			},
		},
		{
			name:        "invalid: not a pure map reference",
			expr:        settings.OtelAnyExpression{Expression: "foo-${spanAttributes}"},
			want:        nil,
			expectError: `foo-${spanAttributes}" is not a valid map expression`,
		},
		{
			name:        "invalid: literal string",
			expr:        settings.OtelAnyExpression{Expression: `"just a string"`},
			want:        nil,
			expectError: `expression "\"just a string\"" is not a valid map expression`,
		},
		{
			name:        "invalid: empty interpolation",
			expr:        settings.OtelAnyExpression{Expression: "${}"},
			want:        nil,
			expectError: `empty interpolation at pos 0`,
		},
		{
			name:        "invalid: keys are not strings",
			expr:        settings.OtelAnyExpression{Expression: "${{true: 'value'}}"},
			want:        nil,
			expectError: "cannot convert key of type '*internal.CelEvaluationError' to string: true",
		},
		{
			name:        "unsupported type returns error",
			expr:        settings.OtelAnyExpression{Expression: `${'test'}`}, // a string type that can be statically checked
			expectError: `expected map type, got: string, for expression '${'test'}'`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := eval.EvalMapExpression(tt.expr, &ctx)

			if tt.expectError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestEvalStringExpression covers already much of the functionality, so here we focus on the differences.
func TestEvalAnyExpression(t *testing.T) {
	eval, err := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	require.NoError(t, err)

	ctx := makeContext(true, true)

	tests := []struct {
		name        string
		expr        string
		expected    any
		errContains string
	}{
		{
			name:     "simple string",
			expr:     "static string",
			expected: "static string",
		},
		{
			name:     "support array attribute",
			expr:     `${resourceAttributes["process.command_args"]}`,
			expected: []any{"java", "-jar", "app.jar"},
		},
		{
			name: "support map attribute",
			expr: `${resourceAttributes["deployment"]}`,
			expected: map[string]any{
				"env":    "prod",
				"region": "eu-west-1",
			},
		},
		{
			name:     "support int attributes",
			expr:     `${spanAttributes["http.status_code"]}`,
			expected: int64(200),
		},
		{
			name:     "support boolean attributes",
			expr:     `${spanAttributes["sampled"]}`,
			expected: true,
		},
		{
			name:     "support doubles",
			expr:     `${spanAttributes["pi"]}`,
			expected: 3.14,
		},
		{
			name:     "support int literal",
			expr:     "${42}",
			expected: int64(42),
		},
		{
			name:     "support double literal",
			expr:     "${3.14}",
			expected: 3.14,
		},
		{
			name:     "support bool literal",
			expr:     "${true}",
			expected: true,
		},
		{
			name:     "support slice literal",
			expr:     "${['foo', 'bar']}",
			expected: []ref.Val{types.String("foo"), types.String("bar")},
		},
		{
			name: "support map literal",
			expr: "${{'foo': 'bar'}}",
			expected: map[ref.Val]ref.Val{
				types.String("foo"): types.String("bar"),
			},
		},
		{
			name:        "missing attribute key returns error",
			expr:        `${resourceAttributes["not-existing-attr"]}`,
			errContains: "no such key",
		},
		{
			name:     "support string interpolation",
			expr:     `service-${resourceAttributes["env"]}`,
			expected: "service-dev",
		},
		{
			name:     "complex string interpolation",
			expr:     `ns-${resourceAttributes["service.name"]}-${vars.namespace}`,
			expected: "ns-cart-service-test",
		},
		{
			name:        "fail on unterminated interpolation",
			expr:        `"foo-${vars["env"]"`, // missing closing }
			errContains: "unterminated interpolation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := eval.EvalAnyExpression(
				settings.OtelAnyExpression{Expression: tt.expr},
				&ctx,
			)

			if tt.errContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestBoolEvalTypeMismatch(t *testing.T) {
	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	ctx := makeContext(false, false)

	// String expression but evaluated with boolean
	_, err := eval.EvalBooleanExpression(settings.OtelBooleanExpression{Expression: `resourceAttributes["cloud.provider"]`}, &ctx)
	require.Error(t, err)
}

func TestStringEvalTypeMismatch(t *testing.T) {
	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	ctx := makeContext(false, false)

	// Bool expression but evaluated with string
	_, err := eval.EvalStringExpression(settings.OtelStringExpression{Expression: `${resourceAttributes["cloud.provider"] == "aws"}`}, &ctx)
	require.Error(t, err)
}

func TestNoMetricAttributesError(t *testing.T) {
	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	ctx := makeContext(false, false)

	_, err := eval.EvalStringExpression(settings.OtelStringExpression{Expression: `${metricAttributes["http.method"]}`}, &ctx)
	assert.Equal(t, err.Error(), "no such attribute(s): metricAttributes")
}

func TestNoSpanAttributesError(t *testing.T) {
	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	ctx := makeContext(false, false)

	_, err := eval.EvalStringExpression(settings.OtelStringExpression{Expression: `${spanAttributes["http.method"]}`}, &ctx)
	assert.Equal(t, err.Error(), "no such attribute(s): spanAttributes")
}

func TestEvalCacheReuse(t *testing.T) {
	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	ctx := makeContext(true, false)

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

func TestEvalCacheEvictionBySize(t *testing.T) {
	// very small cache to force eviction
	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(2, 1*time.Minute))
	ctx := makeContext(true, true)

	exprs := []settings.OtelBooleanExpression{
		{Expression: `spanAttributes["retries"] == 1`},
		{Expression: `spanAttributes["retries"] == 2`},
		{Expression: `spanAttributes["retries"] == 3`}, // this should evict one of the earlier ones
	}

	for _, e := range exprs {
		_, err := eval.EvalBooleanExpression(e, &ctx)
		require.NoError(t, err)
	}

	require.Equal(t, 2, eval.cacheSize()) // capped by size
}

func TestEvalCacheExpiryByTTL(t *testing.T) {
	// short TTL so entries expire quickly
	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 200*time.Millisecond))
	ctx := makeContext(true, true)

	expr := settings.OtelBooleanExpression{Expression: `spanAttributes["http.method"] == "GET"`}

	_, err := eval.EvalBooleanExpression(expr, &ctx)
	require.NoError(t, err)
	require.Equal(t, 1, eval.cacheSize()) // compiled once

	// wait until the TTL expires
	time.Sleep(300 * time.Millisecond)

	_, err = eval.EvalBooleanExpression(expr, &ctx)
	require.NoError(t, err)
	require.Equal(t, 1, eval.cacheSize()) // still 1, but it was recompiled after expiry
}
