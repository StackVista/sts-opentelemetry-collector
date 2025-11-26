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

func makeContext(includeSpan bool, includeDatapoint bool) ExpressionEvalContext {
	spanAttributes := map[string]any{
		"http.method":      "GET",
		"http.status_code": int64(200),
		"user.id":          "123",
		"retries":          "5",
		"sampled":          true,
		"pi":               3.14,
	}

	datapointAttributes := map[string]any{
		"http.method":      "GET",
		"http.status_code": int64(200),
		"user.id":          "123",
	}

	scopeAttributes := map[string]any{
		"otel.scope.Name":    "io.opentelemetry.instrumentation.http",
		"otel.scope.Version": "1.2.3",
	}

	resourceAttributes := map[string]any{
		"service.Name":   "cart-service",
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

	evalCtx := ExpressionEvalContext{
		Scope:    NewScope("io.opentelemetry.instrumentation.http", "1.2.3", scopeAttributes),
		Resource: NewResource(resourceAttributes),
		Metric:   NewMetric("traces_service_graph_request_total", "description", "by"),
		Vars:     vars,
	}

	if includeSpan {
		evalCtx.Span = NewSpan("GET /checkout", spanAttributes)
	}

	if includeDatapoint {
		evalCtx.Datapoint = NewDatapoint(datapointAttributes)
	}

	return evalCtx
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
			name:     "support resource Attributes",
			expr:     `${resource.attributes["service.Name"]}`,
			expected: "cart-service",
		},
		{
			name:     "support Attributes access (with single quotes)",
			expr:     "${resource.attributes['service.Name']}",
			expected: "cart-service",
		},
		{
			name:     "support span Attributes",
			expr:     `${span.attributes["http.method"]}`,
			expected: "GET",
		},
		{
			name:     "support metric fields",
			expr:     `${metric.name}`,
			expected: "traces_service_graph_request_total",
		},
		{
			name:     "support scope Attributes",
			expr:     `${scope.attributes["otel.scope.Name"]}`,
			expected: "io.opentelemetry.instrumentation.http",
		},
		{
			name:     "support custom vars",
			expr:     `${vars.namespace}`,
			expected: "test",
		},
		{
			name:        "missing attribute key returns error",
			expr:        `${resource.attributes["not-existing-attr"]}`,
			errContains: "no such key",
		},
		{
			name:     "coerce int to string",
			expr:     `${span.attributes["retries"]}`,
			expected: "5",
		},
		{
			name:        "not coerce boolean to string",
			expr:        `${true}`,
			errContains: "cannot convert 'bool' to 'string'",
		},
		{
			name:        "string type validation fails on span object",
			expr:        `${span}`, // object, not string
			errContains: "expected string type, got: map(string, dyn), for expression '${span}'",
		},
		{
			name:        "string type validation fails on span attributes map",
			expr:        `${span.attributes}`, // whole map, not string
			errContains: "expected string type, got: map(string, dyn), for expression '${span.attributes}'",
		},
		{
			name:     "support string interpolation with vars",
			expr:     `service-${resource.attributes["env"]}`,
			expected: "service-dev",
		},
		{
			name:     "support string interpolation with resource Attributes",
			expr:     `ns-${resource.attributes["service.Name"]}`,
			expected: "ns-cart-service",
		},
		{
			name:     "multiple interpolations at once",
			expr:     `ns-${resource.attributes["service.Name"]}-${vars.namespace}-${resource.attributes["service.Name"].matches(R'cart-.*') ? 'cart' : 'not-cart'}-end`,
			expected: "ns-cart-service-test-cart-end",
		},
		{
			name:     "interpolation at start and end of expression",
			expr:     `${vars.namespace}/${resource.attributes["service.Name"]}`,
			expected: "test/cart-service",
		},
		{
			name:     "another complex expression",
			expr:     `${resource.attributes["service.Name"].matches(R'cart-.*') ? ( span.attributes["http.status_code"] < 400 ? 'good-cart' : 'bad-cart' ) : 'not-cart'}`,
			expected: "good-cart",
		},
		{
			name:     "support key existence checks",
			expr:     `${'service.Name' in resource.attributes ? 'yes' : 'no'}`,
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
			expr:        `${ ${span.attributes["http.status_code"]} }`,
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
			expr:     `resource.attributes["cloud.provider"] == "aws"`,
			expected: true,
		},
		{
			name:     "simple false condition",
			expr:     `resource.attributes["cloud.provider"] == "gcp"`,
			expected: false,
		},
		{
			name:     "complex boolean expression with AND",
			expr:     `resource.attributes["cloud.provider"] == "aws" && span.attributes["http.method"] == "GET"`,
			expected: true,
		},
		{
			name:     "complex boolean expression with OR",
			expr:     `resource.attributes["cloud.provider"] == "gcp" || span.attributes["http.method"] == "GET"`,
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
			expr:                `resource.attributes["not-existing-attr"]`,
			expectErrorContains: "no such key",
		},
		{
			name:                "type mismatch returns error",
			expr:                `span.attributes["retries"]`, // int but expected boolean, cannot be recognized by type check because type of expression is Dyn
			expectErrorContains: "condition did not evaluate to boolean",
		},
		{
			name:                "bool type validation fails on span object",
			expr:                `span`, // object, not string
			expectErrorContains: "expected bool type, got: map(string, dyn), for expression 'span'",
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
			name: "pure map reference span.attributes",
			expr: settings.OtelAnyExpression{Expression: "${span.attributes}"},
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
			name: "pure map reference resource.attributes",
			expr: settings.OtelAnyExpression{Expression: "${resource.attributes}"},
			want: map[string]any{
				"cloud.provider": "aws",
				"service.Name":   "cart-service",
				"env":            "dev",
				"deployment": map[string]any{
					"env":    "prod",
					"region": "eu-west-1",
				},
				"process.command_args": []interface{}{"java", "-jar", "app.jar"},
			},
		},
		{
			name: "pure map reference scope.attributes",
			expr: settings.OtelAnyExpression{Expression: "${scope.attributes}"},
			want: map[string]any{
				"otel.scope.Name":    "io.opentelemetry.instrumentation.http",
				"otel.scope.Version": "1.2.3",
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
			expr:        settings.OtelAnyExpression{Expression: "foo-${span.attributes}"},
			want:        nil,
			expectError: `foo-${span.attributes}" is not a valid map expression`,
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
			expr:     `${resource.attributes["process.command_args"]}`,
			expected: []any{"java", "-jar", "app.jar"},
		},
		{
			name: "support map attribute",
			expr: `${resource.attributes["deployment"]}`,
			expected: map[string]any{
				"env":    "prod",
				"region": "eu-west-1",
			},
		},
		{
			name:     "support int Attributes",
			expr:     `${span.attributes["http.status_code"]}`,
			expected: int64(200),
		},
		{
			name:     "support boolean Attributes",
			expr:     `${span.attributes["sampled"]}`,
			expected: true,
		},
		{
			name:     "support doubles",
			expr:     `${span.attributes["pi"]}`,
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
			expr:        `${resource.attributes["not-existing-attr"]}`,
			errContains: "no such key",
		},
		{
			name:     "support string interpolation",
			expr:     `service-${resource.attributes["env"]}`,
			expected: "service-dev",
		},
		{
			name:     "complex string interpolation",
			expr:     `ns-${resource.attributes["service.Name"]}-${vars.namespace}`,
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
	_, err := eval.EvalBooleanExpression(settings.OtelBooleanExpression{Expression: `resource.attributes["cloud.provider"]`}, &ctx)
	require.Error(t, err)
}

func TestStringEvalTypeMismatch(t *testing.T) {
	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	ctx := makeContext(false, false)

	// Bool expression but evaluated with string
	_, err := eval.EvalStringExpression(settings.OtelStringExpression{Expression: `${resource.attributes["cloud.provider"] == "aws"}`}, &ctx)
	require.Error(t, err)
}

func TestNoDatapointError(t *testing.T) {
	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	ctx := makeContext(false, false)

	_, err := eval.EvalStringExpression(settings.OtelStringExpression{Expression: `${datapoint.attributes["http.method"]}`}, &ctx)
	assert.Equal(t, err.Error(), "no such attribute(s): datapoint")
}

func TestNoSpanError(t *testing.T) {
	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	ctx := makeContext(false, false)

	_, err := eval.EvalStringExpression(settings.OtelStringExpression{Expression: `${span.attributes["http.method"]}`}, &ctx)
	assert.Equal(t, err.Error(), "no such attribute(s): span")
}

func TestEvalCacheReuse(t *testing.T) {
	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	ctx := makeContext(true, false)

	expr := settings.OtelBooleanExpression{Expression: `span.attributes["retries"] == 2`}

	_, err := eval.EvalBooleanExpression(expr, &ctx)
	require.NoError(t, err)
	require.Equal(t, 1, eval.cacheSize()) // compiled once

	_, err = eval.EvalBooleanExpression(expr, &ctx)
	require.NoError(t, err)
	require.Equal(t, 1, eval.cacheSize()) // still one entry

	// different expression should increase cache size
	other := settings.OtelBooleanExpression{Expression: `span.attributes["retries"] == 10`}
	_, err = eval.EvalBooleanExpression(other, &ctx)
	require.NoError(t, err)
	require.Equal(t, 2, eval.cacheSize())
}

func TestEvalCacheEvictionBySize(t *testing.T) {
	// very small cache to force eviction
	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(2, 1*time.Minute))
	ctx := makeContext(true, true)

	exprs := []settings.OtelBooleanExpression{
		{Expression: `span.attributes["retries"] == 1`},
		{Expression: `span.attributes["retries"] == 2`},
		{Expression: `span.attributes["retries"] == 3`}, // this should evict one of the earlier ones
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

	expr := settings.OtelBooleanExpression{Expression: `span.attributes["http.method"] == "GET"`}

	_, err := eval.EvalBooleanExpression(expr, &ctx)
	require.NoError(t, err)
	require.Equal(t, 1, eval.cacheSize()) // compiled once

	// wait until the TTL expires
	time.Sleep(300 * time.Millisecond)

	_, err = eval.EvalBooleanExpression(expr, &ctx)
	require.NoError(t, err)
	require.Equal(t, 1, eval.cacheSize()) // still 1, but it was recompiled after expiry
}
