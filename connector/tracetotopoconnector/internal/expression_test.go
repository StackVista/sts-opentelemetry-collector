//nolint:testpackage
package internal

import (
	"strings"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
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
	eval, err := NewCELEvaluator(CacheSettings{Size: 100, TTL: 30 * time.Second})
	require.NoError(t, err)

	ctx := makeContext()

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
			errContains: "expression did not evaluate to string",
		},
		{
			name:        "unsupported type returns error",
			expr:        `${spanAttributes}`, // whole map, not string
			errContains: "expression did not evaluate to string, got: map",
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
			name:        "fail on unterminated interpolation",
			expr:        `"foo-${vars["env"]"`, // missing closing }
			errContains: "unterminated interpolation",
		},
		{
			name:        "fail on empty interpolation",
			expr:        `foo-${}`,
			errContains: "empty interpolation",
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
	eval, err := NewCELEvaluator(CacheSettings{Size: 100, TTL: 30 * time.Second})
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
	eval, err := NewCELEvaluator(CacheSettings{Size: 100, TTL: 30 * time.Second})
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

func TestCelEvaluator_EvalMapExpression(t *testing.T) {
	ctx := makeContext()
	eval, _ := NewCELEvaluator(CacheSettings{Size: 100, TTL: 30 * time.Minute})

	tests := []struct {
		name        string
		expr        settings.OtelStringExpression
		want        map[string]any
		expectError bool
	}{
		{
			name: "pure map reference spanAttributes",
			expr: settings.OtelStringExpression{Expression: "${spanAttributes}"},
			want: map[string]any{
				"http.method":      "GET",
				"http.status_code": int64(200),
				"retries":          "5",
				"user.id":          "123",
			},
			expectError: false,
		},
		{
			name: "pure map reference resourceAttributes",
			expr: settings.OtelStringExpression{Expression: "${resourceAttributes}"},
			want: map[string]any{
				"cloud.provider": "aws",
				"service.name":   "cart-service",
				"env":            "dev",
			},
			expectError: false,
		},
		{
			name: "pure map reference scopeAttributes",
			expr: settings.OtelStringExpression{Expression: "${scopeAttributes}"},
			want: map[string]any{
				"otel.scope.name":    "io.opentelemetry.instrumentation.http",
				"otel.scope.version": "1.2.3",
			},
			expectError: false,
		},
		{
			name:        "invalid: not a pure map reference",
			expr:        settings.OtelStringExpression{Expression: "foo-${spanAttributes}"},
			want:        nil,
			expectError: true,
		},
		{
			name:        "invalid: literal string",
			expr:        settings.OtelStringExpression{Expression: `"just a string"`},
			want:        nil,
			expectError: true,
		},
		{
			name:        "invalid: empty interpolation",
			expr:        settings.OtelStringExpression{Expression: "${}"},
			want:        nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := eval.EvalMapExpression(tt.expr, &ctx)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestBoolEvalTypeMismatch(t *testing.T) {
	eval, _ := NewCELEvaluator(CacheSettings{Size: 100, TTL: 30 * time.Second})
	ctx := makeContext()

	// String expression but evaluated with boolean
	_, err := eval.EvalBooleanExpression(settings.OtelBooleanExpression{Expression: `resourceAttributes["cloud.provider"]`}, &ctx)
	require.Error(t, err)
}

func TestStringEvalTypeMismatch(t *testing.T) {
	eval, _ := NewCELEvaluator(CacheSettings{Size: 100, TTL: 30 * time.Second})
	ctx := makeContext()

	// Bool expression but evaluated with string
	_, err := eval.EvalStringExpression(settings.OtelStringExpression{Expression: `${resourceAttributes["cloud.provider"] == "aws"}`}, &ctx)
	require.Error(t, err)
}

func TestEvalCacheReuse(t *testing.T) {
	eval, _ := NewCELEvaluator(CacheSettings{Size: 100, TTL: 30 * time.Second})
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

func TestEvalCacheEvictionBySize(t *testing.T) {
	// very small cache to force eviction
	eval, _ := NewCELEvaluator(CacheSettings{Size: 2, TTL: 1 * time.Minute})
	ctx := makeContext()

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
	eval, _ := NewCELEvaluator(CacheSettings{Size: 10, TTL: 200 * time.Millisecond})
	ctx := makeContext()

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

func TestValidateInterpolation(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr string // substring; empty means valid
	}{
		// Valid: no interpolation at all
		{"literal-no-markers", "hello-world", ""},

		// Valid: single wrapped expression
		{"wrapped-simple", "${foo}", ""},
		{"wrapped-indexing-double-quotes", `${resourceAttributes["service.name"]}`, ""},
		{"wrapped-indexing-single-quotes", `${resourceAttributes['service.name']}`, ""},
		{"wrapped-spaces-inside", `${   vars["env"]   }`, ""},

		// Valid: interpolated with surrounding literals
		{"interp-prefix", `pre-${foo}`, ""},
		{"interp-suffix", `${foo}-post`, ""},
		{"interp-middle", `a-${foo}-b`, ""},
		{"interp-slashes", `a/${foo}/b`, ""},
		{"interp-multiple", `a-${x}-m-${y}-z`, ""},

		// Valid: quoted outer string containing interpolation
		{"quoted-double", `"svc-${vars["env"]}"`, ""},
		{"quoted-single", `'svc-${vars["env"]}'`, ""},

		// Invalid: empty
		{"empty", `foo-${}`, "empty interpolation"},

		// Invalid: unterminated
		{"unterminated-simple", `foo-${bar`, "unterminated"},
		{"unterminated-quoted", `"foo-${vars["env"]"`, "unterminated"},

		// Invalid: nested
		{"nested-simple", `foo-${${bar}}`, "nested"},
		{"nested-deeper", `${x + ${y}}`, "nested"},

		// Invalid: unmatched closing brace
		{"unmatched-closing", `foo-}`, "unmatched"},
		{"unmatched-closing-after", `pre-${bar}}-post`, "unmatched"},

		// Valid: dollar not followed by '{' is just literal
		{"dollar-space", `$ {foo}`, ""},
		{"double-dollar-then-interp", `$${foo}`, ""}, // literal '$' + ${foo}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateInterpolation(tt.in)
			if tt.wantErr == "" {
				require.NoError(t, err, "expected valid but got error: %v", err)
			} else {
				require.Error(t, err, "expected error containing %q", tt.wantErr)
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestClassifyStringExpression(t *testing.T) {
	tests := []struct {
		name      string
		expr      string
		wantKind  expressionKind
		wantError string
	}{
		{"literal", "foo", kindStringLiteral, ""},
		{"wrapped literal", `"foo"`, kindStringLiteral, ""},
		{"wrapped identifier", "${vars[\"env\"]}", kindStringWithIdentifiers, ""},
		{"wrapped map reference", "${vars}", kindMapReferenceOnly, ""},
		{"interpolated ok", "foo-${vars[\"env\"]}-bar", kindStringInterpolation, ""},
		{"unterminated interpolation", `"foo-${vars[\"env\"]"`, kindInvalid, "unterminated"},
		{"empty interpolation", `foo-${}`, kindInvalid, "empty"},
		{"nested interpolation", `foo-${${bar}}`, kindInvalid, "nested"},
		{"dollar-space", `$ {foo}`, kindStringLiteral, ""}, // not interpolation
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, err := classifyExpression(tt.expr)

			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Errorf("expected error containing %q, got %v", tt.wantError, err)
				}
				if kind != kindInvalid {
					t.Errorf("expected kindInvalid, got %v", kind)
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if kind != tt.wantKind {
				t.Errorf("expected %v, got %v", tt.wantKind, kind)
			}
		})
	}
}

func TestRewriteInterpolations(t *testing.T) {
	cases := []struct {
		name      string
		expr      string
		result    string
		errSubstr string
	}{
		{
			name:   "no interpolation (literal remains unchanged)",
			expr:   "just-a-literal",
			result: "just-a-literal",
		},
		{
			name:   "no interpolation (expression remains unchanged)",
			expr:   `resourceAttributes["service.name"]`,
			result: `resourceAttributes["service.name"]`,
		},
		{
			name:   "quoted with interpolation",
			expr:   `"service-${resourceAttributes["env"]}"`,
			result: `"service-" + (resourceAttributes["env"])`,
		},
		{
			name:   "unquoted with interpolation",
			expr:   `ns-${vars.ns}:svc-${resourceAttributes["service.name"]}`,
			result: `"ns-" + (vars.ns) + ":svc-" + (resourceAttributes["service.name"])`,
		},
		{
			name:   "adjacent interpolations",
			expr:   `x-${a}${b}-y`,
			result: `"x-" + (a) + (b) + "-y"`,
		},
		{
			name:      "empty interpolation",
			expr:      `foo-${}`,
			errSubstr: "empty interpolation",
		},
		{
			name:      "whitespace-only interpolation",
			expr:      `foo-${   }`,
			errSubstr: "empty interpolation",
		},
		{
			name:      "unterminated",
			expr:      `foo-${bar`,
			errSubstr: "unterminated interpolation",
		},
		{
			name:      "nested interpolation not allowed",
			expr:      `foo-${${bar}}`,
			errSubstr: "nested interpolation",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rewriteInterpolations(tc.expr)
			if tc.errSubstr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errSubstr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.result, got)
		})
	}
}
