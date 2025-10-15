//nolint:testpackage
package internal

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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
		{"unterminated-double", `foo-${{vars["env"]}`, "unterminated"},

		// Valid: random braces in literal part
		{"literal-brace-before", `foo-{bar-${x}`, ""},
		{"literal-brace-after", `foo-${x}-bar}`, ""},
		{"literal-braces-around", `{foo-${x}-bar}`, ""},

		// Invalid: nested
		{"nested-simple", `foo-${${bar}}`, "nested"},
		{"nested-deeper", `${x+${y}}`, "nested"},

		// Valid: unmatched closing brace, simply part of literal
		{"unmatched-closing", `foo-}`, ""},
		{"unmatched-closing-after", `pre-${bar}}-post`, ""},

		// Valid: dollar not followed by '{' is just literal
		{"dollar-space", `$ {foo}`, ""},
		{"double-dollar-then-interp", `$${foo}`, ""},  // escaped interpolation: literal ${foo}
		{"double-dollar-then-interp", `$$${foo}`, ""}, // interpolation prefixed with literal $: $ + value-of(foo)
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
		wantKind  classification
		wantError string
	}{
		{"literal", "foo", kindStringLiteral, ""},
		{"wrapped literal", `"foo"`, kindStringLiteral, ""},
		{"wrapped identifier", "${vars[\"env\"]}", kindStringWithIdentifiers, ""},
		{"wrapped map reference", "${vars}", kindStringWithIdentifiers, ""},
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
		name   string
		expr   string
		result string
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
			result: `"service-"+(resourceAttributes["env"])`,
		},
		{
			name:   "unquoted with interpolation",
			expr:   `ns-${vars.ns}:svc-${resourceAttributes["service.name"]}`,
			result: `"ns-"+(vars.ns)+":svc-"+(resourceAttributes["service.name"])`,
		},
		{
			name:   "adjacent interpolations",
			expr:   `x-${a}${b}-y`,
			result: `"x-"+(a)+(b)+"-y"`,
		},
		{
			name:   "Curly brace after interpolation",
			expr:   `hello ${resourceAttributes["service.name"]}}`,
			result: `"hello "+(resourceAttributes["service.name"])+"}"`,
		},
		{
			name:   "support escaping ${ with $${ in literal part",
			expr:   `$${a}-${b}`,
			result: `"${a}-"+(b)`,
		},
		{
			name:   "support escaping $$$${ with $${ in literal part",
			expr:   `$$$${a}-${b}`,
			result: `"$${a}-"+(b)`,
		},
		{
			name:   "support escaping $${ with $$${",
			expr:   `$$${a}-${b}`,
			result: `"$"+(a)+"-"+(b)`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := rewriteInterpolations(tc.expr)
			require.Equal(t, tc.result, got)
		})
	}
}
