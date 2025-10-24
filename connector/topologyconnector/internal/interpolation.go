package internal

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// classification determines how an expression should be interpreted.
//
// Motivation:
// In our domain model, expressions can be one of:
//   - A plain string literal (no CEL evaluation): kindStringLiteral
//   - A full CEL expression returning a String, or Map, wrapped expr ${...}:
//     kindStringWithIdentifiers
//   - A string with embedded interpolations ${...}, requiring rewrite to CEL concat: kindStringInterpolation
//
// CEL itself cannot distinguish between a bare string literal and an identifier,
// so we must classify expressions before evaluation. This allows us to:
//   - Avoid sending plain literals to CEL (which would error as unknown identifiers).
//     Example error: CEL compilation error: ERROR: <input>:1:1: undeclared reference to 'staticstring' (in container ”)
//   - Correctly unwrap full CEL expressions before eval
//   - Rewrite interpolated strings into valid CEL concat expressions ("urn:.." + vars.attribute)
//
// This mirrors the backend conventions and keeps frontend (Go collector) behavior aligned.
type classification int

const (
	kindInvalid               classification = iota
	kindStringLiteral                        // the entire expr is a literal string, no CEL evaluation needed
	kindStringWithIdentifiers                // the entire expr is a single CEL expression wrapped in ${...}
	kindStringInterpolation                  // the expr is a string with one or more ${...} interpolations

	// literals used for validating interpolation syntax and balanced expressions
	interpolationFrame = "interpolation"
	braceFrame         = "brace"
)

var (
	// capture group 1: one or more $, group 2: inner expression
	interpolationExprCapturePattern = regexp.MustCompile(`(\$+)\{([^}]*)\}`)
	escapeDollarDollarCurlyPattern  = regexp.MustCompile(`\$\$`) // to replace all $$ to $
)

// classifyExpression determines the kind of expression so the evaluator
// can choose the right handling path. It distinguishes between:
//   - ${expr} - a wrapped CEL expression (kindStringWithIdentifiers)
//   - "...${...}..." - a string with interpolations (kindStringInterpolation)
//   - plain strings without ${} - treated as literals (kindStringLiteral)
//
// Validation is always applied if "${" is detected, and invalid cases are
// classified as kindInvalid with an error.
func classifyExpression(expr string) (classification, error) {
	switch {
	case withoutInterpolation(expr):
		return kindStringLiteral, nil
	case strings.HasPrefix(expr, "${") && strings.Count(expr, "${") <= 1 && strings.HasSuffix(expr, "}"):
		if err := validateInterpolation(expr); err != nil {
			return kindInvalid, err
		}
		return kindStringWithIdentifiers, nil

	case strings.Contains(expr, "${"):
		if err := validateInterpolation(expr); err != nil {
			return kindInvalid, err
		}
		return kindStringInterpolation, nil

	default:
		if err := validateInterpolation(expr); err != nil {
			return kindInvalid, err
		}
		// if validation somehow passes (shouldn’t), treat as interpolation
		return kindStringInterpolation, nil
	}
}

func withoutInterpolation(expr string) bool {
	return !strings.Contains(expr, "${")
}

// validateInterpolation ensures that every ${...} block is well-formed:
// - each ${ has a matching closing }
// - interpolation blocks are not nested (${${...}})
// - interpolation content is not empty or whitespace-only
// - braces inside interpolation (e.g. map literals) are balanced
// - quoted strings inside interpolation are respected (so braces in quotes are ignored)
func validateInterpolation(origExpr string) error {
	// Always scan if the string might contain interpolation or braces
	if !strings.Contains(origExpr, "${") && !strings.ContainsAny(origExpr, "{}") {
		// literal string, no interpolation or braces to validate
		return nil
	}

	expr := origExpr

	// If the entire expression is wrapped in a single pair of quotes,
	// strip them for validation. This allows `"svc-${identifierMap["attributeKey"]}"` to be validated
	// correctly as containing an interpolation.
	isWrappedIn := func(quoteChar byte) bool {
		return expr[0] == quoteChar && expr[len(expr)-1] == quoteChar
	}
	if len(expr) >= 2 && (isWrappedIn('"') || isWrappedIn('\'')) {
		expr = expr[1 : len(expr)-1]
	}

	type frame struct {
		kind       string // interpolationFrame ${...} or braceFrame {...}
		start      int    // index where the frame started
		quote      byte   // current quote char inside this frame, 0 if none
		innerDepth int    // counts nested { } within an interpolation
	}

	var stack []frame
	i := 0
	for i < len(expr) {
		// escaped interpolation
		if i+2 < len(expr) && expr[i] == '$' && expr[i+1] == '$' && expr[i+2] == '{' {
			i += 3
			continue
		}
		// Detect start of interpolation
		if i+1 < len(expr) && expr[i] == '$' && expr[i+1] == '{' {
			// If the top frame is an interpolation, this is nested, which is invalid.
			if len(stack) > 0 && stack[len(stack)-1].kind == interpolationFrame {
				return fmt.Errorf("nested interpolation not allowed at pos %d", i)
			}
			stack = append(stack, frame{kind: interpolationFrame, start: i})
			i += 2
			continue
		}

		char := expr[i]

		// If we're inside any frame...
		if len(stack) > 0 {
			top := &stack[len(stack)-1]

			// If inside a quoted string inside this frame, skip until closing quote.
			if top.quote != 0 {
				if char == '\\' && i+1 < len(expr) {
					// skip escaped char
					i += 2
					continue
				}
				if char == top.quote {
					top.quote = 0 // closed quote
				}
				i++
				continue
			}

			// Start a quoted string inside the current frame
			if char == '"' || char == '\'' {
				top.quote = char
				i++
				continue
			}

			// Within an interpolation, track inner { } pairs as depth
			if top.kind == interpolationFrame {
				if char == '{' {
					top.innerDepth++
					i++
					continue
				}
				if char == '}' {
					if top.innerDepth > 0 {
						top.innerDepth--
						i++
						continue
					}
					// This closes the interpolation
					inner := strings.TrimSpace(expr[top.start+2 : i])
					if inner == "" {
						return fmt.Errorf("empty interpolation at pos %d", top.start)
					}
					// valid interpolation syntax, pop from frame
					stack = stack[:len(stack)-1]
					i++
					continue
				}
				// other chars inside interpolation
				i++
				continue
			}

			// If top frame is a plain brace frame:
			if top.kind == braceFrame {
				if char == '{' {
					// nested plain brace
					stack = append(stack, frame{kind: braceFrame, start: i})
					i++
					continue
				}
				if char == '}' {
					// close the top plain brace
					stack = stack[:len(stack)-1]
					i++
					continue
				}
				i++
				continue
			}
		}

		// otherwise just advance
		i++
	}

	// If anything remains on the stack it’s an unclosed frame
	if len(stack) > 0 {
		top := stack[len(stack)-1]
		if top.kind == interpolationFrame {
			return fmt.Errorf("unterminated interpolation starting at pos %d", top.start)
		}
		return fmt.Errorf("unmatched '{' at pos %d", top.start)
	}

	return nil
}

// unwrap `${...}` - extract the inner CEL expression
func unwrapExpression(expression string) string {
	return expression[2 : len(expression)-1]
}

// rewriteInterpolations converts a string with ${...} interpolations into a CEL-compatible
// concatenation expression, while validating and preserving literal segments.
func rewriteInterpolations(expr string) string {
	// Validation of the interpolation syntax is expected to have been done
	// already before calling this function.
	// Fast path: no interpolation at all
	if !strings.Contains(expr, "${") {
		return expr
	}

	// Strip outer quotes if present
	if len(expr) >= 2 && ((expr[0] == '"' && expr[len(expr)-1] == '"') ||
		(expr[0] == '\'' && expr[len(expr)-1] == '\'')) {
		expr = expr[1 : len(expr)-1]
	}

	//nolint:prealloc
	var parts []string
	lastIndex := 0
	matches := interpolationExprCapturePattern.FindAllStringSubmatchIndex(expr, -1)

	for _, match := range matches {
		fullMatchStart, fullMatchEnd := match[0], match[1]
		dollarGroupStart, dollarGroupEnd := match[2], match[3]
		innerExprStart, innerExprEnd := match[4], match[5]

		dollarSigns := expr[dollarGroupStart:dollarGroupEnd]
		numDollars := len(dollarSigns)

		// Even number of '$' means all are escaped pairs, so this is not a real interpolation.
		// e.g., $${...} or $$$${...}
		if numDollars%2 == 0 {
			continue
		}

		// An odd number of '$' means there's one real interpolation preceded by escaped pairs.
		// e.g., ${...} (1), $$${...} (3), etc.
		interpolationStart := fullMatchStart + numDollars - 1

		// literal before the interpolation
		if interpolationStart > lastIndex {
			lit := expr[lastIndex:interpolationStart]
			parts = append(parts, strconv.Quote(unescapeDollars(lit)))
		}

		// The interpolation part
		inner := strings.TrimSpace(expr[innerExprStart:innerExprEnd])
		parts = append(parts, "("+inner+")")

		lastIndex = fullMatchEnd
	}

	// trailing literal
	if lastIndex < len(expr) {
		lit := expr[lastIndex:]
		parts = append(parts, strconv.Quote(unescapeDollars(lit)))
	}

	return strings.Join(parts, "+")
}

func unescapeDollars(literal string) string {
	return escapeDollarDollarCurlyPattern.ReplaceAllLiteralString(literal, "$")
}
