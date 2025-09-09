package internal

import (
	"fmt"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"regexp"
	"strconv"
	"strings"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// expressionKind classifies how an expression should be interpreted.
//
// Motivation:
// In our domain model, expressions can be one of:
//   - A plain string literal (no CEL evaluation): kindStringLiteral
//   - A full CEL expression returning a String, or Map, wrapped expr ${...}:
//     kindStringWithIdentifiers, kindMapReferenceOnly
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
type expressionKind int

const (
	kindInvalid expressionKind = iota
	kindStringLiteral
	kindStringWithIdentifiers
	kindStringInterpolation
	kindBoolean
	kindMapReferenceOnly // e.g. "${spanAttributes}" or "${vars}"
)

var (
	wrappedExprClassificationPattern = regexp.MustCompile(`^\$\{[^}]+\}$`) // classification
	interpolationExprCapturePattern  = regexp.MustCompile(`\$\{([^}]*)\}`) // capture group for rewrite
)

type ExpressionEvaluator interface {
	EvalStringExpression(expr settings.OtelStringExpression, evalCtx *ExpressionEvalContext) (string, error)
	EvalOptionalStringExpression(expr *settings.OtelStringExpression, evalCtx *ExpressionEvalContext) (*string, error)
	EvalBooleanExpression(expr settings.OtelBooleanExpression, evalCtx *ExpressionEvalContext) (bool, error)
	EvalMapExpression(expr settings.OtelStringExpression, evalCtx *ExpressionEvalContext) (map[string]any, error)
}

type ExpressionEvalContext struct {
	Span     ptrace.Span
	Scope    ptrace.ScopeSpans
	Resource ptrace.ResourceSpans
	Vars     map[string]string
}

type CelEvaluator struct {
	env   *cel.Env
	cache *expirable.LRU[string, cel.Program]
}

func NewCELEvaluator(cacheSettings tracetotopoconnector.ExpressionCacheSettings) (*CelEvaluator, error) {
	env, err := cel.NewEnv(
		cel.Variable("spanAttributes", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("scopeAttributes", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("resourceAttributes", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("vars", cel.MapType(cel.StringType, cel.DynType)),
		ext.Strings(), // enables string manipulation functions
		// ext.Math(ext.MathOption(...)) // TODO: enable math operators
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}
	cache := expirable.NewLRU[string, cel.Program](cacheSettings.Size, nil, cacheSettings.TTL)
	return &CelEvaluator{env, cache}, nil
}

// ---------------------------------------------------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------------------------------------------------

func (e *CelEvaluator) EvalStringExpression(
	expr settings.OtelStringExpression,
	evalCtx *ExpressionEvalContext,
) (string, error) {
	kind, err := classifyExpression(expr.Expression)
	if err != nil {
		return "", err
	}

	if kind == kindStringLiteral {
		return expr.Expression, nil
	}

	evalOrCached, err := e.evalOrCached(expr.Expression, kind, evalCtx)
	if err != nil {
		return "", err
	}
	return stringify(evalOrCached)
}

func (e *CelEvaluator) EvalOptionalStringExpression(
	expr *settings.OtelStringExpression,
	evalCtx *ExpressionEvalContext,
) (*string, error) {
	if expr == nil {
		//nolint:nilnil
		return nil, nil
	}

	result, err := e.EvalStringExpression(*expr, evalCtx)
	return &result, err
}

func (e *CelEvaluator) EvalBooleanExpression(
	expr settings.OtelBooleanExpression,
	evalCtx *ExpressionEvalContext,
) (bool, error) {
	result, err := e.evalOrCached(expr.Expression, kindBoolean, evalCtx)
	if err != nil {
		return false, err
	}

	if boolResult, ok := result.(bool); ok {
		return boolResult, nil
	}

	return false, fmt.Errorf("condition did not evaluate to boolean, got: %T", result)
}

func (e *CelEvaluator) EvalMapExpression(
	expr settings.OtelStringExpression,
	evalCtx *ExpressionEvalContext,
) (map[string]any, error) {
	kind, err := classifyExpression(expr.Expression)
	if err != nil {
		return nil, err
	}
	if kind != kindMapReferenceOnly {
		return nil, fmt.Errorf("expression %q is not a pure map reference", expr.Expression)
	}

	val, err := e.evalOrCached(expr.Expression, kind, evalCtx)
	if err != nil {
		return nil, err
	}

	m, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map[string]any, got %T", val)
	}
	return m, nil
}

// ---------------------------------------------------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------------------------------------------------

// classifyExpression determines the kind of expression so the evaluator
// can choose the right handling path. It distinguishes between:
//   - ${expr} - a wrapped CEL expression (kindStringWithIdentifiers, kindMapReferenceOnly)
//   - "...${...}..." - a string with interpolations (kindStringInterpolation)
//   - plain strings without ${} - treated as literals (kindStringLiteral)
//
// Validation is always applied if "${" is detected, and invalid cases are
// classified as kindInvalid with an error.
func classifyExpression(expr string) (expressionKind, error) {
	switch {
	case wrappedExprClassificationPattern.MatchString(expr):
		if err := validateInterpolation(expr); err != nil {
			return kindInvalid, err
		}

		if isPureMapReference(expr) {
			return kindMapReferenceOnly, nil
		}
		return kindStringWithIdentifiers, nil

	case strings.Contains(expr, "${"):
		if err := validateInterpolation(expr); err != nil {
			return kindInvalid, err
		}
		return kindStringInterpolation, nil

	default:
		// Fallback: contains "${" but didn’t match strict patterns
		if strings.Contains(expr, "${") {
			if err := validateInterpolation(expr); err != nil {
				return kindInvalid, err
			}
			// if validation somehow passes (shouldn’t), treat as interpolation
			return kindStringInterpolation, nil
		}

		return kindStringLiteral, nil
	}
}

func isPureMapReference(expr string) bool {
	inner := strings.TrimSpace(expr[2 : len(expr)-1]) // strip ${...}
	switch inner {
	case "spanAttributes", "scopeAttributes", "resourceAttributes", "vars":
		return true
	default:
		return false
	}
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
		kind       string // "interp" ${...} or "brace" {...}
		start      int    // index where the frame started
		quote      byte   // current quote char inside this frame, 0 if none
		innerDepth int    // counts nested { } within an interpolation
	}

	var stack []frame
	i := 0
	for i < len(expr) {
		// Detect start of interpolation
		if i+1 < len(expr) && expr[i] == '$' && expr[i+1] == '{' {
			// If the top frame is an interpolation, this is nested, which is invalid.
			if len(stack) > 0 && stack[len(stack)-1].kind == "interp" {
				return fmt.Errorf("nested interpolation not allowed at pos %d", i)
			}
			stack = append(stack, frame{kind: "interp", start: i})
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
			if top.kind == "interp" {
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
			if top.kind == "brace" {
				if char == '{' {
					// nested plain brace
					stack = append(stack, frame{kind: "brace", start: i})
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

		// Not inside any frame:
		// - plain '{' opens a brace frame
		// - an unmatched '}' is an error
		if char == '{' {
			stack = append(stack, frame{kind: "brace", start: i})
			i++
			continue
		}
		if char == '}' {
			return fmt.Errorf("unmatched '}' at pos %d", i)
		}

		// otherwise just advance
		i++
	}

	// If anything remains on the stack it’s an unclosed frame
	if len(stack) > 0 {
		top := stack[len(stack)-1]
		if top.kind == "interp" {
			return fmt.Errorf("unterminated interpolation starting at pos %d", top.start)
		}
		return fmt.Errorf("unmatched '{' at pos %d", top.start)
	}

	return nil
}

// evalOrCached compiles (or reuses a cached) CEL program for the given expression
// and evaluates it against the current runtime variables (span, scope, resource, vars).
func (e *CelEvaluator) evalOrCached(expression string, expressionKind expressionKind, ctx *ExpressionEvalContext) (interface{}, error) {
	prog, err := e.getOrCompile(expression, expressionKind)
	if err != nil {
		return "", err
	}

	runtimeVars := map[string]interface{}{
		"spanAttributes":     flattenAttributes(ctx.Span.Attributes()),
		"scopeAttributes":    flattenAttributes(ctx.Scope.Scope().Attributes()),
		"resourceAttributes": flattenAttributes(ctx.Resource.Resource().Attributes()),
		"vars":               ctx.Vars,
	}

	result, _, err := prog.Eval(runtimeVars) // there's also a prog.ContextEval - a means to timeout evaluations
	if err != nil {
		return "", fmt.Errorf("CEL evaluation error: %w", err)
	}

	return result.Value(), nil
}

// getOrCompile returns a cached CEL program for the expression if available,
// otherwise preprocesses it (based on kind), compiles it, caches it, and returns it.
func (e *CelEvaluator) getOrCompile(original string, kind expressionKind) (cel.Program, error) {
	// check cache by original expression
	if prog, ok := e.cache.Get(original); ok {
		return prog, nil
	}

	// preprocess based on kind
	toCompile, err := preprocessExpression(original, kind)
	if err != nil {
		return nil, err
	}

	ast, iss := e.env.Compile(toCompile)
	if iss.Err() != nil {
		return nil, iss.Err()
	}
	prog, err := e.env.Program(ast)
	if err != nil {
		return nil, err
	}

	e.cache.Add(original, prog) // store under original
	return prog, nil
}

func preprocessExpression(expr string, kind expressionKind) (string, error) {
	//nolint:exhaustive
	switch kind {
	case kindStringWithIdentifiers, kindMapReferenceOnly:
		// unwrap `${...}` - extract the inner CEL expression
		return expr[2 : len(expr)-1], nil
	case kindStringInterpolation:
		// rewrite interpolation into valid CEL string concatenation
		return rewriteInterpolations(expr)
	default:
		// like boolean expressions
		return expr, nil
	}
}

func stringify(result interface{}) (string, error) {
	switch v := result.(type) {
	case string:
		return v, nil
	case fmt.Stringer:
		return v.String(), nil
	case int, int32, int64, float32, float64:
		return fmt.Sprint(v), nil
	default:
		return "", fmt.Errorf("expression did not evaluate to string, got: %T", result)
	}
}

// rewriteInterpolations converts a string with ${...} interpolations into a CEL-compatible
// concatenation expression, while validating and preserving literal segments.
func rewriteInterpolations(expr string) (string, error) {
	if err := validateInterpolation(expr); err != nil {
		return "", err
	}

	// Fast path: no interpolation at all
	if !strings.Contains(expr, "${") {
		return expr, nil
	}

	// Strip outer quotes if present
	if len(expr) >= 2 && ((expr[0] == '"' && expr[len(expr)-1] == '"') ||
		(expr[0] == '\'' && expr[len(expr)-1] == '\'')) {
		expr = expr[1 : len(expr)-1]
	}

	var parts []string
	lastIndex := 0

	for _, match := range interpolationExprCapturePattern.FindAllStringSubmatchIndex(expr, -1) {
		start, end := match[0], match[1]
		innerStart, innerEnd := match[2], match[3]

		// literal before the interpolation
		if start > lastIndex {
			lit := expr[lastIndex:start]
			if lit != "" {
				parts = append(parts, strconv.Quote(lit))
			}
		}

		inner := strings.TrimSpace(expr[innerStart:innerEnd])
		if inner == "" {
			return "", fmt.Errorf("empty interpolation found")
		}
		parts = append(parts, inner)

		lastIndex = end
	}

	// trailing literal
	if lastIndex < len(expr) {
		lit := expr[lastIndex:]
		parts = append(parts, strconv.Quote(lit))
	}

	return strings.Join(parts, " + "), nil
}

// flattenAttributes recursively converts pcommon.Map attributes into a plain Go map,
// handling scalars, maps, and slices with native Go types.
func flattenAttributes(attrs pcommon.Map) map[string]interface{} {
	result := make(map[string]interface{})
	attrs.Range(func(k string, v pcommon.Value) bool {
		switch v.Type() {
		case pcommon.ValueTypeStr:
			result[k] = v.Str()
		case pcommon.ValueTypeBool:
			result[k] = v.Bool()
		case pcommon.ValueTypeInt:
			result[k] = v.Int()
		case pcommon.ValueTypeDouble:
			result[k] = v.Double()
		case pcommon.ValueTypeMap:
			result[k] = flattenAttributes(v.Map())
		case pcommon.ValueTypeSlice:
			sliceVals := v.Slice()
			list := make([]interface{}, 0, sliceVals.Len())
			for i := 0; i < sliceVals.Len(); i++ {
				elem := sliceVals.At(i)
				switch elem.Type() {
				case pcommon.ValueTypeStr:
					list = append(list, elem.Str())
				case pcommon.ValueTypeBool:
					list = append(list, elem.Bool())
				case pcommon.ValueTypeInt:
					list = append(list, elem.Int())
				case pcommon.ValueTypeDouble:
					list = append(list, elem.Double())
				case pcommon.ValueTypeMap:
					list = append(list, elem.AsString())
				case pcommon.ValueTypeSlice:
					list = append(list, elem.AsString())
				case pcommon.ValueTypeEmpty:
					list = append(list, elem.AsString())
				case pcommon.ValueTypeBytes:
					list = append(list, elem.AsString())
				default:
					list = append(list, elem.AsString()) // fallback
				}
			}
			result[k] = list
		case pcommon.ValueTypeBytes:
			result[k] = v.AsString()
		case pcommon.ValueTypeEmpty:
			result[k] = v.AsString()
		default:
			// fallback: everything has AsString()
			result[k] = v.AsString()
		}
		return true
	})
	return result
}

// cacheSize is used for testing cache re-use
func (e *CelEvaluator) cacheSize() int {
	return e.cache.Len()
}
