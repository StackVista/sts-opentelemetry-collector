package internal

import (
	"fmt"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"

	"regexp"
	"strconv"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// expressionType specifies the expected result type of a CEL expression.
type expressionType int

const (
	StringType expressionType = iota
	BooleanType
	MapType
)

// classification determines how an expression should be interpreted.
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

type CacheSettings struct {
	Size int
	TTL  time.Duration
}

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
	cache *expirable.LRU[CacheKey, *CacheEntry]
}

type CacheKey struct {
	Expression string
	Type       expressionType
}

type CacheEntry struct {
	Program cel.Program
	Error   error
}

func NewCELEvaluator(cacheSettings CacheSettings) (*CelEvaluator, error) {
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
	cache := expirable.NewLRU[CacheKey, *CacheEntry](cacheSettings.Size, nil, cacheSettings.TTL)
	return &CelEvaluator{env, cache}, nil
}

// ---------------------------------------------------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------------------------------------------------

func (e *CelEvaluator) EvalStringExpression(
	expr settings.OtelStringExpression,
	evalCtx *ExpressionEvalContext,
) (string, error) {
	// String literals are returned as-is without CEL evaluation and caching
	if withoutInterpolation(expr.Expression) {
		return expr.Expression, nil
	}

	// For non-literals, use the cached evaluation path
	val, err := e.evalOrCached(expr.Expression, StringType, func(expression string) (string, error) {
		actualKind, err := classifyExpression(expression)
		if err != nil {
			return "", err
		}

		switch actualKind {
		case kindStringWithIdentifiers:
			return unwrapExpression(expression), nil
		case kindStringInterpolation:
			// rewrite interpolation into valid CEL string concatenation
			toCompile, err := rewriteInterpolations(expression)
			if err != nil {
				return "", err
			}
			return toCompile, nil
		case kindStringLiteral:
			// String literals are handled directly, if we detect a string literal here there is an error in the code path.
			return "", fmt.Errorf("expression %q is a literal string, expected CEL expression", expression)
		default:
			return "", fmt.Errorf("expression %q is not a valid string expression", expression)
		}
	}, evalCtx)

	if err != nil {
		return "", err
	}

	return stringify(val)
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
	result, err := e.evalOrCached(expr.Expression, BooleanType, func(expression string) (string, error) {
		return expression, nil
	}, evalCtx)
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
	val, err := e.evalOrCached(expr.Expression, MapType, func(expression string) (string, error) {
		actualKind, err := classifyExpression(expression)
		if err != nil {
			return "", err
		}

		if actualKind == kindStringWithIdentifiers {
			return unwrapExpression(expression), nil
		} else {
			return "", fmt.Errorf("expression %q is not a valid map expression", expression)
		}
	}, evalCtx)
	if err != nil {
		return nil, err
	}

	m, toMapErr := mapify(val)
	if toMapErr != nil {
		return nil, toMapErr
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
func classifyExpression(expr string) (classification, error) {
	switch {
	case withoutInterpolation(expr):
		return kindStringLiteral, nil
	case strings.HasPrefix(expr, "${") && strings.HasSuffix(expr, "}"):
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

// evalOrCached validates, compiles, and evaluates an expression, using a cache
// to store the result of validation and compilation. The cache key is a composite
// of the expression string and the expected expression type, ensuring that an
// expression is uniquely cached for each evaluation context (e.g., string vs. map).
func (e *CelEvaluator) evalOrCached(
	expression string,
	expectedType expressionType,
	rewriteExpression func(expression string) (string, error),
	ctx *ExpressionEvalContext,
) (interface{}, error) {
	// The cache key includes the expected type to differentiate, for example,
	// a string evaluation from a map evaluation for the same expression text.
	key := CacheKey{Expression: expression, Type: expectedType}

	// 1. Check cache for pre-validated and pre-compiled program
	if entry, ok := e.cache.Get(key); ok {
		if entry.Error != nil {
			return nil, entry.Error
		}
		return e.evaluateProgram(entry.Program, ctx)
	}

	// 2. Cache miss: validate, compile, and cache the result.
	entry := e.validateAndCompile(expression, expectedType, rewriteExpression)
	e.cache.Add(key, entry)
	if entry.Error != nil {
		return nil, entry.Error
	}

	// 3. Evaluation
	return e.evaluateProgram(entry.Program, ctx)
}

// validateAndCompile is a helper that encapsulates the validation and compilation logic.
// It's called on a cache miss.
func (e *CelEvaluator) validateAndCompile(expression string, expectedType expressionType,
	rewriteExpression func(expression string) (string, error)) *CacheEntry {
	// Step 1: Preprocess the expression (unwrap or rewrite interpolations)
	toCompile, err := rewriteExpression(expression)

	if err != nil {
		return &CacheEntry{Error: err}
	}

	// Step 2: Compile the preprocessed expression into a CEL program.
	ast, iss := e.env.Compile(toCompile)
	if iss.Err() != nil {
		return &CacheEntry{Error: iss.Err()}
	}

	// Step 3: Validate that the result type is compatible with the expected type (as far as possible)
	typeErr := validateExpectedType(ast, expectedType, expression)
	if typeErr != nil {
		return &CacheEntry{Error: typeErr}
	}

	prog, err := e.env.Program(ast)
	if err != nil {
		return &CacheEntry{Error: err}
	}

	return &CacheEntry{Program: prog}
}

func validateExpectedType(ast *cel.Ast, expectedType expressionType, expression string) error {
	outputKind := ast.OutputType().Kind()
	switch expectedType {
	case StringType:
		switch outputKind {
		case cel.DynKind, cel.StringKind, cel.IntKind, cel.UintKind, cel.DoubleKind, cel.BoolKind:
			// acceptable types that can be stringified
		default:
			if outputKind != cel.StringKind {
				return fmt.Errorf("expected string type, got: %v, for expression '%v'", ast.OutputType(), expression)
			}
		}
	case MapType:
		if outputKind != cel.MapKind && outputKind != cel.DynKind {
			return fmt.Errorf("expected map type, got: %v, for expression '%v'", ast.OutputType(), expression)
		}
	case BooleanType:
		if outputKind != cel.BoolKind && outputKind != cel.DynKind {
			return fmt.Errorf("expected boolean type, got: %v, for expression '%v'", ast.OutputType(), expression)
		}
	}
	return nil
}

// unwrap `${...}` - extract the inner CEL expression
func unwrapExpression(expression string) string {
	return expression[2 : len(expression)-1]
}

// evaluateProgram executes a compiled CEL program with the given context.
func (e *CelEvaluator) evaluateProgram(prog cel.Program, ctx *ExpressionEvalContext) (interface{}, error) {
	runtimeVars := map[string]interface{}{
		"spanAttributes":     ctx.Span.Attributes().AsRaw(),
		"scopeAttributes":    ctx.Scope.Scope().Attributes().AsRaw(),
		"resourceAttributes": ctx.Resource.Resource().Attributes().AsRaw(),
		"vars":               ctx.Vars,
	}

	result, _, err := prog.Eval(runtimeVars)
	if err != nil {
		return nil, fmt.Errorf("CEL evaluation error: %w", err)
	}

	return result.Value(), nil
}

func stringify(result interface{}) (string, error) {
	switch v := result.(type) {
	case string:
		return v, nil
	case fmt.Stringer:
		return v.String(), nil
	case int, int32, uint64, int64, float32, float64:
		return fmt.Sprint(v), nil
	default:
		return "", fmt.Errorf("expression did not evaluate to string, got: %T", result)
	}
}

func mapify(result interface{}) (map[string]any, error) {
	switch v := result.(type) {
	case map[string]any:
		return v, nil
	case map[ref.Val]ref.Val:
		m := make(map[string]any, len(v))
		for key, val := range v {
			keyStr, keyErr := stringify(key.Value())
			if keyErr != nil {
				return nil, fmt.Errorf("could not convert map key to a string: %w, got: %T", keyErr, key.Value())
			}
			m[keyStr] = val.Value()
		}
		return m, nil
	default:
		return nil, fmt.Errorf("expression did not evaluate to map[string]any, got: %T", result)
	}
}

// rewriteInterpolations converts a string with ${...} interpolations into a CEL-compatible
// concatenation expression, while validating and preserving literal segments.
func rewriteInterpolations(expr string) (string, error) {
	// Validation of the interpolation syntax is expected to have been done
	// already before calling this function.
	// Fast path: no interpolation at all
	if !strings.Contains(expr, "${") {
		return expr, nil
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

	return strings.Join(parts, "+"), nil
}

func unescapeDollars(literal string) string {
	return escapeDollarDollarCurlyPattern.ReplaceAllLiteralString(literal, "$")
}

// cacheSize is used for testing cache re-use
func (e *CelEvaluator) cacheSize() int {
	return e.cache.Len()
}
