package internal

import (
	"context"
	"fmt"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
)

// expressionType specifies the expected result type of a CEL expression.
type expressionType int

const (
	StringType expressionType = iota
	BooleanType
	MapType
	AnyType
)

type ExpressionEvaluator interface {
	// EvalStringExpression "String" Expressions expect/support the interpolation syntax ${}
	EvalStringExpression(expr settings.OtelStringExpression, evalCtx *ExpressionEvalContext) (string, error)
	EvalOptionalStringExpression(expr *settings.OtelStringExpression, evalCtx *ExpressionEvalContext) (*string, error)
	// EvalBooleanExpression "Boolean" Expressions don't support interpolation, but must be a valid CEL boolean expression
	EvalBooleanExpression(expr settings.OtelBooleanExpression, evalCtx *ExpressionEvalContext) (bool, error)
	// EvalMapExpression "Map" Expressions expect/support the interpolation syntax ${}
	EvalMapExpression(expr settings.OtelAnyExpression, evalCtx *ExpressionEvalContext) (map[string]any, error)
	// EvalAnyExpression "Any" Expressions expect/support the interpolation syntax ${}
	EvalAnyExpression(expr settings.OtelAnyExpression, evalCtx *ExpressionEvalContext) (any, error)
}

type ExpressionEvalContext struct {
	SpanAttributes     map[string]any
	MetricAttributes   map[string]any
	ScopeAttributes    map[string]any
	ResourceAttributes map[string]any
	Vars               map[string]any
}

type CelEvaluator struct {
	env   *cel.Env
	cache *metrics.MeteredCache[CacheKey, *CacheEntry]
}

type CacheKey struct {
	Expression string
	Type       expressionType
}

type CacheEntry struct {
	Program cel.Program
	Error   error
}

// Introduce a custom error type for evaluation errors:
// 1. It allows the caller to recognize evaluation errors, because for optional fields evaluation errors are ignored
// 2. It makes the error message lazy to avoid most of the allocations involved in building it when they are ignored
type CelEvaluationError struct {
	format    string
	arguments []any
}

func newCelEvaluationError(format string, a ...any) *CelEvaluationError {
	return &CelEvaluationError{
		format:    format,
		arguments: a,
	}
}

func (e *CelEvaluationError) Error() string {
	return fmt.Sprintf(e.format, e.arguments...)
}

func NewSpanEvalContext(spanAttributes map[string]any,
	scopeAttributes map[string]any, resourceAttributes map[string]any) *ExpressionEvalContext {
	return &ExpressionEvalContext{
		SpanAttributes:     spanAttributes,
		MetricAttributes:   nil,
		ScopeAttributes:    scopeAttributes,
		ResourceAttributes: resourceAttributes,
		Vars:               nil,
	}
}

func NewMetricEvalContext(metricAttributes map[string]any,
	scopeAttributes map[string]any, resourceAttributes map[string]any) *ExpressionEvalContext {
	return &ExpressionEvalContext{
		SpanAttributes:     nil,
		MetricAttributes:   metricAttributes,
		ScopeAttributes:    scopeAttributes,
		ResourceAttributes: resourceAttributes,
		Vars:               nil,
	}
}

func (ec *ExpressionEvalContext) CloneWithVariables(vars map[string]any) *ExpressionEvalContext {
	return &ExpressionEvalContext{
		SpanAttributes:     ec.SpanAttributes,
		MetricAttributes:   ec.MetricAttributes,
		ScopeAttributes:    ec.ScopeAttributes,
		ResourceAttributes: ec.ResourceAttributes,
		Vars:               vars,
	}
}

func NewCELEvaluator(ctx context.Context, cacheSettings metrics.MeteredCacheSettings) (*CelEvaluator, error) {
	env, err := cel.NewEnv(
		cel.Variable("spanAttributes", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("metricAttributes", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("scopeAttributes", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("resourceAttributes", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("vars", cel.MapType(cel.StringType, cel.DynType)),
		ext.Strings(), // enables string manipulation functions
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}
	cache := metrics.NewCache[CacheKey, *CacheEntry](ctx, cacheSettings, nil)
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
			toCompile := rewriteInterpolations(expression)
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
	if err != nil {
		return nil, err
	}
	return &result, nil
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

	return false, newCelEvaluationError("condition did not evaluate to boolean, got: %T", result)
}

func (e *CelEvaluator) EvalMapExpression(
	expr settings.OtelAnyExpression,
	evalCtx *ExpressionEvalContext,
) (map[string]any, error) {
	val, err := e.evalOrCached(expr.Expression, MapType, func(expression string) (string, error) {
		actualKind, err := classifyExpression(expression)
		if err != nil {
			return "", err
		}

		if actualKind == kindStringWithIdentifiers {
			return unwrapExpression(expression), nil
		}
		return "", fmt.Errorf("expression %q is not a valid map expression", expression)
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

func (e *CelEvaluator) EvalAnyExpression(
	expr settings.OtelAnyExpression,
	evalCtx *ExpressionEvalContext,
) (any, error) {
	// String literals are returned as-is without CEL evaluation and caching
	if withoutInterpolation(expr.Expression) {
		return expr.Expression, nil
	}

	// For non-literals, use the cached evaluation path
	val, err := e.evalOrCached(expr.Expression, AnyType, func(expression string) (string, error) {
		actualKind, err := classifyExpression(expression)
		if err != nil {
			return "", err
		}

		switch actualKind {
		case kindStringWithIdentifiers:
			return unwrapExpression(expression), nil
		case kindStringInterpolation:
			// rewrite interpolation into valid CEL string concatenation
			toCompile := rewriteInterpolations(expression)
			return toCompile, nil
		case kindStringLiteral:
			// String literals are handled directly, if we detect a string literal here there is an error in the code path.
			return "", fmt.Errorf("expression %q is a literal string, expected CEL expression", expression)
		default:
			return "", fmt.Errorf("expression %q is not a valid string expression", expression)
		}
	}, evalCtx)

	if err != nil {
		return nil, err
	}
	return val, nil
}

// ---------------------------------------------------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------------------------------------------------

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
			return fmt.Errorf("expected bool type, got: %v, for expression '%v'", ast.OutputType(), expression)
		}
	case AnyType:
		// anything goes
	}
	return nil
}

// evaluateProgram executes a compiled CEL program with the given context.
func (e *CelEvaluator) evaluateProgram(prog cel.Program, ctx *ExpressionEvalContext) (interface{}, error) {
	runtimeVars := map[string]interface{}{
		"scopeAttributes":    ctx.ScopeAttributes,
		"resourceAttributes": ctx.ResourceAttributes,
		"vars":               ctx.Vars,
	}

	// To get the best error messages the metricAttributes/spanAttributes should not be set at all
	// if they are not defined
	if ctx.MetricAttributes != nil {
		runtimeVars["metricAttributes"] = ctx.MetricAttributes
	}

	if ctx.SpanAttributes != nil {
		runtimeVars["spanAttributes"] = ctx.SpanAttributes
	}

	result, _, err := prog.Eval(runtimeVars)
	if err != nil {
		return nil, newCelEvaluationError("%v", err)
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
		return "", newCelEvaluationError("cannot convert '%T' to 'string'", result)
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
				return nil, newCelEvaluationError("cannot convert key of type '%T' to string: %v: ", keyErr, key.Value())
			}
			m[keyStr] = val.Value()
		}
		return m, nil
	default:
		return nil, newCelEvaluationError("expected 'map[string]any', got '%T'", result)
	}
}

// cacheSize is used for testing cache re-use
func (e *CelEvaluator) cacheSize() int {
	return e.cache.Len()
}
