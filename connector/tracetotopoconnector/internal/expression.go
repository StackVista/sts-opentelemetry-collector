package internal

import (
	"fmt"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"

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

type CacheSettings struct {
	Size int
	TTL  time.Duration
}

type ExpressionEvaluator interface {
	// "String" Expressions expect/support the interpolation syntax ${}
	EvalStringExpression(expr settings.OtelStringExpression, evalCtx *ExpressionEvalContext) (string, error)
	EvalOptionalStringExpression(expr *settings.OtelStringExpression, evalCtx *ExpressionEvalContext) (*string, error)
	// "Boolean" Expressions don't support interpolation, but must be a valid CEL boolean expression
	EvalBooleanExpression(expr settings.OtelBooleanExpression, evalCtx *ExpressionEvalContext) (bool, error)
	// "Map" Expressions expect/support the interpolation syntax ${}
	EvalMapExpression(expr settings.OtelStringExpression, evalCtx *ExpressionEvalContext) (map[string]any, error)
	// "Any" Expressions expect/support the interpolation syntax ${}
	EvalAnyExpression(expr settings.OtelStringExpression, evalCtx *ExpressionEvalContext) (any, error)
}

type ExpressionEvalContext struct {
	SpanAttributes     map[string]any
	ScopeAttributes    map[string]any
	ResourceAttributes map[string]any
	Vars               map[string]any
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

func NewEvalContext(spanAttributes map[string]any, scopeAttributes map[string]any,
	resourceAttributes map[string]any) *ExpressionEvalContext {
	return &ExpressionEvalContext{
		SpanAttributes:     spanAttributes,
		ScopeAttributes:    scopeAttributes,
		ResourceAttributes: resourceAttributes,
		Vars:               nil,
	}
}

func (ec *ExpressionEvalContext) CloneWithVariables(vars map[string]any) *ExpressionEvalContext {
	return &ExpressionEvalContext{
		SpanAttributes:     ec.SpanAttributes,
		ScopeAttributes:    ec.ScopeAttributes,
		ResourceAttributes: ec.ResourceAttributes,
		Vars:               vars,
	}
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
	expr settings.OtelStringExpression,
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
			return fmt.Errorf("expected boolean type, got: %v, for expression '%v'", ast.OutputType(), expression)
		}
	case AnyType:
		// anything goes
	}
	return nil
}

// evaluateProgram executes a compiled CEL program with the given context.
func (e *CelEvaluator) evaluateProgram(prog cel.Program, ctx *ExpressionEvalContext) (interface{}, error) {
	runtimeVars := map[string]interface{}{
		"spanAttributes":     ctx.SpanAttributes,
		"scopeAttributes":    ctx.ScopeAttributes,
		"resourceAttributes": ctx.ResourceAttributes,
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

// cacheSize is used for testing cache re-use
func (e *CelEvaluator) cacheSize() int {
	return e.cache.Len()
}
