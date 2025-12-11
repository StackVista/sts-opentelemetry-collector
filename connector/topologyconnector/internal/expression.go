package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
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

	GetStringExpressionAST(expr settings.OtelStringExpression) (*GetASTResult, error)
	GetOptionalStringExpressionAST(expr *settings.OtelStringExpression) (*GetASTResult, error)
	GetBooleanExpressionAST(expr settings.OtelBooleanExpression) (*GetASTResult, error)
	GetMapExpressionAST(expr settings.OtelAnyExpression) (*GetASTResult, error)
	GetAnyExpressionAST(expr settings.OtelAnyExpression) (*GetASTResult, error)
}

type GetASTResult struct {
	CheckedAST *cel.Ast
	literal    *string // if the expression is a literal type (before evaluation, like a string)
}

func ptr[T any](v T) *T { return &v }

type ExpressionEvalContext struct {
	Span      *Span
	Datapoint *Datapoint
	Metric    *Metric
	Scope     *Scope
	Resource  *Resource
	Vars      map[string]any
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
	CheckedAST *cel.Ast
	Program    cel.Program
	Error      error
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

func NewSpanEvalContext(
	span *Span, scope *Scope, resource *Resource,
) *ExpressionEvalContext {
	return &ExpressionEvalContext{
		Span:      span,
		Datapoint: nil,
		Metric:    nil,
		Scope:     scope,
		Resource:  resource,
		Vars:      nil,
	}
}

func NewMetricEvalContext(
	datapoint *Datapoint, metric *Metric, scope *Scope, resource *Resource,
) *ExpressionEvalContext {
	return &ExpressionEvalContext{
		Span:      nil,
		Datapoint: datapoint,
		Metric:    metric,
		Scope:     scope,
		Resource:  resource,
		Vars:      nil,
	}
}

func (ec *ExpressionEvalContext) CloneWithVariables(vars map[string]any) *ExpressionEvalContext {
	return &ExpressionEvalContext{
		Span:      ec.Span,
		Metric:    ec.Metric,
		Datapoint: ec.Datapoint,
		Scope:     ec.Scope,
		Resource:  ec.Resource,
		Vars:      vars,
	}
}

func NewCELEvaluator(ctx context.Context, cacheSettings metrics.MeteredCacheSettings) (*CelEvaluator, error) {
	env, err := cel.NewEnv(
		cel.Variable("span", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("datapoint", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("metric", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("scope", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("resource", cel.MapType(cel.StringType, cel.DynType)),
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
	val, err := e.evalOrCached(expr.Expression, StringType, rewriteStringExpression, evalCtx)

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
	result, err := e.evalOrCached(expr.Expression, BooleanType, rewriteIdentityExpression, evalCtx)
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
	val, err := e.evalOrCached(expr.Expression, MapType, rewriteMapExpression, evalCtx)
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
	val, err := e.evalOrCached(expr.Expression, AnyType, rewriteAnyExpression, evalCtx)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (e *CelEvaluator) GetStringExpressionAST(expr settings.OtelStringExpression) (*GetASTResult, error) {
	// String literals are returned as-is without CEL evaluation and caching
	if withoutInterpolation(expr.Expression) {
		return &GetASTResult{literal: ptr(expr.Expression)}, nil
	}

	ast, err := e.astOrCached(expr.Expression, StringType, rewriteStringExpression)
	if err != nil {
		return nil, err
	}
	return &GetASTResult{CheckedAST: ast}, nil
}

func (e *CelEvaluator) GetOptionalStringExpressionAST(expr *settings.OtelStringExpression) (*GetASTResult, error) {
	if expr == nil {
		//nolint:nilnil
		return nil, nil
	}

	result, err := e.GetStringExpressionAST(*expr)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *CelEvaluator) GetBooleanExpressionAST(expr settings.OtelBooleanExpression) (*GetASTResult, error) {
	ast, err := e.astOrCached(expr.Expression, BooleanType, rewriteIdentityExpression)
	if err != nil {
		return nil, err
	}
	return &GetASTResult{CheckedAST: ast}, nil
}

func (e *CelEvaluator) GetMapExpressionAST(expr settings.OtelAnyExpression) (*GetASTResult, error) {
	ast, err := e.astOrCached(expr.Expression, MapType, rewriteMapExpression)
	if err != nil {
		return nil, err
	}
	return &GetASTResult{CheckedAST: ast}, nil
}

func (e *CelEvaluator) GetAnyExpressionAST(expr settings.OtelAnyExpression) (*GetASTResult, error) {
	ast, err := e.astOrCached(expr.Expression, AnyType, rewriteAnyExpression)
	if err != nil {
		return nil, err
	}
	return &GetASTResult{CheckedAST: ast}, nil
}

// ---------------------------------------------------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------------------------------------------------

type rewriteFunc func(string) (string, error)

func rewriteStringExpression(expression string) (string, error) {
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
}

func rewriteIdentityExpression(expression string) (string, error) {
	return expression, nil
}

func rewriteMapExpression(expression string) (string, error) {
	actualKind, err := classifyExpression(expression)
	if err != nil {
		return "", err
	}

	if actualKind == kindStringWithIdentifiers {
		return unwrapExpression(expression), nil
	}
	return "", fmt.Errorf("expression %q is not a valid map expression", expression)
}

func rewriteAnyExpression(expression string) (string, error) {
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
}

// evalOrCached validates, compiles, and evaluates an expression, using a cache
// to store the result of validation and compilation. The cache Key is a composite
// of the expression string and the expected expression type, ensuring that an
// expression is uniquely cached for each evaluation context (e.g., string vs. map).
func (e *CelEvaluator) evalOrCached(
	expression string,
	expectedType expressionType,
	rewriteExpression rewriteFunc,
	ctx *ExpressionEvalContext,
) (interface{}, error) {
	// The cache Key includes the expected type to differentiate, for example,
	// a string evaluation from a map evaluation for the same expression text.
	key := CacheKey{Expression: expression, Type: expectedType}

	// 1. Check cache for pre-validated and pre-compiled program
	if entry, ok := e.cache.Get(key); ok {
		if entry.Error != nil {
			return nil, entry.Error
		}
		if entry.Program != nil {
			return e.evaluateProgram(entry.Program, ctx)
		}
	}

	// 2. Cache miss: validate, compile, and cache the result.
	entry := e.validateAndCompile(key, rewriteExpression, true)
	e.cache.Add(key, entry)
	if entry.Error != nil {
		return nil, entry.Error
	}

	// 3. Evaluation
	return e.evaluateProgram(entry.Program, ctx)
}

func (e *CelEvaluator) astOrCached(
	expression string,
	expectedType expressionType,
	rewriteExpression rewriteFunc,
) (*cel.Ast, error) {
	// The cache Key includes the expected type to differentiate, for example,
	// a string evaluation from a map evaluation for the same expression text.
	key := CacheKey{Expression: expression, Type: expectedType}

	// Step 1: Check cache for pre-validated and pre-checked AST
	if entry, ok := e.cache.Get(key); ok {
		if entry.Error != nil {
			return nil, entry.Error
		}
		if entry.CheckedAST != nil {
			return entry.CheckedAST, nil
		}
	}

	// 2. Cache miss: validate, compile, and cache the result.
	entry := e.validateAndCompile(key, rewriteExpression, false)
	e.cache.Add(key, entry)
	if entry.Error != nil {
		return nil, entry.Error
	}

	return entry.CheckedAST, nil
}

// validateAndCompile is a helper that encapsulates the validation and compilation logic.
// It's called on a cache miss.
func (e *CelEvaluator) validateAndCompile(
	key CacheKey,
	rewriteExpression rewriteFunc,
	buildProgram bool,
) *CacheEntry {
	entry, ok := e.cache.Get(key)
	if !ok {
		entry = &CacheEntry{}
	}

	// Step 1: Preprocess the expression (unwrap or rewrite interpolations)
	toCompile, err := rewriteExpression(key.Expression)
	if err != nil {
		entry.Error = err
		return entry
	}

	// Step 2: Compile the preprocessed expression into a CEL AST.
	if entry.CheckedAST == nil {
		checkedAst, iss := e.env.Compile(toCompile)
		if iss.Err() != nil {
			entry.Error = iss.Err()
			return entry
		}
		entry.CheckedAST = checkedAst
	}

	// Step 3: Validate that the result type is compatible with the expected type (as far as possible)
	typeErr := validateExpectedType(entry.CheckedAST, key.Type, key.Expression)
	if typeErr != nil {
		entry.Error = typeErr
		return entry
	}

	// Step 4: Generate an evaluable instance of the AST
	if entry.Program == nil && buildProgram {
		prog, programErr := e.env.Program(entry.CheckedAST)
		if programErr != nil {
			entry.Error = programErr
			return entry
		}

		entry.Program = prog
	}

	return entry
}

func validateExpectedType(ast *cel.Ast, expectedType expressionType, expression string) error {
	outputKind := ast.OutputType().Kind()
	switch expectedType {
	case StringType:
		switch outputKind {
		case cel.DynKind:
			// if a string type is expected, and the expression ends with attributes, it references a map, which
			// means the expression is invalid.
			if strings.HasSuffix(expression, "attributes}") {
				return fmt.Errorf("expected string type, got: map(string, dyn), for expression '%v'", expression)
			}
		case cel.StringKind, cel.IntKind, cel.UintKind, cel.DoubleKind, cel.BoolKind:
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
		"resource": ctx.Resource.ToMap(),
		"vars":     ctx.Vars,
	}

	if ctx.Scope != nil {
		runtimeVars["scope"] = ctx.Scope.ToMap()
	}

	// To get the best error messages, the metric/datapoint/span should not be set at all
	// if they are not defined
	if ctx.Metric != nil {
		runtimeVars["metric"] = ctx.Metric.ToMap()
	}

	if ctx.Datapoint != nil {
		runtimeVars["datapoint"] = ctx.Datapoint.ToMap()
	}

	if ctx.Span != nil {
		runtimeVars["span"] = ctx.Span.ToMap()
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
	case int, int16, int32, uint64, int64, float32, float64:
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
				return nil, newCelEvaluationError("cannot convert Key of type '%T' to string: %v: ", keyErr, key.Value())
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
