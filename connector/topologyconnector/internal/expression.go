package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
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
	// EvalStringExpression evaluates a CEL string expression. Plain strings that are not valid CEL
	// are returned as-is (literal fallback).
	EvalStringExpression(expr settingsproto.OtelStringExpression, evalCtx *ExpressionEvalContext) (string, error)
	EvalOptionalStringExpression(expr *settingsproto.OtelStringExpression, evalCtx *ExpressionEvalContext) (*string, error)
	// EvalBooleanExpression evaluates a CEL boolean expression.
	EvalBooleanExpression(expr settingsproto.OtelBooleanExpression, evalCtx *ExpressionEvalContext) (bool, error)
	// EvalMapExpression evaluates a CEL map expression.
	EvalMapExpression(expr settingsproto.OtelAnyExpression, evalCtx *ExpressionEvalContext) (map[string]any, error)
	// EvalAnyExpression evaluates a CEL expression of any type. Plain strings that are not valid CEL
	// are returned as-is (literal fallback).
	EvalAnyExpression(expr settingsproto.OtelAnyExpression, evalCtx *ExpressionEvalContext) (any, error)

	// GetStringExpressionAST returns the parsed AST of a String expression without evaluating it.
	GetStringExpressionAST(expr settingsproto.OtelStringExpression) (*GetASTResult, error)
	// GetBooleanExpressionAST returns the parsed AST of a Boolean expression without evaluation.
	GetBooleanExpressionAST(expr settingsproto.OtelBooleanExpression) (*GetASTResult, error)
	// GetMapExpressionAST returns the parsed AST of a Map expression without evaluating it.
	GetMapExpressionAST(expr settingsproto.OtelAnyExpression) (*GetASTResult, error)
	// GetAnyExpressionAST returns the parsed AST of an Any expression without evaluating it.
	GetAnyExpressionAST(expr settingsproto.OtelAnyExpression) (*GetASTResult, error)
}

type GetASTResult struct {
	CheckedAST *cel.Ast
}

func ptr[T any](v T) *T { return &v }

type ExpressionEvalContext struct {
	Span      *Span
	Log       *Log
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
		Log:       nil,
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
		Log:       nil,
		Datapoint: datapoint,
		Metric:    metric,
		Scope:     scope,
		Resource:  resource,
		Vars:      nil,
	}
}

func NewLogEvalContext(
	log *Log, scope *Scope, resource *Resource,
) *ExpressionEvalContext {
	return &ExpressionEvalContext{
		Span:      nil,
		Log:       log,
		Datapoint: nil,
		Metric:    nil,
		Scope:     scope,
		Resource:  resource,
		Vars:      nil,
	}
}

func (ec *ExpressionEvalContext) CloneWithVariables(vars map[string]any) *ExpressionEvalContext {
	return &ExpressionEvalContext{
		Span:      ec.Span,
		Log:       ec.Log,
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
		cel.Variable("log", cel.MapType(cel.StringType, cel.DynType)),
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
	expr settingsproto.OtelStringExpression,
	evalCtx *ExpressionEvalContext,
) (string, error) {
	val, err := e.evalOrCached(expr.Expression, StringType, evalCtx)
	if err != nil {
		return "", err
	}
	return stringify(val)
}

func (e *CelEvaluator) EvalOptionalStringExpression(
	expr *settingsproto.OtelStringExpression,
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
	expr settingsproto.OtelBooleanExpression,
	evalCtx *ExpressionEvalContext,
) (bool, error) {
	result, err := e.evalOrCached(expr.Expression, BooleanType, evalCtx)
	if err != nil {
		return false, err
	}

	if boolResult, ok := result.(bool); ok {
		return boolResult, nil
	}

	return false, newCelEvaluationError("condition did not evaluate to boolean, got: %T", result)
}

func (e *CelEvaluator) EvalMapExpression(
	expr settingsproto.OtelAnyExpression,
	evalCtx *ExpressionEvalContext,
) (map[string]any, error) {
	val, err := e.evalOrCached(expr.Expression, MapType, evalCtx)
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
	expr settingsproto.OtelAnyExpression,
	evalCtx *ExpressionEvalContext,
) (any, error) {
	val, err := e.evalOrCached(expr.Expression, AnyType, evalCtx)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (e *CelEvaluator) GetStringExpressionAST(expr settingsproto.OtelStringExpression) (*GetASTResult, error) {
	ast, err := e.astOrCached(expr.Expression, StringType)
	if err != nil {
		return nil, err
	}
	return &GetASTResult{CheckedAST: ast}, nil
}

func (e *CelEvaluator) GetBooleanExpressionAST(expr settingsproto.OtelBooleanExpression) (*GetASTResult, error) {
	ast, err := e.astOrCached(expr.Expression, BooleanType)
	if err != nil {
		return nil, err
	}
	return &GetASTResult{CheckedAST: ast}, nil
}

func (e *CelEvaluator) GetMapExpressionAST(expr settingsproto.OtelAnyExpression) (*GetASTResult, error) {
	ast, err := e.astOrCached(expr.Expression, MapType)
	if err != nil {
		return nil, err
	}
	return &GetASTResult{CheckedAST: ast}, nil
}

func (e *CelEvaluator) GetAnyExpressionAST(expr settingsproto.OtelAnyExpression) (*GetASTResult, error) {
	ast, err := e.astOrCached(expr.Expression, AnyType)
	if err != nil {
		return nil, err
	}
	return &GetASTResult{CheckedAST: ast}, nil
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
	ctx *ExpressionEvalContext,
) (interface{}, error) {
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
	entry := e.validateAndCompile(key, true)
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
) (*cel.Ast, error) {
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
	entry := e.validateAndCompile(key, false)
	e.cache.Add(key, entry)
	if entry.Error != nil {
		return nil, entry.Error
	}

	return entry.CheckedAST, nil
}

// validateAndCompile compiles a CEL expression and validates its output type.
// All expressions must be valid CEL. String literals must be quoted (e.g., 'backend').
func (e *CelEvaluator) validateAndCompile(
	key CacheKey,
	buildProgram bool,
) *CacheEntry {
	entry, ok := e.cache.Get(key)
	if !ok {
		entry = &CacheEntry{}
	}

	// Step 1: Compile the expression into a CEL AST.
	if entry.CheckedAST == nil {
		checkedAst, iss := e.env.Compile(key.Expression)
		if iss.Err() != nil {
			entry.Error = iss.Err()
			return entry
		}
		entry.CheckedAST = checkedAst
	}

	// Step 2: Validate that the result type is compatible with the expected type (as far as possible)
	typeErr := validateExpectedType(entry.CheckedAST, key.Type, key.Expression)
	if typeErr != nil {
		entry.Error = typeErr
		return entry
	}

	// Step 3: Generate an evaluable instance of the AST
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
			// if a string type is expected, and the expression ends with "attributes", it references a map, which
			// means the expression is invalid.
			if strings.HasSuffix(expression, "attributes") {
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

	if ctx.Log != nil {
		runtimeVars["log"] = ctx.Log.ToMap()
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
