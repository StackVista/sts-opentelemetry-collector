package internal

import (
	"errors"
	"fmt"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type ExpressionEvaluator interface {
	EvalStringExpression(expr settings.OtelStringExpression, evalCtx *ExpressionEvalContext) (string, error)
	EvalOptionalStringExpression(expr *settings.OtelStringExpression, evalCtx *ExpressionEvalContext) (*string, error)
	EvalBooleanExpression(expr settings.OtelBooleanExpression, evalCtx *ExpressionEvalContext) (bool, error)
}

type ExpressionEvalContext struct {
	Span     ptrace.Span
	Scope    ptrace.ScopeSpans
	Resource ptrace.ResourceSpans
	Vars     map[string]string
}

type CELEvaluator struct {
	env   *cel.Env
	cache sync.Map // map[string]cel.Program -> could use a LRU cache
}

func NewCELEvaluator() (*CELEvaluator, error) {
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

	return &CELEvaluator{env: env}, nil
}

func (e *CELEvaluator) EvalStringExpression(
	expr settings.OtelStringExpression,
	evalCtx *ExpressionEvalContext,
) (string, error) {
	result, err := e.evalExpression(expr.Expression, evalCtx)
	if err != nil {
		return "", err
	}

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

func (e *CELEvaluator) EvalOptionalStringExpression(
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

func (e *CELEvaluator) EvalBooleanExpression(
	expr settings.OtelBooleanExpression,
	evalCtx *ExpressionEvalContext,
) (bool, error) {
	result, err := e.evalExpression(expr.Expression, evalCtx)
	if err != nil {
		return false, err
	}

	if boolResult, ok := result.(bool); ok {
		return boolResult, nil
	}

	return false, fmt.Errorf("condition did not evaluate to boolean, got: %T", result)
}

func (e *CELEvaluator) evalExpression(expression string, ctx *ExpressionEvalContext) (interface{}, error) {
	prog, err := e.getOrCompile(expression) // cached compiled program
	if err != nil {
		return "", err
	}

	vars := map[string]interface{}{
		"spanAttributes":     flattenAttributes(ctx.Span.Attributes()),
		"scopeAttributes":    flattenAttributes(ctx.Scope.Scope().Attributes()),
		"resourceAttributes": flattenAttributes(ctx.Resource.Resource().Attributes()),
		"vars":               ctx.Vars,
	}

	result, _, err := prog.Eval(vars)
	if err != nil {
		return "", fmt.Errorf("CEL evaluation error: %w", err)
	}

	return result.Value(), nil
}

func (e *CELEvaluator) getOrCompile(expr string) (cel.Program, error) {
	if prog, ok := e.cache.Load(expr); ok {
		ownedProgram, ok := prog.(cel.Program)
		if !ok {
			return nil, errors.New("unable to cast program to owned program")
		}

		return ownedProgram, nil
	}

	// Compile (parse and check) the expression
	ast, issues := e.env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("CEL compilation error: %w", issues.Err())
	}

	// Get evaluable instance
	prog, err := e.env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL program: %w", err)
	}

	e.cache.Store(expr, prog)
	return prog, nil
}

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
				// recursive conversion
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

// used for testing
func (e *CELEvaluator) cacheSize() int {
	count := 0
	e.cache.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
