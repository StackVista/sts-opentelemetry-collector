package internal

import (
	"fmt"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
)

// CollectVarReferences walks the given string expressions and returns the union of variable
// names referenced as `vars.<name>` across them.
//
// Returns nil (meaning "could not statically determine — assume all vars are needed") if any
// expression's AST cannot be retrieved. Callers should treat a nil result as a signal to
// fall back to evaluating all variables.
func CollectVarReferences(
	evaluator ExpressionEvaluator,
	expressions ...settingsproto.OtelStringExpression,
) map[string]struct{} {
	names := make(map[string]struct{})
	for _, expr := range expressions {
		astRes, err := evaluator.GetStringExpressionAST(expr)
		if err != nil || astRes == nil || astRes.CheckedAST == nil {
			return nil
		}
		walker := NewExpressionAstWalker()
		walker.Walk(astRes.CheckedAST.NativeRep().Expr())
		for name := range walker.GetVarReferences() {
			names[name] = struct{}{}
		}
	}
	return names
}

// FilterVarsByName returns the subset of vars whose Name is present in `names`.
// Returns the original slice if names is nil (no filtering); returns an empty slice if vars is nil or empty.
func FilterVarsByName(
	vars *[]settingsproto.OtelVariableMapping,
	names map[string]struct{},
) []settingsproto.OtelVariableMapping {
	if names == nil {
		if vars == nil {
			return nil
		}
		return *vars
	}
	if vars == nil {
		return nil
	}
	filtered := make([]settingsproto.OtelVariableMapping, 0, len(names))
	for _, v := range *vars {
		if _, ok := names[v.Name]; ok {
			filtered = append(filtered, v)
		}
	}
	return filtered
}

// EvalVariables evaluates a list of OtelVariableMapping objects and resolves variable values
// based on span data or expressions.
// It returns a map of variable names and their resolved values and a map of variable names and
// any encountered errors.
func EvalVariables(
	expressionEvaluator ExpressionEvaluator,
	evalContext *ExpressionEvalContext,
	vars *[]settingsproto.OtelVariableMapping,
) (map[string]any, []error) {
	result := make(map[string]any)
	errs := make(map[string]error)
	if vars == nil {
		return result, nil
	}

	for _, variable := range *vars {
		if value, err := expressionEvaluator.EvalAnyExpression(variable.Value, evalContext); err == nil {
			result[variable.Name] = value
		} else {
			errs[variable.Name] = err
		}
	}

	if len(errs) > 0 {
		errSlice := make([]error, 0, len(errs))
		for varName, err := range errs {
			errSlice = append(errSlice, fmt.Errorf("variable %q evaluation failed: %w", varName, err))
		}
		return nil, errSlice
	}

	return result, nil
}
