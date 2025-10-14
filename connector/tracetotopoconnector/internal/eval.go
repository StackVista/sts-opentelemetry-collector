package internal

import (
	"fmt"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
)

// EvalVariables evaluates a list of OtelVariableMapping objects and resolves variable values
// based on span data or expressions.
// It returns a map of variable names and their resolved values and a map of variable names and
// any encountered errors.
func EvalVariables(
	expressionEvaluator ExpressionEvaluator,
	evalContext *ExpressionEvalContext,
	vars *[]settings.OtelVariableMapping,
) (map[string]any, []error) {
	result := make(map[string]any)
	errs := make(map[string]error)
	if vars == nil {
		return result, nil
	}

	for _, variable := range *vars {
		//TODO: Add EvalAnyExpression so variables can also be of other types and don't get stringified
		if value, err := expressionEvaluator.EvalStringExpression(variable.Value, evalContext); err == nil {
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
