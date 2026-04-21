package internal

import (
	"fmt"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"go.uber.org/zap"
)

// EvalVariables evaluates a list of OtelVariableMapping objects and resolves variable values
// based on span data or expressions.
// It returns a map of variable names and their resolved values and a map of variable names and
// any encountered errors.
func EvalVariables(
	logger *zap.Logger,
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
			logger.Debug("Variable evaluated",
				zap.String("name", variable.Name),
				zap.String("expression", variable.Value.Expression),
				zap.Any("value", value),
			)
			result[variable.Name] = value
		} else {
			logger.Debug("Variable evaluation failed",
				zap.String("name", variable.Name),
				zap.String("expression", variable.Value.Expression),
				zap.Error(err),
			)
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
