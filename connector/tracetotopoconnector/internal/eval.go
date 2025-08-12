package internal

import (
	"errors"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// EvalVariables evaluates a list of OtelVariableMapping objects and resolves variable values based on span data or expressions.
// It returns a map of variable names and their resolved values or an error if evaluation fails.
func EvalVariables(vars *[]settings.OtelVariableMapping, span *ptrace.Span) (map[string]string, error) {
	result := make(map[string]string)
	if vars == nil {
		return result, nil
	}

	for _, variable := range *vars {
		if value, err := EvalStringExpression(variable.Value, span, &result); err == nil {
			result[variable.Name] = value
		} else {
			return result, errors.New("Error '" + err.Error() + "' evaluating variable: " + variable.Name)
		}
	}

	return result, nil
}
