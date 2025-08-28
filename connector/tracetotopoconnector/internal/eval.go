package internal

import (
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// EvalVariables evaluates a list of OtelVariableMapping objects and resolves variable values based on span data or expressions.
// It returns a map of variable names and their resolved values and a map of variable names and any encountered errors.
func EvalVariables(expressionEvaluator ExpressionEvaluator, span *ptrace.Span, scope *ptrace.ScopeSpans, resource *ptrace.ResourceSpans, vars *[]settings.OtelVariableMapping) (map[string]string, map[string]error) {
	result := make(map[string]string)
	errs := make(map[string]error)
	if vars == nil {
		return result, nil
	}

	evalContext := &ExpressionEvalContext{}
	if span != nil {
		evalContext.Span = *span
	}
	if scope != nil {
		evalContext.Scope = *scope
	}
	if resource != nil {
		evalContext.Resource = *resource
	}

	for _, variable := range *vars {
		if value, err := expressionEvaluator.EvalStringExpression(variable.Value, evalContext); err == nil {
			result[variable.Name] = value
		} else {
			errs[variable.Name] = err
		}
	}

	if len(errs) > 0 {
		return result, errs
	} else {
		return result, nil
	}
}
