package internal

import (
	"errors"
	"strings"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func EvalBooleanExpression(span *ptrace.Span, expression *settings.OtelBooleanExpression) bool {
	_, exists := span.Attributes().Get(expression.Expression) //TODO implement it
	return exists
}

func EvalStringExpression(expression *settings.OtelStringExpression, span *ptrace.Span, vars *map[string]string) (string, error) {
	if strings.HasPrefix(expression.Expression, "attributes.") {
		attributeName := strings.TrimPrefix(expression.Expression, "attributes.")
		if val, exists := span.Attributes().Get(attributeName); exists {
			return val.AsString(), nil
		}
		return "", errors.New("Not found attribute with name: " + attributeName)
	}
	if strings.HasPrefix(expression.Expression, "vars.") {
		varName := strings.TrimPrefix(expression.Expression, "vars.")
		if val, exists := (*vars)[varName]; exists {
			return val, nil
		}
		return "", errors.New("Not found variable with name: " + varName)
	}
	return expression.Expression, nil
}
