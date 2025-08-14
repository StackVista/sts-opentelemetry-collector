package internal

import (
	"errors"
	"strings"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func EvalBooleanExpression(expression *settings.OtelBooleanExpression, span *ptrace.Span, scope *ptrace.ScopeSpans, resource *ptrace.ResourceSpans) bool {
	if strings.HasPrefix(expression.Expression, "spanAttributes.") {
		attributeName := strings.TrimPrefix(expression.Expression, "spanAttributes.")
		_, exists := span.Attributes().Get(attributeName) //TODO implement it
		return exists
	} else if strings.HasPrefix(expression.Expression, "scopeAttributes.") {
		attributeName := strings.TrimPrefix(expression.Expression, "scopeAttributes.")
		_, exists := scope.Scope().Attributes().Get(attributeName) //TODO implement it
		return exists
	} else if strings.HasPrefix(expression.Expression, "resourceAttributes.") {
		attributeName := strings.TrimPrefix(expression.Expression, "resourceAttributes.")
		_, exists := resource.Resource().Attributes().Get(attributeName) //TODO implement it
		return exists
	} else {
		return false
	}
}

func EvalStringExpression(expression settings.OtelStringExpression, span *ptrace.Span, scope *ptrace.ScopeSpans, resource *ptrace.ResourceSpans, vars *map[string]string) (string, error) {
	if strings.HasPrefix(expression.Expression, "spanAttributes.") {
		attributeName := strings.TrimPrefix(expression.Expression, "spanAttributes.")
		if val, exists := span.Attributes().Get(attributeName); exists {
			return val.AsString(), nil
		}
		return "", errors.New("Not found span attribute with name: " + attributeName)
	}
	if strings.HasPrefix(expression.Expression, "scopeAttributes.") {
		attributeName := strings.TrimPrefix(expression.Expression, "scopeAttributes.")
		if val, exists := scope.Scope().Attributes().Get(attributeName); exists {
			return val.AsString(), nil
		}
		return "", errors.New("Not found scope attribute with name: " + attributeName)
	}
	if strings.HasPrefix(expression.Expression, "resourceAttributes.") {
		attributeName := strings.TrimPrefix(expression.Expression, "resourceAttributes.")
		if val, exists := resource.Resource().Attributes().Get(attributeName); exists {
			return val.AsString(), nil
		}
		return "", errors.New("Not found span attribute with name: " + attributeName)
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

func EvalOptionalStringExpression(expression *settings.OtelStringExpression, span *ptrace.Span, scope *ptrace.ScopeSpans, resource *ptrace.ResourceSpans, vars *map[string]string) (*string, error) {
	if expression == nil {
		return nil, nil
	} else {
		result, err := EvalStringExpression(*expression, span, scope, resource, vars)
		return &result, err
	}
}
