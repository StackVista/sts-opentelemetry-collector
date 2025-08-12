package internal

import (
	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func filterByConditions(span *ptrace.Span, scope *ptrace.ScopeSpans, resource *ptrace.ResourceSpans, vars *map[string]string, conditions *[]settings.OtelConditionMapping) bool {
	for _, condition := range *conditions {
		switch evalCondition(span, scope, resource, vars, &condition) {
		case settings.CREATE:
			return true
		case settings.REJECT:
			return false
		case settings.CONTINUE:
			continue
		}
	}
	// If there are no conditions, the span is considered a matched.
	return true
}

func evalCondition(span *ptrace.Span, scope *ptrace.ScopeSpans, resource *ptrace.ResourceSpans, vars *map[string]string, condition *settings.OtelConditionMapping) settings.OtelConditionMappingAction {
	expressionResult := EvalBooleanExpression(&condition.Expression, span, scope, resource, vars)

	if expressionResult {
		return condition.Action
	}
	return settings.CONTINUE
}
