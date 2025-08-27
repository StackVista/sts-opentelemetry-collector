package internal

import (
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func filterByConditions(span *ptrace.Span, scope *ptrace.ScopeSpans, resource *ptrace.ResourceSpans, vars *map[string]string, conditions *[]settings.OtelConditionMapping) bool {
	for _, condition := range *conditions {
		action := evalCondition(span, scope, resource, vars, &condition)
		if action == nil {
			continue
		}
		switch *action {
		case settings.CREATE:
			return true
		case settings.REJECT:
			return false
		}
	}
	// If there are no matching conditions, the span is rejected
	return false
}

func evalCondition(span *ptrace.Span, scope *ptrace.ScopeSpans, resource *ptrace.ResourceSpans, vars *map[string]string, condition *settings.OtelConditionMapping) *settings.OtelConditionMappingAction {
	expressionResult := EvalBooleanExpression(&condition.Expression, span, scope, resource, vars)

	if expressionResult {
		return &condition.Action
	}
	return nil
}
