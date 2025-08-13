package internal

import (
	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// FilterComponent filters a span based on conditions defined in the OtelComponentMapping and returns the filtered span or nil.
func FilterComponent(span *ptrace.Span, scope *ptrace.ScopeSpans, resource *ptrace.ResourceSpans, mapping *settings.OtelComponentMapping) *ptrace.Span {
	if filterByConditions(span, scope, resource, mapping.Conditions) {
		return span
	} else {
		return nil
	}
}

// FilterRelation filters a given span based on specified conditions in the OtelComponentMapping.
// Returns the span if it matches the conditions, otherwise returns nil.
func FilterRelation(span *ptrace.Span, scope *ptrace.ScopeSpans, resource *ptrace.ResourceSpans, mapping *settings.OtelComponentMapping) *ptrace.Span {
	if filterByConditions(span, scope, resource, mapping.Conditions) {
		return span
	} else {
		return nil
	}
}

func filterByConditions(span *ptrace.Span, scope *ptrace.ScopeSpans, resource *ptrace.ResourceSpans, conditions *[]settings.OtelConditionMapping) bool {
	for _, condition := range *conditions {
		switch evalCondition(span, scope, resource, &condition) {
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

func evalCondition(span *ptrace.Span, scope *ptrace.ScopeSpans, resource *ptrace.ResourceSpans, condition *settings.OtelConditionMapping) settings.OtelConditionMappingAction {
	expressionResult := EvalBooleanExpression(&condition.Expression, span, scope, resource)

	if expressionResult {
		return condition.Action
	}
	return settings.CONTINUE
}
