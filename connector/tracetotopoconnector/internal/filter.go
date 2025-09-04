package internal

import (
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
)

func filterByConditions(
	expressionEvaluator ExpressionEvaluator,
	expressionEvalCtx *ExpressionEvalContext,
	conditions *[]settings.OtelConditionMapping,
) bool {
	for _, condition := range *conditions {
		action := evalCondition(expressionEvaluator, expressionEvalCtx, &condition)
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

func evalCondition(
	expressionEvaluator ExpressionEvaluator,
	expressionEvalCtx *ExpressionEvalContext,
	condition *settings.OtelConditionMapping,
) *settings.OtelConditionMappingAction {
	expressionResult, err := expressionEvaluator.EvalBooleanExpression(condition.Expression, expressionEvalCtx)
	if err != nil {
		return nil
	}

	if expressionResult {
		return &condition.Action
	}
	return nil
}
