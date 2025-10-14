//nolint:testpackage
package internal

import (
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"

	"github.com/stretchr/testify/assert"
)

type mockFilterExpressionEvaluator struct {
	conditionExpressionLookup map[string]bool
}

func (f *mockFilterExpressionEvaluator) EvalStringExpression(_ settings.OtelStringExpression, _ *ExpressionEvalContext) (string, error) {
	return "", nil
}

func (f *mockFilterExpressionEvaluator) EvalOptionalStringExpression(_ *settings.OtelStringExpression, _ *ExpressionEvalContext) (*string, error) {
	//nolint:nilnil
	return nil, nil
}

func (f *mockFilterExpressionEvaluator) EvalBooleanExpression(expr settings.OtelBooleanExpression, _ *ExpressionEvalContext) (bool, error) {
	if _, ok := f.conditionExpressionLookup[expr.Expression]; ok {
		return true, nil
	}
	return false, nil
}

func (f *mockFilterExpressionEvaluator) EvalMapExpression(_ settings.OtelStringExpression, _ *ExpressionEvalContext) (map[string]any, error) {
	//nolint:nilnil
	return nil, nil
}

func createConditionMapping(expr string) settings.OtelConditionMapping {
	return conditionMapping(expr, settings.CREATE)
}

func rejectConditionMapping(expr string) settings.OtelConditionMapping {
	return conditionMapping(expr, settings.REJECT)
}

func conditionMapping(expr string, action settings.OtelConditionMappingAction) settings.OtelConditionMapping {
	return settings.OtelConditionMapping{
		Expression: settings.OtelBooleanExpression{Expression: expr},
		Action:     action,
	}
}

func TestFilter_evalCondition(t *testing.T) {
	t.Parallel()

	evalCtx := NewEvalContext(map[string]any{}, map[string]any{}, map[string]any{}).CloneWithVariables(map[string]any{})

	matchingExpr := "spanAttributes.test-attr"
	nonMatchingExpr := "spanAttributes.non-existing-attr"

	testCases := []struct {
		name                      string
		condition                 settings.OtelConditionMapping
		conditionExpressionLookup map[string]bool
		expectedAction            *settings.OtelConditionMappingAction
	}{
		{
			name:                      "matched condition with CREATE action",
			condition:                 createConditionMapping(matchingExpr),
			conditionExpressionLookup: map[string]bool{matchingExpr: true},
			expectedAction:            ptr(settings.CREATE),
		},
		{
			name:                      "non-matched condition with CREATE action",
			condition:                 createConditionMapping(nonMatchingExpr),
			conditionExpressionLookup: map[string]bool{},
			expectedAction:            nil,
		},
		{
			name:                      "matched condition with REJECT action",
			condition:                 rejectConditionMapping(matchingExpr),
			conditionExpressionLookup: map[string]bool{matchingExpr: true},
			expectedAction:            ptr(settings.REJECT),
		},
		{
			name:                      "non-matched condition with REJECT action",
			condition:                 rejectConditionMapping(nonMatchingExpr),
			conditionExpressionLookup: map[string]bool{},
			expectedAction:            nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			condition := tc.condition
			resultAction := evalCondition(
				&mockFilterExpressionEvaluator{
					conditionExpressionLookup: tc.conditionExpressionLookup,
				},
				evalCtx, &condition,
			)
			assert.Equal(t, tc.expectedAction, resultAction)
		})
	}
}

func TestFilter_filterByConditions(t *testing.T) {
	t.Parallel()

	evalCtx := NewEvalContext(map[string]any{}, map[string]any{}, map[string]any{}).CloneWithVariables(map[string]any{})

	matchingExpr := "spanAttributes.test-attr"
	nonMatchingExpr := "spanAttributes.non-existing-attr"

	testCases := []struct {
		name                      string
		conditions                []settings.OtelConditionMapping
		conditionExpressionLookup map[string]bool
		expected                  bool
	}{
		{
			name:       "Empty conditions list",
			conditions: []settings.OtelConditionMapping{},
			expected:   false,
		},
		{
			name: "Single CREATE condition - matched",
			conditions: []settings.OtelConditionMapping{
				createConditionMapping(matchingExpr),
			},
			conditionExpressionLookup: map[string]bool{matchingExpr: true},
			expected:                  true,
		},
		{
			name: "Single CREATE condition - not matched",
			conditions: []settings.OtelConditionMapping{
				createConditionMapping(nonMatchingExpr),
			},
			expected: false,
		},
		{
			name: "Single REJECT condition - matched",
			conditions: []settings.OtelConditionMapping{
				rejectConditionMapping(matchingExpr),
			},
			conditionExpressionLookup: map[string]bool{matchingExpr: true},
			expected:                  false,
		},
		{
			name: "Single REJECT condition - not matched",
			conditions: []settings.OtelConditionMapping{
				rejectConditionMapping(nonMatchingExpr),
			},
			expected: false,
		},
		{
			name: "CREATE matched then REJECT matched",
			conditions: []settings.OtelConditionMapping{
				createConditionMapping(matchingExpr),
				rejectConditionMapping(matchingExpr),
			},
			conditionExpressionLookup: map[string]bool{matchingExpr: true},
			expected:                  true,
		},
		{
			name: "CREATE not matched then REJECT matched",
			conditions: []settings.OtelConditionMapping{
				createConditionMapping(nonMatchingExpr),
				rejectConditionMapping(matchingExpr),
			},
			conditionExpressionLookup: map[string]bool{matchingExpr: true},
			expected:                  false,
		},
		{
			name: "REJECT matched then CREATE matched",
			conditions: []settings.OtelConditionMapping{
				rejectConditionMapping(matchingExpr),
				createConditionMapping(matchingExpr),
			},
			conditionExpressionLookup: map[string]bool{matchingExpr: true},
			expected:                  false,
		},
		{
			name: "REJECT not matched then CREATE matched",
			conditions: []settings.OtelConditionMapping{
				rejectConditionMapping(nonMatchingExpr),
				createConditionMapping(matchingExpr),
			},
			conditionExpressionLookup: map[string]bool{matchingExpr: true},
			expected:                  true,
		},
		{
			name: "REJECT not matched then CREATE not-matched",
			conditions: []settings.OtelConditionMapping{
				rejectConditionMapping(nonMatchingExpr),
				createConditionMapping(nonMatchingExpr),
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conditions := tc.conditions
			result := filterByConditions(
				&mockFilterExpressionEvaluator{
					conditionExpressionLookup: tc.conditionExpressionLookup,
				},
				evalCtx, &conditions,
			)
			assert.Equal(t, tc.expected, result)
		})
	}
}
