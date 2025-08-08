package internal

import (
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestFilter_evalCondition(t *testing.T) {
	t.Parallel()

	testSpan := ptrace.NewSpan()
	testSpan.Attributes().PutBool("test-attr", true)

	testCases := []struct {
		name           string
		span           ptrace.Span
		condition      settings.OtelConditionMapping
		expectedAction settings.OtelConditionMappingAction
	}{
		{
			name: "Matched condition with CREATE action",
			span: testSpan,
			condition: settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "attributes.test-attr"},
				Action:     settings.CREATE,
			},
			expectedAction: settings.CREATE,
		},
		{
			name: "Non-matched condition with CREATE action",
			span: testSpan,
			condition: settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "attributes.non-existing-attr"},
				Action:     settings.CREATE,
			},
			expectedAction: settings.CONTINUE,
		},
		{
			name: "Matched condition with REJECT action",
			span: testSpan,
			condition: settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "attributes.test-attr"},
				Action:     settings.REJECT,
			},
			expectedAction: settings.REJECT,
		},
		{
			name: "Non-matched condition with REJECT action",
			span: testSpan,
			condition: settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "attributes.non-existing-attr"},
				Action:     settings.REJECT,
			},
			expectedAction: settings.CONTINUE,
		},
		{
			name: "Matched condition with CONTINUE action",
			span: testSpan,
			condition: settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "attributes.test-attr"},
				Action:     settings.CONTINUE,
			},
			expectedAction: settings.CONTINUE,
		},
		{
			name: "Non-matched condition with CONTINUE action",
			span: testSpan,
			condition: settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "attributes.non-existing-attr"},
				Action:     settings.CONTINUE,
			},
			expectedAction: settings.CONTINUE,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resultAction := evalCondition(&tc.span, &tc.condition)
			if resultAction != tc.expectedAction {
				t.Errorf("Expected action %v, got %v", tc.expectedAction, resultAction)
			}
		})
	}
}

func TestFilter_filterByConditions(t *testing.T) {
	t.Parallel()

	testSpan := ptrace.NewSpan()
	testSpan.Attributes().PutBool("test-attr", true)

	testCases := []struct {
		name       string
		span       ptrace.Span
		conditions []settings.OtelConditionMapping
		expected   bool
	}{
		{
			name:       "Empty conditions list",
			span:       testSpan,
			conditions: []settings.OtelConditionMapping{},
			expected:   true,
		},
		{
			name: "Single CREATE condition - matched",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.test-attr"},
					Action:     settings.CREATE,
				},
			},
			expected: true,
		},
		{
			name: "Single CREATE condition - not matched",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.non-existing-attr"},
					Action:     settings.CREATE,
				},
			},
			expected: true,
		},
		{
			name: "Single REJECT condition - matched",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.test-attr"},
					Action:     settings.REJECT,
				},
			},
			expected: false,
		},
		{
			name: "Single REJECT condition - not matched",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.non-existing-attr"},
					Action:     settings.REJECT,
				},
			},
			expected: true,
		},
		{
			name: "Single CONTINUE condition - matched",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.test-attr"},
					Action:     settings.CONTINUE,
				},
			},
			expected: true,
		},
		{
			name: "Single CONTINUE condition - not matched",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.non-existing-attr"},
					Action:     settings.CONTINUE,
				},
			},
			expected: true,
		},
		{
			name: "CREATE matched then REJECT matched",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.test-attr"},
					Action:     settings.CREATE,
				},
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.test-attr"},
					Action:     settings.REJECT,
				},
			},
			expected: true,
		},
		{
			name: "CREATE not matched then REJECT matched",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.non-existing-attr"},
					Action:     settings.CREATE,
				},
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.test-attr"},
					Action:     settings.REJECT,
				},
			},
			expected: false,
		},
		{
			name: "REJECT matched then CREATE matched",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.test-attr"},
					Action:     settings.REJECT,
				},
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.test-attr"},
					Action:     settings.CREATE,
				},
			},
			expected: false,
		},
		{
			name: "REJECT not matched then CREATE matched",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.non-existing-attr"},
					Action:     settings.REJECT,
				},
				{
					Expression: settings.OtelBooleanExpression{Expression: "attributes.test-attr"},
					Action:     settings.CREATE,
				},
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := filterByConditions(&tc.span, &tc.conditions)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}
