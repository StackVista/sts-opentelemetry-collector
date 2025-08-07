package internal

import (
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestFilter_filterByCondition(t *testing.T) {
	t.Parallel()

	testSpan := ptrace.NewSpan()
	testSpan.Attributes().PutBool("test-attr", true)

	testCases := []struct {
		name          string
		span          ptrace.Span
		condition     *settings.OtelConditionMapping
		lastCondition bool
		expected      settings.OtelConditionMappingAction
	}{
		{
			name: "Return CREATE action when expression matches",
			span: testSpan,
			condition: &settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "test-attr"},
				Action:     settings.CREATE,
			},
			lastCondition: false,
			expected:      settings.CREATE,
		},
		{
			name: "Return REJECT action when expression matches",
			span: testSpan,
			condition: &settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "test-attr"},
				Action:     settings.REJECT,
			},
			lastCondition: false,
			expected:      settings.REJECT,
		},
		{
			name: "Continue to the next condition when expression matches and it isn't the last condition",
			span: testSpan,
			condition: &settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "test-attr"},
				Action:     settings.CONTINUE,
			},
			lastCondition: false,
			expected:      settings.CONTINUE,
		},
		{
			name: "Reject on last condition when expression does not match",
			span: testSpan,
			condition: &settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "non-existing-attr"},
				Action:     settings.CREATE,
			},
			lastCondition: true,
			expected:      settings.REJECT,
		},
		{
			name: "Continue on non-last condition when expression does not match",
			span: testSpan,
			condition: &settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "non-existing-attr"},
				Action:     settings.CREATE,
			},
			lastCondition: false,
			expected:      settings.CONTINUE,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := filterByCondition(&tc.span, tc.condition, tc.lastCondition)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
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
			name:       "No conditions provided",
			span:       testSpan,
			conditions: []settings.OtelConditionMapping{},
			expected:   true,
		},
		{
			name: "Single condition matches with CREATE",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "test-attr"},
					Action:     settings.CREATE,
				},
			},
			expected: true,
		},
		{
			name: "Single condition matches with REJECT",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "test-attr"},
					Action:     settings.REJECT,
				},
			},
			expected: false,
		},
		{
			name: "Single condition does not match, continue",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "non-existing-attr"},
					Action:     settings.CONTINUE,
				},
			},
			expected: false,
		},
		{
			name: "Multiple conditions where one matches CREATE",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "non-existing-attr"},
					Action:     settings.CONTINUE,
				},
				{
					Expression: settings.OtelBooleanExpression{Expression: "test-attr"},
					Action:     settings.CREATE,
				},
			},
			expected: true,
		},
		{
			name: "Multiple conditions all result in CONTINUE",
			span: testSpan,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "non-existing-attr"},
					Action:     settings.CONTINUE,
				},
				{
					Expression: settings.OtelBooleanExpression{Expression: "another-non-existing-attr"},
					Action:     settings.CONTINUE,
				},
			},
			expected: false,
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
