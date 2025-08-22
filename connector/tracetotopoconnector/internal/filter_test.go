package internal

import (
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestFilter_evalCondition(t *testing.T) {
	t.Parallel()

	testSpan := ptrace.NewSpan()
	testSpan.Attributes().PutBool("test-attr", true)

	testScope := ptrace.NewScopeSpans()
	testScope.Scope().Attributes().PutBool("test-attr-scope", true)

	testResource := ptrace.NewResourceSpans()
	testResource.Resource().Attributes().PutBool("test-attr-resource", true)

	testCases := []struct {
		name           string
		span           *ptrace.Span
		scope          *ptrace.ScopeSpans
		resource       *ptrace.ResourceSpans
		condition      settings.OtelConditionMapping
		expectedAction *settings.OtelConditionMappingAction
	}{
		{
			name:     "Matched condition with CREATE action",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			condition: settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.test-attr"},
				Action:     settings.CREATE,
			},
			expectedAction: Ptr(settings.CREATE),
		},
		{
			name:     "Non-matched condition with CREATE action",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			condition: settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.non-existing-attr"},
				Action:     settings.CREATE,
			},
			expectedAction: nil,
		},
		{
			name:     "Matched condition with REJECT action",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			condition: settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.test-attr"},
				Action:     settings.REJECT,
			},
			expectedAction: Ptr(settings.REJECT),
		},
		{
			name:     "Non-matched condition with REJECT action",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			condition: settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.non-existing-attr"},
				Action:     settings.REJECT,
			},
			expectedAction: nil,
		},
		{
			name:     "Support scope attributes",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			condition: settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "scopeAttributes.test-attr-scope"},
				Action:     settings.CREATE,
			},
			expectedAction: Ptr(settings.CREATE),
		},
		{
			name:     "Support resource attributes",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			condition: settings.OtelConditionMapping{
				Expression: settings.OtelBooleanExpression{Expression: "resourceAttributes.test-attr-resource"},
				Action:     settings.CREATE,
			},
			expectedAction: Ptr(settings.CREATE),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resultAction := evalCondition(tc.span, tc.scope, tc.resource, &tc.condition)
			assert.Equal(t, tc.expectedAction, resultAction)
		})
	}
}

func TestFilter_filterByConditions(t *testing.T) {
	t.Parallel()

	testSpan := ptrace.NewSpan()
	testSpan.Attributes().PutBool("test-attr", true)

	testScope := ptrace.NewScopeSpans()
	testScope.Scope().Attributes().PutBool("test-attr-scope", true)

	testResource := ptrace.NewResourceSpans()
	testResource.Resource().Attributes().PutBool("test-attr-resource", true)

	testCases := []struct {
		name       string
		span       *ptrace.Span
		scope      *ptrace.ScopeSpans
		resource   *ptrace.ResourceSpans
		conditions []settings.OtelConditionMapping
		expected   bool
	}{
		{
			name:       "Empty conditions list",
			span:       &testSpan,
			scope:      &testScope,
			resource:   &testResource,
			conditions: []settings.OtelConditionMapping{},
			expected:   false,
		},
		{
			name:     "Single CREATE condition - matched",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.test-attr"},
					Action:     settings.CREATE,
				},
			},
			expected: true,
		},
		{
			name:     "Single CREATE condition - not matched",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.non-existing-attr"},
					Action:     settings.CREATE,
				},
			},
			expected: false,
		},
		{
			name:     "Single REJECT condition - matched",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.test-attr"},
					Action:     settings.REJECT,
				},
			},
			expected: false,
		},
		{
			name:     "Single REJECT condition - not matched",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.non-existing-attr"},
					Action:     settings.REJECT,
				},
			},
			expected: false,
		},
		{
			name:     "CREATE matched then REJECT matched",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.test-attr"},
					Action:     settings.CREATE,
				},
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.test-attr"},
					Action:     settings.REJECT,
				},
			},
			expected: true,
		},
		{
			name:     "CREATE not matched then REJECT matched",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.non-existing-attr"},
					Action:     settings.CREATE,
				},
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.test-attr"},
					Action:     settings.REJECT,
				},
			},
			expected: false,
		},
		{
			name:     "REJECT matched then CREATE matched",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.test-attr"},
					Action:     settings.REJECT,
				},
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.test-attr"},
					Action:     settings.CREATE,
				},
			},
			expected: false,
		},
		{
			name:     "REJECT not matched then CREATE matched",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.non-existing-attr"},
					Action:     settings.REJECT,
				},
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.test-attr"},
					Action:     settings.CREATE,
				},
			},
			expected: true,
		},
				{
			name:     "REJECT not matched then CREATE not-matched",
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			conditions: []settings.OtelConditionMapping{
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.non-existing-attr"},
					Action:     settings.REJECT,
				},
				{
					Expression: settings.OtelBooleanExpression{Expression: "spanAttributes.non-existing-attr"},
					Action:     settings.CREATE,
				},
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := filterByConditions(tc.span, tc.scope, tc.resource, &tc.conditions)
			assert.Equal(t, tc.expected, result)
		})
	}
}
