package internal_test

import (
	"context"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/stretchr/testify/assert"
)

type mockEvaluator struct {
	result bool
	err    error
}

func (m *mockEvaluator) EvalBooleanExpression(_ settingsproto.OtelBooleanExpression, _ *internal.ExpressionEvalContext) (bool, error) {
	return m.result, m.err
}
func (m *mockEvaluator) EvalStringExpression(_ settingsproto.OtelStringExpression, _ *internal.ExpressionEvalContext) (string, error) {
	return "", nil
}
func (m *mockEvaluator) EvalOptionalStringExpression(_ *settingsproto.OtelStringExpression, _ *internal.ExpressionEvalContext) (*string, error) {
	//nolint:nilnil
	return nil, nil
}
func (m *mockEvaluator) EvalMapExpression(_ settingsproto.OtelAnyExpression, _ *internal.ExpressionEvalContext) (map[string]any, error) {
	//nolint:nilnil
	return nil, nil
}
func (m *mockEvaluator) EvalAnyExpression(_ settingsproto.OtelAnyExpression, _ *internal.ExpressionEvalContext) (any, error) {
	//nolint:nilnil
	return nil, nil
}

func (m *mockEvaluator) GetStringExpressionAST(_ settingsproto.OtelStringExpression) (*internal.GetASTResult, error) {
	//nolint:nilnil
	return nil, nil
}

func (m *mockEvaluator) GetBooleanExpressionAST(_ settingsproto.OtelBooleanExpression) (*internal.GetASTResult, error) {
	//nolint:nilnil
	return nil, nil
}

func (m *mockEvaluator) GetMapExpressionAST(_ settingsproto.OtelAnyExpression) (*internal.GetASTResult, error) {
	//nolint:nilnil
	return nil, nil
}

func (m *mockEvaluator) GetAnyExpressionAST(_ settingsproto.OtelAnyExpression) (*internal.GetASTResult, error) {
	//nolint:nilnil
	return nil, nil
}

func makeHandler(t *testing.T, eval *mockEvaluator) (*internal.MappingHandler[settingsproto.OtelComponentMapping], *bool) {
	t.Helper()
	executed := false

	mockBaseCtx := internal.BaseContext{
		Signal:              settingsproto.TRACES,
		Evaluator:           eval,
		CollectionTimestamp: time.Now().UnixMilli(),
		Results:             &[]internal.MessageWithKey{},
		MetricsRecorder:     &metrics.NoopConnectorMetricsRecorder{},
	}

	mockMappingCtx := &internal.MappingContext[settingsproto.OtelComponentMapping]{
		BaseCtx: mockBaseCtx,
		Mapping: settingsproto.OtelComponentMapping{}, // empty dummy mapping
	}

	handler := internal.NewMappingHandler(mockMappingCtx)
	handler.ExecuteMappingFunc = func(_ context.Context, _ *internal.ExpressionEvalContext) {
		executed = true
	}

	return handler, &executed
}

func TestHandleVisitLevel_Behavior(t *testing.T) {
	ctx := context.Background()
	evalCtx := &internal.ExpressionEvalContext{}

	tests := []struct {
		name           string
		evalResult     bool
		evalErr        error
		action         *settingsproto.OtelInputConditionAction
		condition      *settingsproto.OtelBooleanExpression
		expectedResult internal.VisitResult
		expectExecuted bool
	}{
		{
			name:           "nil condition + nil action => defaults to CONTINUE (VisitContinue), no execute",
			expectedResult: internal.VisitContinue,
		},
		{
			name:           "true condition + CONTINUE => VisitContinue, no execute",
			evalResult:     true,
			action:         ptr(settingsproto.CONTINUE),
			expectedResult: internal.VisitContinue,
		},
		{
			name:           "true condition + CREATE => VisitSkip and execute",
			evalResult:     true,
			action:         ptr(settingsproto.CREATE),
			condition:      ptr(settingsproto.OtelBooleanExpression{Expression: `true`}),
			expectedResult: internal.VisitSkip,
			expectExecuted: true,
		},
		{
			name:           "nil condition + CREATE => VisitSkip and execute",
			action:         ptr(settingsproto.CREATE),
			expectedResult: internal.VisitSkip,
			expectExecuted: true,
		},
		{
			name:           "false condition + CREATE => VisitSkip, no execute",
			evalResult:     false,
			action:         ptr(settingsproto.CREATE),
			condition:      ptr(settingsproto.OtelBooleanExpression{Expression: `false`}),
			expectedResult: internal.VisitSkip,
		},
		{
			name:           "error during condition => VisitSkip, no execute",
			evalErr:        assert.AnError,
			action:         ptr(settingsproto.CREATE),
			condition:      ptr(settingsproto.OtelBooleanExpression{}),
			expectedResult: internal.VisitSkip,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockEval := &mockEvaluator{result: tt.evalResult, err: tt.evalErr}
			handler, executed := makeHandler(t, mockEval)

			res := handler.HandleVisitLevel(ctx, evalCtx, tt.action, tt.condition)

			assert.Equal(t, tt.expectedResult, res)
			assert.Equal(t, tt.expectExecuted, *executed)
		})
	}
}

func TestHandleTerminalVisit_Behavior(t *testing.T) {
	ctx := context.Background()
	evalCtx := &internal.ExpressionEvalContext{}

	tests := []struct {
		name           string
		evalResult     bool
		evalErr        error
		action         *settingsproto.OtelInputConditionAction
		condition      *settingsproto.OtelBooleanExpression
		expectExecuted bool
	}{
		{
			name:           "nil condition + nil action => no execute",
			expectExecuted: false,
		},
		{
			name:           "nil condition + CREATE => executes",
			action:         ptr(settingsproto.CREATE),
			expectExecuted: true,
		},
		{
			name:           "true condition + CREATE => executes",
			evalResult:     true,
			action:         ptr(settingsproto.CREATE),
			condition:      ptr(settingsproto.OtelBooleanExpression{Expression: `true`}),
			expectExecuted: true,
		},
		{
			name:           "false condition + CREATE => no execute",
			evalResult:     false,
			action:         ptr(settingsproto.CREATE),
			condition:      ptr(settingsproto.OtelBooleanExpression{Expression: `false`}),
			expectExecuted: false,
		},
		{
			name:           "error evaluating => no execute",
			evalErr:        assert.AnError,
			action:         ptr(settingsproto.CREATE),
			condition:      ptr(settingsproto.OtelBooleanExpression{}),
			expectExecuted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockEval := &mockEvaluator{result: tt.evalResult, err: tt.evalErr}
			handler, executed := makeHandler(t, mockEval)

			handler.HandleTerminalVisit(ctx, evalCtx, tt.action, tt.condition)

			assert.Equal(t, tt.expectExecuted, *executed)
		})
	}
}

func ptr[T any](v T) *T { return &v }
