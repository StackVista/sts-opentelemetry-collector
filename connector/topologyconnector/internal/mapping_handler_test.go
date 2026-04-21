package internal_test

import (
	"context"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type mockEvaluator struct {
	boolResult bool
	boolErr    error
	// stringResult is typed as Action for convenience in tests; converted to string at return site.
	stringResult internal.Action
	stringErr    error
}

func (m *mockEvaluator) EvalBooleanExpression(_ settingsproto.OtelBooleanExpression, _ *internal.ExpressionEvalContext) (bool, error) {
	return m.boolResult, m.boolErr
}
func (m *mockEvaluator) EvalStringExpression(_ settingsproto.OtelStringExpression, _ *internal.ExpressionEvalContext) (string, error) {
	return string(m.stringResult), m.stringErr
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

func makeHandler(t *testing.T, eval *mockEvaluator, signal ...settingsproto.OtelInputSignal) (*internal.MappingHandler[settingsproto.OtelComponentMapping], *internal.Action) {
	t.Helper()
	var executedAction internal.Action

	sig := settingsproto.TRACES
	if len(signal) > 0 {
		sig = signal[0]
	}

	mockBaseCtx := internal.BaseContext{
		Logger:              zap.NewNop(),
		Signal:              sig,
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
	handler.ExecuteMappingFunc = func(_ context.Context, _ *internal.ExpressionEvalContext, action internal.Action) {
		executedAction = action
	}

	return handler, &executedAction
}

func actionExpr(expr string) *settingsproto.OtelStringExpression {
	return &settingsproto.OtelStringExpression{Expression: expr}
}

func TestHandleVisitLevel_Behavior(t *testing.T) {
	ctx := context.Background()
	evalCtx := &internal.ExpressionEvalContext{}

	tests := []struct {
		name           string
		boolResult     bool
		boolErr        error
		stringResult   internal.Action
		stringErr      error
		action         *settingsproto.OtelStringExpression
		condition      *settingsproto.OtelBooleanExpression
		expectedResult internal.VisitResult
		expectedAction internal.Action
	}{
		{
			name:           "nil condition + nil action => defaults to CONTINUE (VisitContinue), no execute",
			stringResult:   internal.ActionContinue,
			expectedResult: internal.VisitContinue,
		},
		{
			name:           "true condition + CONTINUE => VisitContinue, no execute",
			boolResult:     true,
			stringResult:   internal.ActionContinue,
			action:         actionExpr("'CONTINUE'"),
			expectedResult: internal.VisitContinue,
		},
		{
			name:           "true condition + CREATE => VisitSkip and execute CREATE",
			boolResult:     true,
			stringResult:   internal.ActionCreate,
			action:         actionExpr("'CREATE'"),
			condition:      ptr(settingsproto.OtelBooleanExpression{Expression: `true`}),
			expectedResult: internal.VisitSkip,
			expectedAction: internal.ActionCreate,
		},
		{
			name:           "nil condition + CREATE => VisitSkip and execute CREATE",
			stringResult:   internal.ActionCreate,
			action:         actionExpr("'CREATE'"),
			expectedResult: internal.VisitSkip,
			expectedAction: internal.ActionCreate,
		},
		{
			name:           "false condition + CREATE => VisitSkip, no execute",
			boolResult:     false,
			stringResult:   internal.ActionCreate,
			action:         actionExpr("'CREATE'"),
			condition:      ptr(settingsproto.OtelBooleanExpression{Expression: `false`}),
			expectedResult: internal.VisitSkip,
		},
		{
			name:           "error during condition => VisitSkip, no execute",
			boolErr:        assert.AnError,
			stringResult:   internal.ActionCreate,
			action:         actionExpr("'CREATE'"),
			condition:      ptr(settingsproto.OtelBooleanExpression{}),
			expectedResult: internal.VisitSkip,
		},
		{
			name:           "DELETE at non-terminal level => VisitSkip and execute DELETE",
			boolResult:     true,
			stringResult:   internal.ActionDelete,
			action:         actionExpr("'DELETE'"),
			condition:      ptr(settingsproto.OtelBooleanExpression{Expression: `true`}),
			expectedResult: internal.VisitSkip,
			expectedAction: internal.ActionDelete,
		},
		{
			name:           "action eval error => VisitSkip, no execute",
			boolResult:     true,
			stringErr:      assert.AnError,
			action:         actionExpr("invalid"),
			condition:      ptr(settingsproto.OtelBooleanExpression{Expression: `true`}),
			expectedResult: internal.VisitSkip,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockEval := &mockEvaluator{boolResult: tt.boolResult, boolErr: tt.boolErr, stringResult: tt.stringResult, stringErr: tt.stringErr}
			handler, executedAction := makeHandler(t, mockEval)

			res := handler.HandleVisitLevel(ctx, evalCtx, tt.action, tt.condition)

			assert.Equal(t, tt.expectedResult, res)
			assert.Equal(t, tt.expectedAction, *executedAction)
		})
	}
}

func TestHandleTerminalVisit_Behavior(t *testing.T) {
	ctx := context.Background()
	evalCtx := &internal.ExpressionEvalContext{}

	tests := []struct {
		name           string
		boolResult     bool
		boolErr        error
		stringResult   internal.Action
		stringErr      error
		action         *settingsproto.OtelStringExpression
		condition      *settingsproto.OtelBooleanExpression
		signal         settingsproto.OtelInputSignal
		expectedAction internal.Action
	}{
		{
			name:         "nil condition + nil action => no execute",
			stringResult: internal.ActionContinue,
		},
		{
			name:           "nil condition + CREATE => executes CREATE",
			stringResult:   internal.ActionCreate,
			action:         actionExpr("'CREATE'"),
			expectedAction: internal.ActionCreate,
		},
		{
			name:           "true condition + CREATE => executes CREATE",
			boolResult:     true,
			stringResult:   internal.ActionCreate,
			action:         actionExpr("'CREATE'"),
			condition:      ptr(settingsproto.OtelBooleanExpression{Expression: `true`}),
			expectedAction: internal.ActionCreate,
		},
		{
			name:         "false condition + CREATE => no execute",
			boolResult:   false,
			stringResult: internal.ActionCreate,
			action:       actionExpr("'CREATE'"),
			condition:    ptr(settingsproto.OtelBooleanExpression{Expression: `false`}),
		},
		{
			name:         "error evaluating condition => no execute",
			boolErr:      assert.AnError,
			stringResult: internal.ActionCreate,
			action:       actionExpr("'CREATE'"),
			condition:    ptr(settingsproto.OtelBooleanExpression{}),
		},
		{
			name:           "DELETE + LOGS signal => executes DELETE",
			boolResult:     true,
			stringResult:   internal.ActionDelete,
			action:         actionExpr("'DELETE'"),
			condition:      ptr(settingsproto.OtelBooleanExpression{Expression: `true`}),
			signal:         settingsproto.LOGS,
			expectedAction: internal.ActionDelete,
		},
		{
			name:         "DELETE + TRACES signal => no execute",
			boolResult:   true,
			stringResult: internal.ActionDelete,
			action:       actionExpr("'DELETE'"),
			condition:    ptr(settingsproto.OtelBooleanExpression{Expression: `true`}),
			signal:       settingsproto.TRACES,
		},
		{
			name:       "action eval error => no execute",
			boolResult: true,
			stringErr:  assert.AnError,
			action:     actionExpr("invalid"),
			condition:  ptr(settingsproto.OtelBooleanExpression{Expression: `true`}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockEval := &mockEvaluator{boolResult: tt.boolResult, boolErr: tt.boolErr, stringResult: tt.stringResult, stringErr: tt.stringErr}
			handler, executedAction := makeHandler(t, mockEval, tt.signal)

			handler.HandleTerminalVisit(ctx, evalCtx, tt.action, tt.condition)

			assert.Equal(t, tt.expectedAction, *executedAction)
		})
	}
}

func ptr[T any](v T) *T { return &v }
