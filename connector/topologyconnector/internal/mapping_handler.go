package internal

import (
	"context"
	"fmt"
	"time"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/types"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"go.uber.org/zap"
)

// Action represents a resolved mapping action.
type Action string

// Action constants
const (
	ActionContinue Action = "CONTINUE"
	ActionCreate   Action = "CREATE"
	ActionDelete   Action = "DELETE"
)

// MappingHandler evaluates mapping conditions and triggers mapping execution.
type MappingHandler[T settingsproto.SettingExtension] struct {
	MappingCtx *MappingContext[T]

	// ExecuteMappingFunc is called to execute the mapping for a given action.
	// The action parameter is the resolved action string (e.g., ActionCreate, ActionDelete).
	// In tests, it can be overridden to record calls.
	ExecuteMappingFunc func(ctx context.Context, evalCtx *ExpressionEvalContext, action Action)
}

// NewMappingHandler creates a new handler. In production, ExecuteMappingFunc defaults to MappingCtx.ExecuteMapping
func NewMappingHandler[T settingsproto.SettingExtension](ctx *MappingContext[T]) *MappingHandler[T] {
	h := &MappingHandler[T]{MappingCtx: ctx}
	h.ExecuteMappingFunc = ctx.ExecuteMapping
	return h
}

func (h *MappingHandler[T]) HandleVisitLevel(
	ctx context.Context,
	evalCtx *ExpressionEvalContext,
	action *settingsproto.OtelStringExpression,
	condition *settingsproto.OtelBooleanExpression,
) VisitResult {
	ok := h.evaluateCondition(evalCtx, condition)

	if !ok {
		return VisitSkip
	}

	act, err := h.evaluateAction(evalCtx, action)
	if err != nil {
		return VisitSkip
	}

	switch act {
	case ActionCreate, ActionDelete:
		h.ExecuteMappingFunc(ctx, evalCtx, act)
		return VisitSkip
	case ActionContinue:
		return VisitContinue
	default:
		return VisitSkip
	}
}

// HandleTerminalVisit doesn't return a VisitResult.
// DELETE is only valid for the LOGS signal; for other signals it is silently ignored.
func (h *MappingHandler[T]) HandleTerminalVisit(
	ctx context.Context,
	evalCtx *ExpressionEvalContext,
	action *settingsproto.OtelStringExpression,
	condition *settingsproto.OtelBooleanExpression,
) {
	ok := h.evaluateCondition(evalCtx, condition)
	if !ok {
		return
	}

	act, err := h.evaluateAction(evalCtx, action)
	if err != nil {
		return
	}

	switch act {
	case ActionCreate:
		h.ExecuteMappingFunc(ctx, evalCtx, act)
	case ActionDelete:
		if h.MappingCtx.BaseCtx.Signal == settingsproto.LOGS {
			h.ExecuteMappingFunc(ctx, evalCtx, act)
		}
	case ActionContinue:
		// Terminal level with CONTINUE: nothing to do
	}
}

// evaluateAction evaluates an action expression and returns the resolved Action.
// Returns ActionContinue if action is nil (default).
func (h *MappingHandler[T]) evaluateAction(
	evalCtx *ExpressionEvalContext,
	action *settingsproto.OtelStringExpression,
) (Action, error) {
	if action == nil {
		return ActionContinue, nil
	}

	act, err := h.MappingCtx.BaseCtx.Evaluator.EvalStringExpression(*action, evalCtx)
	if err != nil {
		return "", fmt.Errorf("failed to evaluate action expression: %w", err)
	}

	switch Action(act) {
	case ActionContinue, ActionCreate, ActionDelete:
		return Action(act), nil
	default:
		return "", fmt.Errorf("unknown action value: %q, expected one of CONTINUE, CREATE, DELETE", act)
	}
}

// evaluateCondition checks a condition safely with defaults.
func (h *MappingHandler[T]) evaluateCondition(
	evalCtx *ExpressionEvalContext,
	condition *settingsproto.OtelBooleanExpression,
) bool {
	// Default: true if condition not provided
	if condition == nil {
		return true
	}

	ok, err := h.MappingCtx.BaseCtx.Evaluator.EvalBooleanExpression(*condition, evalCtx)
	if err != nil {
		h.MappingCtx.BaseCtx.Logger.Debug("Condition evaluation error (treated as false)",
			zap.String("mapping", h.MappingCtx.Mapping.GetIdentifier()),
			zap.String("condition", condition.Expression),
			zap.Error(err),
			h.evalCtxSummary(evalCtx),
		)
		return false
	}
	return ok
}

// evalCtxSummary returns a zap field summarizing what data is available in the eval context.
func (h *MappingHandler[T]) evalCtxSummary(evalCtx *ExpressionEvalContext) zap.Field {
	summary := make(map[string]any)
	if evalCtx.Log != nil {
		logMap := evalCtx.Log.ToMap()
		summary["log.eventName"] = logMap["eventName"]
		summary["log.attributes"] = logMap["attributes"]
	}
	if evalCtx.Resource != nil {
		summary["resource.attributes"] = evalCtx.Resource.ToMap()["attributes"]
	}
	return zap.Any("evalCtx", summary)
}

// BaseContext contains shared dependencies used by all mapping contexts.
type BaseContext struct {
	Logger                 *zap.Logger
	Signal                 settingsproto.OtelInputSignal
	Mapper                 *Mapper
	Evaluator              ExpressionEvaluator
	Deduplicator           Deduplicator
	ExpressionRefSummaries map[string]*types.ExpressionRefSummary
	CollectionTimestamp    int64
	MetricsRecorder        metrics.ConnectorMetricsRecorder
	Results                *[]MessageWithKey
}

// MappingContext binds a specific mapping (component or relation) to its runtime (dependency) context.
type MappingContext[T settingsproto.SettingExtension] struct {
	BaseCtx BaseContext
	Mapping T
}

// ExecuteMapping evaluates and executes a mapping of type T for the given action.
func (v *MappingContext[T]) ExecuteMapping(
	ctx context.Context,
	evalCtx *ExpressionEvalContext,
	action Action,
) {
	switch action {
	case ActionCreate:
		// Perform pre-mapping deduplication for CREATE
		send := v.BaseCtx.Deduplicator.ShouldSend(
			v.Mapping.GetIdentifier(), v.BaseCtx.Signal, evalCtx,
			time.Duration(v.Mapping.GetExpireAfterMs())*time.Millisecond,
		)
		if !send {
			return
		}

		switch mapping := any(v.Mapping).(type) {
		case settingsproto.OtelComponentMapping:
			v.handleComponent(ctx, evalCtx, mapping)
		case settingsproto.OtelRelationMapping:
			v.handleRelation(ctx, evalCtx, mapping)
		default:
		}
	case ActionDelete:
		switch mapping := any(v.Mapping).(type) {
		case settingsproto.OtelComponentMapping:
			v.handleComponentDelete(ctx, evalCtx, mapping)
		case settingsproto.OtelRelationMapping:
			v.handleRelationDelete(ctx, evalCtx, mapping)
		default:
		}
	}
}

func (v *MappingContext[T]) handleComponentDelete(
	ctx context.Context,
	evalCtx *ExpressionEvalContext,
	mapping settingsproto.OtelComponentMapping,
) {
	evaluatedVars, errs := EvalVariables(v.BaseCtx.Evaluator, evalCtx, mapping.Vars)
	if errs != nil {
		*v.BaseCtx.Results = append(
			*v.BaseCtx.Results,
			*ErrorsToMessageWithKey(&errs, mapping, v.BaseCtx.CollectionTimestamp),
		)
		v.BaseCtx.MetricsRecorder.IncMappingErrors(ctx, 1, settingsproto.SettingTypeOtelComponentMapping, v.BaseCtx.Signal)
		return
	}
	evalCtxWithVars := evalCtx.CloneWithVariables(evaluatedVars)

	externalId, err := v.BaseCtx.Evaluator.EvalStringExpression(mapping.Output.Identifier, evalCtxWithVars)
	if err != nil {
		errs := []error{err}
		*v.BaseCtx.Results = append(
			*v.BaseCtx.Results,
			*ErrorsToMessageWithKey(&errs, mapping, v.BaseCtx.CollectionTimestamp),
		)
		v.BaseCtx.MetricsRecorder.IncMappingErrors(ctx, 1, settingsproto.SettingTypeOtelComponentMapping, v.BaseCtx.Signal)
		return
	}

	*v.BaseCtx.Results = append(
		*v.BaseCtx.Results,
		*ComponentDeleteToMessageWithKey(externalId, mapping, v.BaseCtx.CollectionTimestamp),
	)
	v.BaseCtx.MetricsRecorder.IncTopologyProduced(ctx, 1, settingsproto.SettingTypeOtelComponentMapping, v.BaseCtx.Signal)
}

func (v *MappingContext[T]) handleRelationDelete(
	ctx context.Context,
	evalCtx *ExpressionEvalContext,
	mapping settingsproto.OtelRelationMapping,
) {
	evaluatedVars, errs := EvalVariables(v.BaseCtx.Evaluator, evalCtx, mapping.Vars)
	if errs != nil {
		*v.BaseCtx.Results = append(
			*v.BaseCtx.Results,
			*ErrorsToMessageWithKey(&errs, mapping, v.BaseCtx.CollectionTimestamp),
		)
		v.BaseCtx.MetricsRecorder.IncMappingErrors(ctx, 1, settingsproto.SettingTypeOtelRelationMapping, v.BaseCtx.Signal)
		return
	}
	evalCtxWithVars := evalCtx.CloneWithVariables(evaluatedVars)

	sourceId, sourceErr := v.BaseCtx.Evaluator.EvalStringExpression(mapping.Output.SourceId, evalCtxWithVars)
	targetId, targetErr := v.BaseCtx.Evaluator.EvalStringExpression(mapping.Output.TargetId, evalCtxWithVars)
	if sourceErr != nil || targetErr != nil {
		evalErrs := make([]error, 0, 2)
		if sourceErr != nil {
			evalErrs = append(evalErrs, sourceErr)
		}
		if targetErr != nil {
			evalErrs = append(evalErrs, targetErr)
		}
		*v.BaseCtx.Results = append(
			*v.BaseCtx.Results,
			*ErrorsToMessageWithKey(&evalErrs, mapping, v.BaseCtx.CollectionTimestamp),
		)
		v.BaseCtx.MetricsRecorder.IncMappingErrors(ctx, 1, settingsproto.SettingTypeOtelRelationMapping, v.BaseCtx.Signal)
		return
	}

	externalId := sourceId + "-" + targetId
	*v.BaseCtx.Results = append(
		*v.BaseCtx.Results,
		*RelationDeleteToMessageWithKey(externalId, mapping, v.BaseCtx.CollectionTimestamp),
	)
	v.BaseCtx.MetricsRecorder.IncTopologyProduced(ctx, 1, settingsproto.SettingTypeOtelRelationMapping, v.BaseCtx.Signal)
}

func (v *MappingContext[T]) handleComponent(
	ctx context.Context,
	evalCtx *ExpressionEvalContext,
	mapping settingsproto.OtelComponentMapping,
) {
	componentMappingStart := time.Now()

	component, errs := convertToComponent(v.BaseCtx.Evaluator, v.BaseCtx.Mapper, evalCtx, &mapping)
	if component != nil {
		*v.BaseCtx.Results = append(
			*v.BaseCtx.Results,
			*OutputToMessageWithKey(
				component, mapping, v.BaseCtx.CollectionTimestamp,
				func() []*topostreamv1.TopologyStreamComponent {
					return []*topostreamv1.TopologyStreamComponent{component}
				},
				func() []*topostreamv1.TopologyStreamRelation {
					return nil
				},
			),
		)
		v.BaseCtx.MetricsRecorder.IncTopologyProduced(ctx, 1, settingsproto.SettingTypeOtelComponentMapping, v.BaseCtx.Signal)
	}
	if errs != nil {
		*v.BaseCtx.Results = append(
			*v.BaseCtx.Results,
			*ErrorsToMessageWithKey(&errs, mapping, v.BaseCtx.CollectionTimestamp),
		)
		v.BaseCtx.MetricsRecorder.IncMappingErrors(ctx, 1, settingsproto.SettingTypeOtelComponentMapping, v.BaseCtx.Signal)
	}

	componentMappingDuration := time.Since(componentMappingStart)
	v.BaseCtx.MetricsRecorder.RecordMappingDuration(
		ctx,
		componentMappingDuration,
		v.BaseCtx.Signal,
		settingsproto.SettingTypeOtelComponentMapping,
		v.Mapping.GetIdentifier(),
	)
}

func (v *MappingContext[T]) handleRelation(
	ctx context.Context,
	evalCtx *ExpressionEvalContext,
	mapping settingsproto.OtelRelationMapping,
) {
	relation, errs := convertToRelation(v.BaseCtx.Evaluator, v.BaseCtx.Mapper, evalCtx, &mapping)
	if relation != nil {
		*v.BaseCtx.Results = append(
			*v.BaseCtx.Results,
			*OutputToMessageWithKey(
				relation, mapping, v.BaseCtx.CollectionTimestamp,
				func() []*topostreamv1.TopologyStreamComponent { return nil },
				func() []*topostreamv1.TopologyStreamRelation { return []*topostreamv1.TopologyStreamRelation{relation} },
			),
		)
		v.BaseCtx.MetricsRecorder.IncTopologyProduced(ctx, 1, settingsproto.SettingTypeOtelRelationMapping, v.BaseCtx.Signal)
	}
	if errs != nil {
		*v.BaseCtx.Results = append(
			*v.BaseCtx.Results,
			*ErrorsToMessageWithKey(&errs, mapping, v.BaseCtx.CollectionTimestamp),
		)
		v.BaseCtx.MetricsRecorder.IncMappingErrors(ctx, 1, settingsproto.SettingTypeOtelRelationMapping, v.BaseCtx.Signal)
	}
}

func convertToComponent(
	expressionEvaluator ExpressionEvaluator,
	mapper *Mapper,
	evalContext *ExpressionEvalContext,
	mapping *settingsproto.OtelComponentMapping,
) (*topostreamv1.TopologyStreamComponent, []error) {
	// at this point, we've done the condition filtering and can proceed to evaluate variables
	evaluatedVars, errs := EvalVariables(expressionEvaluator, evalContext, mapping.Vars)
	if errs != nil {
		return nil, errs
	}
	evalContextWithVars := evalContext.CloneWithVariables(evaluatedVars)

	component, err := mapper.MapComponent(mapping, expressionEvaluator, evalContextWithVars)
	if len(err) > 0 {
		return nil, err
	}
	return component, nil
}

func convertToRelation(
	expressionEvaluator ExpressionEvaluator,
	mapper *Mapper,
	evalContext *ExpressionEvalContext,
	mapping *settingsproto.OtelRelationMapping,
) (*topostreamv1.TopologyStreamRelation, []error) {
	// at this point, we've done the condition filtering and can proceed to evaluate variables
	evaluatedVars, errs := EvalVariables(expressionEvaluator, evalContext, mapping.Vars)
	if errs != nil {
		return nil, errs
	}
	evalContextWithVars := evalContext.CloneWithVariables(evaluatedVars)

	relation, err := mapper.MapRelation(mapping, expressionEvaluator, evalContextWithVars)
	if len(err) > 0 {
		return nil, err
	}
	return relation, nil
}
