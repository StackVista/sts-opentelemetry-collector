package internal

import (
	"context"
	"time"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
)

// MappingHandler evaluates mapping conditions and triggers mapping execution.
type MappingHandler[T settings.SettingExtension] struct {
	MappingCtx *MappingContext[T]

	// ExecuteMappingFunc is called to execute the mapping.
	// In production, points to MappingCtx.ExecuteMapping.
	// In tests, it can be overridden to record calls.
	ExecuteMappingFunc func(ctx context.Context, evalCtx *ExpressionEvalContext)
}

// NewMappingHandler creates a new handler. In production, ExecuteMappingFunc defaults to MappingCtx.ExecuteMapping
func NewMappingHandler[T settings.SettingExtension](ctx *MappingContext[T]) *MappingHandler[T] {
	h := &MappingHandler[T]{MappingCtx: ctx}
	h.ExecuteMappingFunc = ctx.ExecuteMapping
	return h
}

func (h *MappingHandler[T]) HandleVisitLevel(
	ctx context.Context,
	evalCtx *ExpressionEvalContext,
	action *settings.OtelInputConditionAction,
	condition *settings.OtelBooleanExpression,
) VisitResult {
	ok := h.evaluateCondition(evalCtx, condition)

	if !ok {
		return VisitSkip
	}

	// Default: CONTINUE if action not provided
	act := settings.CONTINUE
	if action != nil {
		act = *action
	}

	switch act {
	case settings.CREATE:
		h.ExecuteMappingFunc(ctx, evalCtx)
		return VisitSkip
	case settings.CONTINUE:
		return VisitContinue
	default:
		return VisitSkip
	}
}

// HandleTerminalVisit doesn't return a VisitResult
func (h *MappingHandler[T]) HandleTerminalVisit(
	ctx context.Context,
	evalCtx *ExpressionEvalContext,
	action *settings.OtelInputConditionAction,
	condition *settings.OtelBooleanExpression,
) {
	ok := h.evaluateCondition(evalCtx, condition)

	// Default: CONTINUE if action not provided
	act := settings.CONTINUE
	if action != nil {
		act = *action
	}

	if ok && act == settings.CREATE {
		h.ExecuteMappingFunc(ctx, evalCtx)
	}
}

// evaluateCondition checks a condition safely with defaults.
func (h *MappingHandler[T]) evaluateCondition(
	evalCtx *ExpressionEvalContext,
	condition *settings.OtelBooleanExpression,
) bool {
	// Default: true if condition not provided
	if condition == nil {
		return true
	}

	ok, err := h.MappingCtx.BaseCtx.Evaluator.EvalBooleanExpression(*condition, evalCtx)
	if err != nil {
		// On evaluation error, treat the condition as false
		return false
	}
	return ok
}

// BaseContext contains shared dependencies used by all mapping contexts.
type BaseContext struct {
	Signal              settings.OtelInputSignal
	Mapper              *Mapper
	Evaluator           ExpressionEvaluator
	CollectionTimestamp int64
	MetricsRecorder     metrics.ConnectorMetricsRecorder
	Results             *[]MessageWithKey
}

// MappingContext binds a specific mapping (component or relation) to its runtime (dependency) context.
type MappingContext[T settings.SettingExtension] struct {
	BaseCtx BaseContext
	Mapping T
}

// ExecuteMapping evaluates and executes a mapping of type T.
func (v *MappingContext[T]) ExecuteMapping(
	ctx context.Context,
	evalCtx *ExpressionEvalContext,
) {
	switch mapping := any(v.Mapping).(type) {
	case settings.OtelComponentMapping:
		v.handleComponent(ctx, evalCtx, mapping)
	case settings.OtelRelationMapping:
		v.handleRelation(ctx, evalCtx, mapping)
	default:
	}
}

func (v *MappingContext[T]) handleComponent(
	ctx context.Context,
	evalCtx *ExpressionEvalContext,
	mapping settings.OtelComponentMapping,
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
		v.BaseCtx.MetricsRecorder.IncTopologyProduced(ctx, 1, settings.SettingTypeOtelComponentMapping, v.BaseCtx.Signal)
	}
	if errs != nil {
		*v.BaseCtx.Results = append(
			*v.BaseCtx.Results,
			*ErrorsToMessageWithKey(&errs, mapping, v.BaseCtx.CollectionTimestamp),
		)
		v.BaseCtx.MetricsRecorder.IncMappingErrors(ctx, 1, settings.SettingTypeOtelComponentMapping, v.BaseCtx.Signal)
	}

	componentMappingDuration := time.Since(componentMappingStart)
	v.BaseCtx.MetricsRecorder.RecordMappingDuration(
		ctx,
		componentMappingDuration,
		v.BaseCtx.Signal,
		settings.SettingTypeOtelComponentMapping,
		v.Mapping.GetIdentifier(),
	)
}

func (v *MappingContext[T]) handleRelation(
	ctx context.Context,
	evalCtx *ExpressionEvalContext,
	mapping settings.OtelRelationMapping) {
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
		v.BaseCtx.MetricsRecorder.IncTopologyProduced(ctx, 1, settings.SettingTypeOtelRelationMapping, v.BaseCtx.Signal)
	}
	if errs != nil {
		*v.BaseCtx.Results = append(
			*v.BaseCtx.Results,
			*ErrorsToMessageWithKey(&errs, mapping, v.BaseCtx.CollectionTimestamp),
		)
		v.BaseCtx.MetricsRecorder.IncMappingErrors(ctx, 1, settings.SettingTypeOtelRelationMapping, v.BaseCtx.Signal)
	}
}

func convertToComponent(
	expressionEvaluator ExpressionEvaluator,
	mapper *Mapper,
	evalContext *ExpressionEvalContext,
	mapping *settings.OtelComponentMapping,
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
	mapping *settings.OtelRelationMapping,
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
