package internal

import (
	"context"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
)

type VisitResult int

const (
	VisitContinue VisitResult = iota // traverse deeper, go to the next level in the input hierarchy
	VisitSkip                        // skip subtree, jump to next iteration on same level (if applicable)
)

// MappingVisitor defines a visitor interface for traversing OpenTelemetry input data
// (metricData, traces, and eventually logs) in a consistent way.
// Each Visit* method corresponds to a level in the OTEL data hierarchy, allowing
// a mapping implementation to evaluate conditions and decide whether to:
//
//   - continue: Traverse deeper into the next level of the hierarchy.
//   - create:   Produce topology data (i.e., components or relations), which will then:
//   - skip:     Stop traversal for the current branch.
//
// The visitor is applied by signal-specific traversers (e.g., MetricsTraverser, TracesTraverser)
//
// Visit* methods receive an ExpressionEvalContext containing the available Attributes
// for that level (resource, scope, metric/span, etc.) and can evaluate CEL conditions
// to determine if a mapping applies.
//
// Example traversal order:
//
//	Metrics: Resource -> Scope -> Metric -> Datapoint
//	Traces:  Resource -> Scope -> Span
type MappingVisitor interface {
	VisitResource(ctx context.Context, evalCtx *ExpressionEvalContext) VisitResult
	VisitScope(ctx context.Context, evalCtx *ExpressionEvalContext) VisitResult

	VisitMetric(ctx context.Context, evalCtx *ExpressionEvalContext) VisitResult
	VisitDatapoint(ctx context.Context, evalCtx *ExpressionEvalContext)
	VisitSpan(ctx context.Context, evalCtx *ExpressionEvalContext)
}

type GenericMappingVisitor[T settings.SettingExtension] struct {
	handler    *MappingHandler[T]
	mappingCtx *MappingContext[T]
}

func NewGenericMappingVisitor[T settings.SettingExtension](
	mappingCtx *MappingContext[T],
) *GenericMappingVisitor[T] {
	return &GenericMappingVisitor[T]{
		handler:    NewMappingHandler[T](mappingCtx),
		mappingCtx: mappingCtx,
	}
}

func (v *GenericMappingVisitor[T]) VisitResource(ctx context.Context, evalCtx *ExpressionEvalContext) VisitResult {
	resourceInput := v.mappingCtx.Mapping.GetInput().Resource
	return v.handler.HandleVisitLevel(ctx, evalCtx, resourceInput.Action, resourceInput.Condition)
}

func (v *GenericMappingVisitor[T]) VisitScope(ctx context.Context, evalCtx *ExpressionEvalContext) VisitResult {
	scopeInput := v.mappingCtx.Mapping.GetInput().Resource.Scope
	if scopeInput == nil {
		return VisitSkip
	}
	return v.handler.HandleVisitLevel(ctx, evalCtx, scopeInput.Action, scopeInput.Condition)
}

func (v *GenericMappingVisitor[T]) VisitMetric(ctx context.Context, evalCtx *ExpressionEvalContext) VisitResult {
	scopeInput := v.mappingCtx.Mapping.GetInput().Resource.Scope
	if scopeInput == nil || scopeInput.Metric == nil {
		return VisitSkip
	}
	metricInput := scopeInput.Metric
	return v.handler.HandleVisitLevel(ctx, evalCtx, metricInput.Action, metricInput.Condition)
}

func (v *GenericMappingVisitor[T]) VisitDatapoint(ctx context.Context, evalCtx *ExpressionEvalContext) {
	scopeInput := v.mappingCtx.Mapping.GetInput().Resource.Scope
	if scopeInput == nil || scopeInput.Metric == nil || scopeInput.Metric.Datapoint == nil {
		return
	}
	datapointInput := scopeInput.Metric.Datapoint
	v.handler.HandleTerminalVisit(ctx, evalCtx, datapointInput.Action, datapointInput.Condition)
}

func (v *GenericMappingVisitor[T]) VisitSpan(ctx context.Context, evalCtx *ExpressionEvalContext) {
	scopeInput := v.mappingCtx.Mapping.GetInput().Resource.Scope
	if scopeInput == nil || scopeInput.Span == nil {
		return
	}
	spanInput := scopeInput.Span
	v.handler.HandleTerminalVisit(ctx, evalCtx, spanInput.Action, spanInput.Condition)
}
