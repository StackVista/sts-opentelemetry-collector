package internal

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"time"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

// ShardCount is the number of shards to use for the topology stream.
const ShardCount = 4

type MessageWithKey struct {
	Key     *topostreamv1.TopologyStreamMessageKey
	Message *topostreamv1.TopologyStreamMessage
}

func ConvertSpanToTopologyStreamMessage(
	ctx context.Context,
	logger *zap.Logger,
	eval ExpressionEvaluator,
	mapper *Mapper,
	trace ptrace.Traces,
	componentMappings []settings.OtelComponentMapping,
	relationMappings []settings.OtelRelationMapping,
	collectionTimestampMs int64,
	metricsRecorder metrics.ConnectorMetricsRecorder,
) []MessageWithKey {
	result := make([]MessageWithKey, 0)
	var components, relations, componentErrs, relationErrs int
	var componentMappingStart, relationMappingStart time.Time
	var componentMappingDuration, relationMappingDuration time.Duration

	iterateSpans(trace, func(expressionEvalContext *ExpressionEvalContext, span *ptrace.Span) {
		componentMappingStart = time.Now()
		for _, componentMapping := range componentMappings {
			currentComponentMapping := componentMapping
			component, errs := convertToComponent(eval, mapper, expressionEvalContext, &currentComponentMapping)
			if component != nil {
				result = append(result, *outputToMessageWithKey(
					component,
					componentMapping,
					span,
					collectionTimestampMs,
					func() []*topostreamv1.TopologyStreamComponent {
						return []*topostreamv1.TopologyStreamComponent{component}
					},
					func() []*topostreamv1.TopologyStreamRelation {
						return nil
					}),
				)
				components++
			}
			if errs != nil {
				result = append(result, *errorsToMessageWithKey(&errs, componentMapping, span, collectionTimestampMs))
				componentErrs++
			}
		}
		componentMappingDuration = time.Since(componentMappingStart)

		relationMappingStart = time.Now()
		for _, relationMapping := range relationMappings {
			currentRelationMapping := relationMapping
			relation, errs := convertToRelation(eval, mapper, expressionEvalContext, &currentRelationMapping)
			if relation != nil {
				result = append(result, *outputToMessageWithKey(
					relation,
					relationMapping,
					span,
					collectionTimestampMs,
					func() []*topostreamv1.TopologyStreamComponent {
						return nil
					},
					func() []*topostreamv1.TopologyStreamRelation {
						return []*topostreamv1.TopologyStreamRelation{relation}
					}),
				)
				relations++
			}
			if errs != nil {
				result = append(result, *errorsToMessageWithKey(&errs, relationMapping, span, collectionTimestampMs))
				relationErrs++
			}
		}
		relationMappingDuration = time.Since(relationMappingStart)
	})

	// Record metrics
	metricsRecorder.IncSpansProcessed(ctx, int64(trace.SpanCount()))
	metricsRecorder.IncComponentsProduced(ctx, int64(components), attribute.String("inputSignal", "spans"))
	metricsRecorder.IncRelationsProduced(ctx, int64(relations), attribute.String("inputSignal", "spans"))
	if componentErrs > 0 {
		metricsRecorder.IncErrors(ctx, int64(componentErrs+relationErrs), "mapping")
	}
	metricsRecorder.RecordMappingDuration(
		ctx, componentMappingDuration,
		attribute.String("phase", "convert_span_to_topology_stream_message"),
		attribute.String("target", "components"),
		attribute.Int("mapping_count", len(componentMappings)),
	)
	metricsRecorder.RecordMappingDuration(
		ctx, relationMappingDuration,
		attribute.String("phase", "convert_span_to_topology_stream_message"),
		attribute.String("target", "relations"),
		attribute.Int("mapping_count", len(relationMappings)),
	)

	logger.Debug(
		"Converted spans to topology stream messages",
		zap.Int("components", components),
		zap.Int("relations", relations),
		zap.Int("componentErrs", componentErrs),
		zap.Int("relationErrors", relationErrs),
	)

	return result
}

func convertToComponent(
	expressionEvaluator ExpressionEvaluator,
	mapper *Mapper,
	evalContext *ExpressionEvalContext,
	mapping *settings.OtelComponentMapping,
) (*topostreamv1.TopologyStreamComponent, []error) {
	evaluatedVars, errs := EvalVariables(expressionEvaluator, evalContext, mapping.Vars)
	if errs != nil {
		return nil, errs
	}

	evalContextWithVars := evalContext.CloneWithVariables(evaluatedVars)

	if filterByConditions(expressionEvaluator, evalContextWithVars, &mapping.Conditions) {
		component, err := mapper.MapComponent(mapping, expressionEvaluator, evalContextWithVars)
		if len(err) > 0 {
			return nil, err
		}
		return component, nil
	}
	return nil, nil
}

func convertToRelation(
	expressionEvaluator ExpressionEvaluator,
	mapper *Mapper,
	evalContext *ExpressionEvalContext,
	mapping *settings.OtelRelationMapping,
) (*topostreamv1.TopologyStreamRelation, []error) {
	evaluatedVars, errs := EvalVariables(expressionEvaluator, evalContext, mapping.Vars)
	if errs != nil {
		return nil, errs
	}
	evalContextWithVars := evalContext.CloneWithVariables(evaluatedVars)

	if filterByConditions(expressionEvaluator, evalContextWithVars, &mapping.Conditions) {
		relation, err := mapper.MapRelation(mapping, expressionEvaluator, evalContextWithVars)
		if len(err) > 0 {
			return nil, err
		}
		return relation, nil
	}
	return nil, nil
}

type mappingHandler func(expressionEvalContext *ExpressionEvalContext, span *ptrace.Span)

func iterateSpans(trace ptrace.Traces, handler mappingHandler) {
	resourceSpans := trace.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		resourceAttributes := rs.Resource().Attributes().AsRaw()
		scopeSpans := rs.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			ss := scopeSpans.At(j)
			scopeAttributes := ss.Scope().Attributes().AsRaw()
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				spanAttributes := span.Attributes().AsRaw()
				handler(NewEvalContext(spanAttributes, scopeAttributes, resourceAttributes), &span)
			}
		}
	}
}

func errorsToMessageWithKey(
	errs *[]error,
	mapping settings.Mapping,
	span *ptrace.Span,
	collectionTimestampMs int64,
) *MessageWithKey {
	streamErrors := make([]*topostreamv1.TopoStreamError, len(*errs))
	for i, err := range *errs {
		streamErrors[i] = &topostreamv1.TopoStreamError{
			Message: err.Error(),
		}
	}
	return &MessageWithKey{
		Key: &topostreamv1.TopologyStreamMessageKey{
			Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: mapping.GetIdentifier(),
			ShardId:    "unknown",
		},
		Message: &topostreamv1.TopologyStreamMessage{
			CollectionTimestamp: collectionTimestampMs,
			SubmittedTimestamp:  convertTimestampToInt64(span.EndTimestamp()),
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
					ExpiryIntervalMs: mapping.GetExpireAfterMs(),
					Errors:           streamErrors,
				},
			},
		},
	}
}

func outputToMessageWithKey(
	output topostreamv1.ComponentOrRelation,
	mapping settings.Mapping,
	span *ptrace.Span,
	collectionTimestampMs int64,
	toComponents func() []*topostreamv1.TopologyStreamComponent,
	toRelations func() []*topostreamv1.TopologyStreamRelation,
) *MessageWithKey {
	submittedTimestamp := convertTimestampToInt64(span.EndTimestamp())
	return &MessageWithKey{
		Key: &topostreamv1.TopologyStreamMessageKey{
			Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: mapping.GetIdentifier(),
			ShardId:    stableShardID(output.GetExternalId(), ShardCount),
		},
		Message: &topostreamv1.TopologyStreamMessage{
			CollectionTimestamp: collectionTimestampMs,
			SubmittedTimestamp:  submittedTimestamp,
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
					ExpiryIntervalMs: mapping.GetExpireAfterMs(),
					Components:       toComponents(),
					Relations:        toRelations(),
				},
			},
		},
	}
}

func stableShardID(shardKey string, shardCount uint32) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(shardKey))
	return fmt.Sprintf("%d", h.Sum32()%shardCount)
}

func convertTimestampToInt64(input pcommon.Timestamp) int64 {
	actualU64 := uint64(input)
	if actualU64 > math.MaxInt64 {
		return 0
	}
	return int64(actualU64)
}
