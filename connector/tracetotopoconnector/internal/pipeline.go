package internal

import (
	"fmt"
	"hash/fnv"
	"math"

	topo_stream_v1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// ShardCount is the number of shards to use for the topology stream.
const ShardCount = 4

type MessageWithKey struct {
	Key     *topo_stream_v1.TopologyStreamMessageKey
	Message *topo_stream_v1.TopologyStreamMessage
}

func ConvertSpanToTopologyStreamMessage(
	logger *zap.Logger,
	eval ExpressionEvaluator,
	mapper *Mapper,
	trace ptrace.Traces,
	componentMappings []settings.OtelComponentMapping,
	relationMappings []settings.OtelRelationMapping,
	now int64,
) []MessageWithKey {
	result := make([]MessageWithKey, 0)
	var components, relations, errors int

	iterateSpans(trace, func(expressionEvalContext *ExpressionEvalContext, span *ptrace.Span) {
		for _, componentMapping := range componentMappings {
			currentComponentMapping := componentMapping
			component, errs := convertToComponent(eval, mapper, expressionEvalContext, &currentComponentMapping)
			if component != nil {
				result = append(result, *outputToMessageWithKey(
					component,
					componentMapping,
					span,
					now,
					func() []*topo_stream_v1.TopologyStreamComponent {
						return []*topo_stream_v1.TopologyStreamComponent{component}
					},
					func() []*topo_stream_v1.TopologyStreamRelation {
						return nil
					}),
				)
				components++
			}
			if errs != nil {
				result = append(result, *errorsToMessageWithKey(&errs, componentMapping, span, now))
				errors++
			}
		}
		for _, relationMapping := range relationMappings {
			currentRelationMapping := relationMapping
			relation, errs := convertToRelation(eval, mapper, expressionEvalContext, &currentRelationMapping)
			if relation != nil {
				result = append(result, *outputToMessageWithKey(
					relation,
					relationMapping,
					span,
					now,
					func() []*topo_stream_v1.TopologyStreamComponent {
						return nil
					},
					func() []*topo_stream_v1.TopologyStreamRelation {
						return []*topo_stream_v1.TopologyStreamRelation{relation}
					}),
				)
				relations++
			}
			if errs != nil {
				result = append(result, *errorsToMessageWithKey(&errs, relationMapping, span, now))
				errors++
			}
		}
	})

	logger.Debug(
		"Converted spans to topology stream messages",
		zap.Int("components", components),
		zap.Int("relations", relations),
		zap.Int("errors", errors),
	)

	return result
}

func convertToComponent(
	expressionEvaluator ExpressionEvaluator,
	mapper *Mapper,
	evalContext *ExpressionEvalContext,
	mapping *settings.OtelComponentMapping,
) (*topo_stream_v1.TopologyStreamComponent, []error) {
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
) (*topo_stream_v1.TopologyStreamRelation, []error) {
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

func errorsToMessageWithKey(errs *[]error, mapping settings.Mapping, span *ptrace.Span, now int64) *MessageWithKey {
	streamErrors := make([]*topo_stream_v1.TopoStreamError, len(*errs))
	for i, err := range *errs {
		streamErrors[i] = &topo_stream_v1.TopoStreamError{
			Message: err.Error(),
		}
	}
	return &MessageWithKey{
		Key: &topo_stream_v1.TopologyStreamMessageKey{
			Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: mapping.GetIdentifier(),
			ShardId:    "unknown",
		},
		Message: &topo_stream_v1.TopologyStreamMessage{
			CollectionTimestamp: now,
			SubmittedTimestamp:  convertTimestampToInt64(span.EndTimestamp()),
			Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
					ExpiryIntervalMs: mapping.GetExpireAfterMs(),
					Errors:           streamErrors,
				},
			},
		},
	}
}

func outputToMessageWithKey(
	output topo_stream_v1.ComponentOrRelation,
	mapping settings.Mapping,
	span *ptrace.Span,
	now int64,
	toComponents func() []*topo_stream_v1.TopologyStreamComponent,
	toRelations func() []*topo_stream_v1.TopologyStreamRelation,
) *MessageWithKey {
	submittedTimestamp := convertTimestampToInt64(span.EndTimestamp())
	return &MessageWithKey{
		Key: &topo_stream_v1.TopologyStreamMessageKey{
			Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: mapping.GetIdentifier(),
			ShardId:    stableShardID(output.GetExternalId(), ShardCount),
		},
		Message: &topo_stream_v1.TopologyStreamMessage{
			CollectionTimestamp: now,
			SubmittedTimestamp:  submittedTimestamp,
			Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
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
