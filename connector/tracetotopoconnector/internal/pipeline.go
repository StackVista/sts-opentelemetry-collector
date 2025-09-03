package internal

import (
	topo_stream_v1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type MessageWithKey struct {
	Key     *topo_stream_v1.TopologyStreamMessageKey
	Message *topo_stream_v1.TopologyStreamMessage
}

func ConvertSpanToTopologyStreamMessage(eval ExpressionEvaluator, trace ptrace.Traces, componentMappings []settings.OtelComponentMapping, relationMappings []settings.OtelRelationMapping, now int64) []MessageWithKey {
	result := make([]MessageWithKey, 0)

	for _, componentMapping := range componentMappings {
		iterateSpans(trace, func(resourceSpan *ptrace.ResourceSpans, scopeSpan *ptrace.ScopeSpans, span *ptrace.Span) {
			component, errs := convertToComponent(eval, resourceSpan, scopeSpan, span, &componentMapping)
			if component != nil {
				result = append(result, *outputToMessageWithKey(component, componentMapping, span, now, func() []*topo_stream_v1.TopologyStreamComponent {
					return []*topo_stream_v1.TopologyStreamComponent{component}
				}, func() []*topo_stream_v1.TopologyStreamRelation {
					return nil
				}))
			}
			if errs != nil {
				result = append(result, *errorsToMessageWithKey(&errs, componentMapping, span, now))
			}
		})
	}
	for _, relationMapping := range relationMappings {
		iterateSpans(trace, func(resourceSpan *ptrace.ResourceSpans, scopeSpan *ptrace.ScopeSpans, span *ptrace.Span) {
			relation, errs := convertToRelation(eval, resourceSpan, scopeSpan, span, &relationMapping)
			if relation != nil {
				result = append(result, *outputToMessageWithKey(relation, relationMapping, span, now, func() []*topo_stream_v1.TopologyStreamComponent {
					return nil
				}, func() []*topo_stream_v1.TopologyStreamRelation {
					return []*topo_stream_v1.TopologyStreamRelation{relation}
				}))
			}
			if errs != nil {
				result = append(result, *errorsToMessageWithKey(&errs, relationMapping, span, now))
			}
		})
	}
	return result
}

func convertToComponent(expressionEvaluator ExpressionEvaluator, resourceSpan *ptrace.ResourceSpans, scopeSpan *ptrace.ScopeSpans, span *ptrace.Span, mapping *settings.OtelComponentMapping) (*topo_stream_v1.TopologyStreamComponent, []error) {
	evaluatedVars, _ := EvalVariables(expressionEvaluator, span, scopeSpan, resourceSpan, mapping.Vars)
	expressionEvalCtx := &ExpressionEvalContext{*span, *scopeSpan, *resourceSpan, evaluatedVars}

	if filterByConditions(expressionEvaluator, expressionEvalCtx, &mapping.Conditions) {
		component, err := MapComponent(mapping, expressionEvaluator, expressionEvalCtx)
		if len(err) > 0 {
			return nil, err
		}
		return component, nil
	}
	return nil, nil
}

func convertToRelation(expressionEvaluator ExpressionEvaluator, resourceSpan *ptrace.ResourceSpans, scopeSpan *ptrace.ScopeSpans, span *ptrace.Span, mapping *settings.OtelRelationMapping) (*topo_stream_v1.TopologyStreamRelation, []error) {
	evaluatedVars, _ := EvalVariables(expressionEvaluator, span, scopeSpan, resourceSpan, mapping.Vars)
	expressionEvalCtx := &ExpressionEvalContext{*span, *scopeSpan, *resourceSpan, evaluatedVars}

	if filterByConditions(expressionEvaluator, expressionEvalCtx, &mapping.Conditions) {
		relation, err := MapRelation(mapping, expressionEvaluator, expressionEvalCtx)
		if len(err) > 0 {
			return nil, err
		}
		return relation, nil
	}
	return nil, nil
}

func iterateSpans(trace ptrace.Traces, handler func(resourceSpan *ptrace.ResourceSpans, scopeSpan *ptrace.ScopeSpans, span *ptrace.Span)) {
	resourceSpans := trace.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		scopeSpans := rs.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			ss := scopeSpans.At(j)
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				handler(&rs, &ss, &span)
			}
		}
	}
}

func errorsToMessageWithKey(errs *[]error, mapping settings.Mapping, span *ptrace.Span, now int64) *MessageWithKey {
	errsAsString := make([]string, len(*errs))
	for i, err := range *errs {
		errsAsString[i] = err.Error()
	}
	return &MessageWithKey{
		Key: &topo_stream_v1.TopologyStreamMessageKey{
			Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: mapping.GetId(),
			ShardId:    "unknown",
		},
		Message: &topo_stream_v1.TopologyStreamMessage{
			CollectionTimestamp: now,
			SubmittedTimestamp:  int64(span.EndTimestamp()),
			Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
					ExpiryIntervalMs: mapping.GetExpireAfterMs(),
					Errors:           errsAsString,
				},
			},
		},
	}
}

func outputToMessageWithKey(output topo_stream_v1.ComponentOrRelation, mapping settings.Mapping, span *ptrace.Span, now int64, toComponents func() []*topo_stream_v1.TopologyStreamComponent, toRelations func() []*topo_stream_v1.TopologyStreamRelation) *MessageWithKey {
	return &MessageWithKey{
		Key: &topo_stream_v1.TopologyStreamMessageKey{
			Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: mapping.GetId(),
			ShardId:    output.GetExternalId(),
		},
		Message: &topo_stream_v1.TopologyStreamMessage{
			CollectionTimestamp: now,
			SubmittedTimestamp:  int64(span.EndTimestamp()),
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
