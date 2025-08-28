package internal

import (
	topo_stream_v1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func ConvertSpanToTopologyStreamMessage(eval ExpressionEvaluator, trace ptrace.Traces, componentMappings []settings.OtelComponentMapping, relationMappings []settings.OtelRelationMapping, now int64) topo_stream_v1.TopologyStreamMessage {
	components := make([]*topo_stream_v1.TopologyStreamComponent, 0)
	relations := make([]*topo_stream_v1.TopologyStreamRelation, 0)
	errors := make([]error, 0)

	resourceSpans := trace.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		scopeSpans := rs.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			ss := scopeSpans.At(j)
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)

				for _, componentMapping := range componentMappings {
					component, errs := convertToComponent(eval, &rs, &ss, &span, &componentMapping)
					if component != nil {
						components = append(components, component)
					}
					if errs != nil {
						errors = append(errors, errs...)
					}
				}

				for _, relationMapping := range relationMappings {
					relation, errs := convertToRelation(eval, &rs, &ss, &span, &relationMapping)
					if relation != nil {
						relations = append(relations, relation)
					}
					if errs != nil {
						errors = append(errors, errs...)
					}
				}
			}
		}
	}

	errorStrings := make([]string, len(errors))
	for i, err := range errors {
		if err != nil {
			errorStrings[i] = err.Error()
		}
	}

	return topo_stream_v1.TopologyStreamMessage{
		CollectionTimestamp: now,
		SubmittedTimestamp:  now,
		Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
			TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
				ExpiryIntervalMs: 0, //TODO handle TTL
				Components:       components,
				Relations:        relations,
				Errors:           errorStrings,
			},
		},
	}
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
