package internal

import (
	topo_stream_v1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
)

// MapComponent maps an OTEL span and variables to a TopologyStreamComponent based on the given mapping configuration.
// It evaluates expressions, constructs a component, and returns it along with any encountered conditionErrsLookup.
func MapComponent(
	mapping *settings.OtelComponentMapping,
	expressionEvaluator ExpressionEvaluator,
	expressionEvalCtx *ExpressionEvalContext,
) (*topo_stream_v1.TopologyStreamComponent, []error) {
	errors := make([]error, 0)
	joinErr := func(err error) {
		if err != nil {
			errors = append(errors, err)
		}
	}
	evalStr := func(expr settings.OtelStringExpression) string {
		val, err := expressionEvaluator.EvalStringExpression(expr, expressionEvalCtx)
		joinErr(err)
		return val
	}
	evalOptStr := func(expr *settings.OtelStringExpression) *string {
		val, err := expressionEvaluator.EvalOptionalStringExpression(expr, expressionEvalCtx)
		joinErr(err)
		return val
	}

	additionalIdentifiers := make([]string, 0)
	if mapping.Output.Optional != nil && mapping.Output.Optional.AdditionalIdentifiers != nil {
		for _, expr := range *mapping.Output.Optional.AdditionalIdentifiers {
			if id, err := expressionEvaluator.EvalStringExpression(expr, expressionEvalCtx); err == nil {
				additionalIdentifiers = append(additionalIdentifiers, id)
			}
		}
	}
	if mapping.Output.Required != nil && mapping.Output.Required.AdditionalIdentifiers != nil {
		for _, expr := range *mapping.Output.Required.AdditionalIdentifiers {
			additionalIdentifiers = append(additionalIdentifiers, evalStr(expr))
		}
	}

	tags := make(map[string]string)
	if mapping.Output.Optional != nil && mapping.Output.Optional.Tags != nil {
		for key, expr := range *mapping.Output.Optional.Tags {
			if tagValue, err := expressionEvaluator.EvalStringExpression(expr, expressionEvalCtx); err == nil {
				tags[key] = tagValue
			}
		}
	}
	if mapping.Output.Required != nil && mapping.Output.Required.Tags != nil {
		for key, expr := range *mapping.Output.Required.Tags {
			tags[key] = evalStr(expr)
		}
	}
	tagsList := make([]string, 0, len(tags))
	for key, value := range tags {
		tagsList = append(tagsList, key+":"+value)
	}

	result := topo_stream_v1.TopologyStreamComponent{
		ExternalId:         evalStr(mapping.Output.Identifier),
		Identifiers:        additionalIdentifiers,
		Name:               evalStr(mapping.Output.Name),
		TypeName:           evalStr(mapping.Output.TypeName),
		TypeIdentifier:     evalOptStr(mapping.Output.TypeIdentifier),
		LayerName:          evalStr(mapping.Output.LayerName),
		LayerIdentifier:    evalOptStr(mapping.Output.LayerIdentifier),
		DomainName:         evalStr(mapping.Output.DomainName),
		DomainIdentifier:   evalOptStr(mapping.Output.DomainIdentifier),
		ResourceDefinition: nil,
		StatusData:         nil,
		Tags:               tagsList,
	}
	if len(errors) > 0 {
		return nil, errors
	}
	return &result, nil
}

// MapRelation creates and returns a TopologyStreamRelation based on the provided
// OtelRelationMapping, span, and variables.
func MapRelation(
	mapping *settings.OtelRelationMapping,
	expressionEvaluator ExpressionEvaluator,
	expressionEvalCtx *ExpressionEvalContext,
) (*topo_stream_v1.TopologyStreamRelation, []error) {
	errors := make([]error, 0)
	joinErr := func(err error) {
		if err != nil {
			errors = append(errors, err)
		}
	}
	evalStr := func(expr settings.OtelStringExpression) string {
		val, err := expressionEvaluator.EvalStringExpression(expr, expressionEvalCtx)
		joinErr(err)
		return val
	}
	evalOptStr := func(expr *settings.OtelStringExpression) *string {
		val, err := expressionEvaluator.EvalOptionalStringExpression(expr, expressionEvalCtx)
		joinErr(err)
		return val
	}

	sourceID := evalStr(mapping.Output.SourceId)
	targetID := evalStr(mapping.Output.TargetId)
	result := topo_stream_v1.TopologyStreamRelation{
		ExternalId:       sourceID + "-" + targetID,
		SourceIdentifier: sourceID,
		TargetIdentifier: targetID,
		Name:             "", // TODO the name should be nil
		TypeName:         evalStr(mapping.Output.TypeName),
		TypeIdentifier:   evalOptStr(mapping.Output.TypeIdentifier),
		Tags:             nil,
	}
	if len(errors) > 0 {
		return nil, errors
	}
	return &result, nil
}
