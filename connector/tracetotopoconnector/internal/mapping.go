package internal

import (
	"errors"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	topo_stream_v1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// MapComponent maps an OpenTelemetry span and variables to a TopologyStreamComponent based on the given mapping configuration.
// It evaluates expressions, constructs a component, and returns it along with any encountered errors.
func MapComponent(mapping *settings.OtelComponentMapping, span *ptrace.Span, vars *map[string]string) (*topo_stream_v1.TopologyStreamComponent, error) {
	var errs error
	joinErr := func(err error) {
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	evalStr := func(expr settings.OtelStringExpression) string {
		val, err := EvalStringExpression(expr, span, vars)
		joinErr(err)
		return val
	}
	evalOptStr := func(expr *settings.OtelStringExpression) *string {
		val, err := EvalOptionalStringExpression(expr, span, vars)
		joinErr(err)
		return val
	}

	additionalIdentifiers := make([]string, 0)
	if mapping.Output.Optional != nil && mapping.Output.Optional.AdditionalIdentifiers != nil {
		for _, expr := range *mapping.Output.Optional.AdditionalIdentifiers {
			if id, err := EvalStringExpression(expr, span, vars); err == nil {
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
			if tagValue, err := EvalStringExpression(expr, span, vars); err == nil {
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
	return &result, errs
}

// MapRelation creates and returns a TopologyStreamRelation based on the provided OtelRelationMapping, span, and variables.
func MapRelation(mapping *settings.OtelRelationMapping, span *ptrace.Span, vars *map[string]string) (*topo_stream_v1.TopologyStreamRelation, error) {
	var errs error
	joinErr := func(err error) {
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	evalStr := func(expr settings.OtelStringExpression) string {
		val, err := EvalStringExpression(expr, span, vars)
		joinErr(err)
		return val
	}
	evalOptStr := func(expr *settings.OtelStringExpression) *string {
		val, err := EvalOptionalStringExpression(expr, span, vars)
		joinErr(err)
		return val
	}

	result := topo_stream_v1.TopologyStreamRelation{
		ExternalId:       "todo",
		SourceIdentifier: evalStr(mapping.Output.SourceId),
		TargetIdentifier: evalStr(mapping.Output.TargetId),
		Name:             "todo",
		TypeName:         evalStr(mapping.Output.TypeName),
		TypeIdentifier:   evalOptStr(mapping.Output.TypeIdentifier),
		Tags:             []string{},
	}
	return &result, errs
}
