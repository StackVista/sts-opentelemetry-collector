package internal

import (
	"fmt"
	"github.com/hashicorp/golang-lru/v2/expirable"
	topo_stream_v1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"regexp"
	"strings"
	"time"
)

// TODO: turn mapping into a type so we don't have this global variable
//
//nolint:gochecknoglobals
var regexCache *expirable.LRU[string, *regexp.Regexp]

func init() {
	regexCache = expirable.NewLRU[string, *regexp.Regexp](1000, nil, 12*time.Hour)
}

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
	processTags := func(tagMappings *[]settings.OtelTagMapping, handleErrors bool) {
		if tagMappings == nil {
			return
		}
		resolved, errs := ResolveTagMappings(*tagMappings, expressionEvaluator, expressionEvalCtx)
		for k, v := range resolved {
			tags[k] = v
		}
		if handleErrors {
			for _, err := range errs {
				joinErr(err)
			}
		}
	}

	if mapping.Output.Optional != nil {
		processTags(mapping.Output.Optional.Tags, false)
	}
	if mapping.Output.Required != nil {
		processTags(mapping.Output.Required.Tags, true)
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

func ResolveTagMappings(
	mappings []settings.OtelTagMapping,
	evaluator ExpressionEvaluator,
	evalCtx *ExpressionEvalContext,
) (map[string]string, []error) {
	tags := make(map[string]string)
	var errs []error

	for _, m := range mappings {
		if m.Pattern == nil {
			// no regex pattern assumes standard string expr evaluation
			val, err := evaluator.EvalStringExpression(m.Source, evalCtx)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to evaluate OtelTagMapping source %q: %w", m.Source.Expression, err))
				continue
			}
			tags[m.Target] = val
			continue
		}

		// regex-based mapping requires map evaluation
		resolvedMap, err := evaluator.EvalMapExpression(m.Source, evalCtx)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to evaluate OtelTagMapping source %q: %w", m.Source.Expression, err))
			continue
		}

		re, ok := regexCache.Get(*m.Pattern)
		if !ok {
			re, err = regexp.Compile(*m.Pattern)
			if err != nil {
				errs = append(errs, fmt.Errorf("invalid OtelTagMapping regex pattern %q: %w", *m.Pattern, err))
				continue
			}
			regexCache.Add(*m.Pattern, re)
		}

		for key, value := range resolvedMap {
			strVal, stringifyErr := stringifyTagValue(value)
			if stringifyErr != nil {
				errs = append(
					errs,
					fmt.Errorf("value for key %q in OtelTagMapping source %q is not a string", key, m.Source.Expression),
				)
				continue
			}

			matches := re.FindStringSubmatch(key)
			if len(matches) == 0 {
				continue
			}

			targetKey := m.Target
			// substitute capture groups, skip matches[0] - the entire matched string
			for i, match := range matches[1:] {
				placeholder := fmt.Sprintf("${%d}", i+1) // capture groups start at 1
				targetKey = strings.ReplaceAll(targetKey, placeholder, match)
			}

			// Preserve existing (explicit) keys
			if _, exists := tags[targetKey]; exists {
				continue
			}

			tags[targetKey] = strVal
		}
	}

	return tags, errs
}

func stringifyTagValue(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case fmt.Stringer:
		return v.String(), nil
	case int, int32, int64, float32, float64, bool:
		return fmt.Sprint(v), nil
	default:
		return "", fmt.Errorf("value did not evaluate to string, got: %T", value)
	}
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
