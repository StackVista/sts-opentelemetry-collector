package internal

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
)

var (
	placeholderRegex = regexp.MustCompile(`\$\{(\d+)\}`)
)

func init() {
}

type Mapper struct {
	regexCache    *expirable.LRU[string, *regexp.Regexp]
	templateCache *expirable.LRU[string, string]
}

func NewMapper() *Mapper {
	return &Mapper{
		regexCache:    expirable.NewLRU[string, *regexp.Regexp](1000, nil, 12*time.Hour),
		templateCache: expirable.NewLRU[string, string](1000, nil, 12*time.Hour),
	}
}

// MapComponent maps an OTEL span and variables to a TopologyStreamComponent based on the given mapping configuration.
// It evaluates expressions, constructs a component, and returns it along with any encountered conditionErrsLookup.
func (me *Mapper) MapComponent(
	mapping *settings.OtelComponentMapping,
	expressionEvaluator ExpressionEvaluator,
	expressionEvalCtx *ExpressionEvalContext,
) (*topostreamv1.TopologyStreamComponent, []error) {
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

	allIdentifiers := make([]string, 0)
	allIdentifiers = append(allIdentifiers, evalStr(mapping.Output.Identifier))
	if mapping.Output.Optional != nil && mapping.Output.Optional.AdditionalIdentifiers != nil {
		for _, expr := range *mapping.Output.Optional.AdditionalIdentifiers {
			if id, err := expressionEvaluator.EvalStringExpression(expr, expressionEvalCtx); err == nil {
				allIdentifiers = append(allIdentifiers, id)
			}
		}
	}
	if mapping.Output.Required != nil && mapping.Output.Required.AdditionalIdentifiers != nil {
		for _, expr := range *mapping.Output.Required.AdditionalIdentifiers {
			allIdentifiers = append(allIdentifiers, evalStr(expr))
		}
	}

	tags := make(map[string]string)
	processTags := func(tagMappings *[]settings.OtelTagMapping, handleErrors bool) {
		if tagMappings == nil {
			return
		}
		resolved, errs := me.ResolveTagMappings(*tagMappings, expressionEvaluator, expressionEvalCtx)
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

	result := topostreamv1.TopologyStreamComponent{
		ExternalId:         evalStr(mapping.Output.Identifier),
		Identifiers:        allIdentifiers,
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

func (me *Mapper) ResolveTagMappings(
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

		re, ok := me.regexCache.Get(*m.Pattern)
		if !ok {
			re, err = regexp.Compile(*m.Pattern)
			if err != nil {
				errs = append(errs, fmt.Errorf("invalid OtelTagMapping regex pattern %q: %w", *m.Pattern, err))
				continue
			}
			me.regexCache.Add(*m.Pattern, re)
		}

		// Convert the target template to the format expected by ExpandString ("$1")
		// and cache it for reuse.
		expandTemplate, ok := me.templateCache.Get(m.Target)
		if !ok {
			expandTemplate = placeholderRegex.ReplaceAllStringFunc(m.Target, func(s string) string {
				// s is the matched string, e.g., "${1}"
				// we need to extract the "1" and return "$1"
				return "$" + s[2:len(s)-1]
			})
			me.templateCache.Add(m.Target, expandTemplate)
		}

		for key, value := range resolvedMap {
			strVal, stringifyErr := stringifyTagValue(value)
			if stringifyErr != nil {
				errs = append(
					errs,
					fmt.Errorf(
						"value for key %q in OtelTagMapping source %q is not a string: %w",
						key, m.Source.Expression, stringifyErr,
					),
				)
				continue
			}

			// Use FindStringSubmatchIndex to avoid allocating strings for matches.
			matchIndexes := re.FindStringSubmatchIndex(key)
			if len(matchIndexes) == 0 {
				continue
			}

			// Use ExpandString for an efficient, allocation-optimized substitution.
			targetKeyBytes := re.ExpandString(nil, expandTemplate, key, matchIndexes)
			targetKey := string(targetKeyBytes)

			// Preserve existing (explicit) keys
			if _, exists := tags[targetKey]; exists {
				continue
			}

			tags[targetKey] = strVal
		}
	}

	return tags, errs
}

// MapRelation creates and returns a TopologyStreamRelation based on the provided
// OtelRelationMapping, span, and variables.
func (me *Mapper) MapRelation(
	mapping *settings.OtelRelationMapping,
	expressionEvaluator ExpressionEvaluator,
	expressionEvalCtx *ExpressionEvalContext,
) (*topostreamv1.TopologyStreamRelation, []error) {
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
	result := topostreamv1.TopologyStreamRelation{
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

func stringifyTagValue(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case fmt.Stringer:
		return v.String(), nil
	case int, int32, uint64, int64, float32, float64, bool:
		return fmt.Sprint(v), nil
	case []interface{}:
		parts := make([]string, 0, len(v))
		for _, elem := range v {
			parts = append(parts, fmt.Sprint(elem))
		}
		return strings.Join(parts, " "), nil
	case map[string]interface{}:
		bytes, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("failed to stringify map: %w", err)
		}
		return string(bytes), nil
	default:
		return "", fmt.Errorf("value did not evaluate to string, got: %T", value)
	}
}
