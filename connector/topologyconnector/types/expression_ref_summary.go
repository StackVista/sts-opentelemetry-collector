//nolint:revive
package types

import "sort"

type EntityRefSummary struct {
	// Hash all attributes (attributes)
	AllAttributes bool

	// Hash only these attribute keys (attributes["key"])
	AttributeKeys []string

	// Hash these top-level entity fields (span.name, metric.unit, etc.)
	FieldKeys []string
}

func NewEntityRefSummary(allAttrs bool, attrKeys, fieldKeys []string) EntityRefSummary {
	sort.Strings(attrKeys)
	sort.Strings(fieldKeys)

	return EntityRefSummary{
		AllAttributes: allAttrs,
		AttributeKeys: attrKeys,
		FieldKeys:     fieldKeys,
	}
}

func (e EntityRefSummary) HasRefs() bool {
	return e.AllAttributes ||
		len(e.FieldKeys) > 0 ||
		len(e.AttributeKeys) > 0
}

// ExpressionRefSummary summarizes which inputs to include in the projection for a mapping.
// Resource attributes and Scope fields are always included entirely.
type ExpressionRefSummary struct {
	Datapoint EntityRefSummary
	Span      EntityRefSummary
	Metric    EntityRefSummary
	Scope     EntityRefSummary
	Resource  EntityRefSummary
}

// NewExpressionRefSummary Constructor ensures all slices are sorted deterministically
func NewExpressionRefSummary(datapoint, span, metric, scope, resource EntityRefSummary) *ExpressionRefSummary {
	sortStrings := func(s []string) []string {
		if len(s) == 0 {
			return nil
		}
		c := append([]string(nil), s...)
		sort.Strings(c)
		return c
	}

	datapoint.AttributeKeys = sortStrings(datapoint.AttributeKeys)
	datapoint.FieldKeys = sortStrings(datapoint.FieldKeys)

	span.AttributeKeys = sortStrings(span.AttributeKeys)
	span.FieldKeys = sortStrings(span.FieldKeys)

	metric.AttributeKeys = sortStrings(metric.AttributeKeys)
	metric.FieldKeys = sortStrings(metric.FieldKeys)

	scope.AttributeKeys = sortStrings(scope.AttributeKeys)
	scope.FieldKeys = sortStrings(scope.FieldKeys)

	resource.AttributeKeys = sortStrings(resource.AttributeKeys)
	resource.FieldKeys = sortStrings(resource.FieldKeys)

	return &ExpressionRefSummary{
		Datapoint: datapoint,
		Span:      span,
		Metric:    metric,
		Scope:     scope,
		Resource:  resource,
	}
}
