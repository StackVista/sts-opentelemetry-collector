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
	Span      EntityRefSummary
	Log       EntityRefSummary
	Datapoint EntityRefSummary
	Metric    EntityRefSummary
	Scope     EntityRefSummary
	Resource  EntityRefSummary
}

// NewExpressionRefSummary Constructor ensures all slices are sorted deterministically
func NewExpressionRefSummary(span, log, datapoint, metric, scope, resource EntityRefSummary) *ExpressionRefSummary {
	sortStrings := func(s []string) []string {
		if len(s) == 0 {
			return nil
		}
		c := append([]string(nil), s...)
		sort.Strings(c)
		return c
	}

	sortEntity := func(e *EntityRefSummary) {
		e.AttributeKeys = sortStrings(e.AttributeKeys)
		e.FieldKeys = sortStrings(e.FieldKeys)
	}

	sortEntity(&log)
	sortEntity(&span)
	sortEntity(&datapoint)
	sortEntity(&metric)
	sortEntity(&scope)
	sortEntity(&resource)

	return &ExpressionRefSummary{
		Span:      span,
		Log:       log,
		Datapoint: datapoint,
		Metric:    metric,
		Scope:     scope,
		Resource:  resource,
	}
}
