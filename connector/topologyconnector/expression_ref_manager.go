package topologyconnector

import (
	"sort"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/types"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.uber.org/zap"
)

// ExpressionRefManager should perform best-effort extraction of referenced variables
// and attribute keys from mapping expressions.
//
// All errors during expression parsing or type-checking should intentionally be ignored.
// Missing or invalid expressions simply result in fewer extracted references.
//
// Note: Mappings that only reference resource/scope do not produce ExpressionRefSummaries,
// as those are always hashed unconditionally during deduplication.
type ExpressionRefManager interface {
	Update(
		signals []stsSettingsModel.OtelInputSignal,
		componentMappings map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelComponentMapping,
		relationMappings map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelRelationMapping,
	)

	Current(signal stsSettingsModel.OtelInputSignal) map[string]*types.ExpressionRefSummary
}

type DefaultExpressionRefManager struct {
	logger    *zap.Logger
	evaluator internal.ExpressionEvaluator

	mu sync.RWMutex
	// signal -> mappingIdentifier -> summary
	expressionRefSummaries map[stsSettingsModel.OtelInputSignal]map[string]*types.ExpressionRefSummary
}

func NewExpressionRefManager(
	logger *zap.Logger,
	evaluator internal.ExpressionEvaluator,
) *DefaultExpressionRefManager {
	return &DefaultExpressionRefManager{
		logger:    logger,
		evaluator: evaluator,
		expressionRefSummaries: make(
			map[stsSettingsModel.OtelInputSignal]map[string]*types.ExpressionRefSummary,
		),
	}
}

// Update walks the CEL ASTs of mapping expressions to precompute referenced vars and attribute keys used by mappings
// for each signal.
func (p *DefaultExpressionRefManager) Update(
	signals []stsSettingsModel.OtelInputSignal,
	componentMappings map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelComponentMapping,
	relationMappings map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelRelationMapping,
) {
	p.logger.Debug("ExpressionRefManager processing snapshot update",
		zap.Int("signal_count", len(signals)))

	summariesBySignalUpdate := make(map[stsSettingsModel.OtelInputSignal]map[string]*types.ExpressionRefSummary)

	for _, sig := range signals {
		summariesBySignal := make(map[string]*types.ExpressionRefSummary)

		for _, cm := range componentMappings[sig] {
			refSummary := p.collectRefsForComponent(&cm)
			if refSummary != nil {
				summariesBySignal[cm.GetIdentifier()] = refSummary
			}
		}
		for _, rm := range relationMappings[sig] {
			refSummary := p.collectRefsForRelation(&rm)
			if refSummary != nil {
				summariesBySignal[rm.GetIdentifier()] = refSummary
			}
		}

		if len(summariesBySignal) > 0 {
			summariesBySignalUpdate[sig] = summariesBySignal
		}
	}

	p.mu.Lock()
	p.expressionRefSummaries = summariesBySignalUpdate
	p.mu.Unlock()

	p.logger.Debug("ExpressionRefManager update complete",
		zap.Int("total_refs", p.countTotalRefs(summariesBySignalUpdate)))
}

func (p *DefaultExpressionRefManager) countTotalRefs(
	refs map[stsSettingsModel.OtelInputSignal]map[string]*types.ExpressionRefSummary,
) int {
	count := 0
	for _, m := range refs {
		count += len(m)
	}
	return count
}

func (p *DefaultExpressionRefManager) Current(
	signal stsSettingsModel.OtelInputSignal,
) map[string]*types.ExpressionRefSummary {
	p.mu.RLock()
	defer p.mu.RUnlock()

	src := p.expressionRefSummaries[signal]
	if src == nil {
		return nil
	}

	dst := make(map[string]*types.ExpressionRefSummary, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func (p *DefaultExpressionRefManager) collectRefsForComponent(
	m *stsSettingsModel.OtelComponentMapping,
) *types.ExpressionRefSummary {
	agg := newExpressionRefAggregator(p.logger)

	// input not being walked - it's already processed at this point (via the signal traverser/visitor)

	// variables
	if m.Vars != nil {
		for _, v := range *m.Vars {
			agg.walkAny(p.evaluator, v.Value)
		}
	}

	// core outputs
	agg.walkString(p.evaluator, m.Output.Identifier)
	agg.walkString(p.evaluator, m.Output.Name)
	agg.walkString(p.evaluator, m.Output.TypeName)
	agg.walkOptionalString(p.evaluator, m.Output.TypeIdentifier)
	agg.walkString(p.evaluator, m.Output.LayerName)
	agg.walkOptionalString(p.evaluator, m.Output.LayerIdentifier)
	agg.walkString(p.evaluator, m.Output.DomainName)
	agg.walkOptionalString(p.evaluator, m.Output.DomainIdentifier)
	p.collectRefsForComponentFieldMapping(agg, m.Output.Optional)
	p.collectRefsForComponentFieldMapping(agg, m.Output.Required)

	return agg.toSummary()
}

func (p *DefaultExpressionRefManager) collectRefsForComponentFieldMapping(
	agg *expressionRefAggregator,
	componentFieldMapping *stsSettingsModel.OtelComponentMappingFieldMapping,
) {
	if componentFieldMapping != nil {
		if componentFieldMapping.AdditionalIdentifiers != nil {
			for _, e := range *componentFieldMapping.AdditionalIdentifiers {
				agg.walkString(p.evaluator, e)
			}
		}
		if componentFieldMapping.Tags != nil {
			for _, tm := range *componentFieldMapping.Tags {
				agg.walkAny(p.evaluator, tm.Source)
			}
		}
		if componentFieldMapping.Version != nil {
			agg.walkOptionalString(p.evaluator, componentFieldMapping.Version)
		}
	}
}

func (p *DefaultExpressionRefManager) collectRefsForRelation(
	m *stsSettingsModel.OtelRelationMapping,
) *types.ExpressionRefSummary {
	agg := newExpressionRefAggregator(p.logger)

	// input not being walked - it's already processed at this point (via the signal traverser/visitor)

	// variables
	if m.Vars != nil {
		for _, v := range *m.Vars {
			agg.walkAny(p.evaluator, v.Value)
		}
	}

	// outputs
	agg.walkString(p.evaluator, m.Output.SourceId)
	agg.walkString(p.evaluator, m.Output.TargetId)
	agg.walkString(p.evaluator, m.Output.TypeName)
	agg.walkOptionalString(p.evaluator, m.Output.TypeIdentifier)

	return agg.toSummary()
}

// expressionRefAggregator accumulates references using the ExpressionAstWalker and reduces them
// to ExpressionRefSummary.

type entityFieldSelector struct {
	// attributes["*"] – hash the entire attribute map
	allAttributes bool

	// attributes["key"]
	attributeKeys map[string]struct{}

	// top-level fields (e.g. span.name, metric.unit)
	fieldKeys map[string]struct{}
}

type expressionRefAggregator struct {
	logger *zap.Logger

	datapoint entityFieldSelector
	span      entityFieldSelector
	metric    entityFieldSelector

	hasValidExpr bool
}

func newExpressionRefAggregator(logger *zap.Logger) *expressionRefAggregator {
	return &expressionRefAggregator{
		logger: logger,
		datapoint: entityFieldSelector{
			attributeKeys: make(map[string]struct{}),
			fieldKeys:     make(map[string]struct{}),
		},
		span: entityFieldSelector{
			attributeKeys: make(map[string]struct{}),
			fieldKeys:     make(map[string]struct{}),
		},
		metric: entityFieldSelector{
			attributeKeys: make(map[string]struct{}),
			fieldKeys:     make(map[string]struct{}),
		},
		hasValidExpr: false,
	}
}

func (r *expressionRefAggregator) walkString(
	eval internal.ExpressionEvaluator,
	expr stsSettingsModel.OtelStringExpression,
) {
	astRes, err := eval.GetStringExpressionAST(expr)
	if err != nil || astRes == nil || astRes.CheckedAST == nil {
		return
	}
	r.hasValidExpr = true
	r.walkAST(astRes.CheckedAST)
}

func (r *expressionRefAggregator) walkOptionalString(
	eval internal.ExpressionEvaluator,
	expr *stsSettingsModel.OtelStringExpression,
) {
	if expr == nil {
		return
	}
	r.walkString(eval, *expr)
}

func (r *expressionRefAggregator) walkAny(
	eval internal.ExpressionEvaluator,
	expr stsSettingsModel.OtelAnyExpression,
) {
	astRes, err := eval.GetAnyExpressionAST(expr)
	if err != nil || astRes == nil || astRes.CheckedAST == nil {
		return
	}
	r.hasValidExpr = true
	r.walkAST(astRes.CheckedAST)
}

// walkAST processes a checked CEL AST and accumulates attribute references
// into the corresponding sets (datapointAttrFilter, spanAttrFilter, metricAttrFilter).
//
// Current behavior:
//   - "datapoint" root: adds keys from datapoint.attributes
//   - "span" root: adds keys from span.attributes
//   - "metric" root: adds keys from metric.attributes
//   - "resource" and "scope" roots are ignored because they are fully included
//
// IMPORTANT: If a new type of input is added (e.g., logs, events) or a new root
// is introduced in the mapping expressions, this function must be extended
// to correctly accumulate references for deduplication purposes.
//
// Also, if additional roots are supported, make sure to update:
//   - types.ExpressionRefSummary
//   - expressionRefAggregator.toSummary()
//   - collectRefsForComponent / collectRefsForRelation
func (r *expressionRefAggregator) walkAST(checked *cel.Ast) {
	walker := internal.NewExpressionAstWalker()
	walker.Walk(checked.NativeRep().Expr())

	for _, ref := range walker.GetReferences() {
		switch ref.Root {
		case "datapoint":
			r.walkAttributeRef(&r.datapoint, ref)

		case "span":
			r.walkAttributeRef(&r.span, ref)

		case "metric":
			r.walkAttributeRef(&r.metric, ref)

		case "resource", "scope", "vars":
			// resource & scope are always fully included
			// vars resolve to datapoint/span/metric data

		default:
			r.logger.Debug(
				"Unknown expression ref root detected; consider updating walkAST and toSummary",
				zap.String("root", ref.Root),
			)
		}
	}
}

func (r *expressionRefAggregator) walkAttributeRef(
	sel *entityFieldSelector,
	ref internal.Reference,
) {
	if sel.allAttributes {
		return
	}

	// attributes
	if len(ref.Path) == 1 && ref.Path[0] == "attributes" {
		sel.allAttributes = true
		sel.attributeKeys = nil
		return
	}

	// top-level fields (span.name, metric.unit, etc.)
	if len(ref.Path) == 1 {
		sel.fieldKeys[ref.Path[0]] = struct{}{}
	}

	// attributes["key"]
	if len(ref.Path) >= 2 && ref.Path[0] == "attributes" {
		sel.attributeKeys[ref.Path[1]] = struct{}{}
	}
}

func (r *expressionRefAggregator) toSummary() *types.ExpressionRefSummary {
	if !r.hasValidExpr {
		return nil
	}

	return types.NewExpressionRefSummary(
		selectorToSummary(r.datapoint),
		selectorToSummary(r.span),
		selectorToSummary(r.metric),
	)
}

func selectorToSummary(sel entityFieldSelector) types.EntityRefSummary {
	return types.NewEntityRefSummary(
		sel.allAttributes,
		setKeys(sel.attributeKeys),
		setKeys(sel.fieldKeys),
	)
}

func setKeys(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
