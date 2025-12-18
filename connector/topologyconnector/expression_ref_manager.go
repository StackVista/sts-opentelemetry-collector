package topologyconnector

import (
	"slices"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/types"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.uber.org/zap"
)

const attributeMap = "attributes"

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
	return p.expressionRefSummaries[signal]
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
	if componentFieldMapping != nil && componentFieldMapping.AdditionalIdentifiers != nil {
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
type expressionRefAggregator struct {
	logger *zap.Logger

	datapointAttrSet map[string]struct{}
	spanAttrSet      map[string]struct{}
	metricAttrSet    map[string]struct{}

	// track if any valid AST was processed
	hasValidExpr bool
}

func newExpressionRefAggregator(logger *zap.Logger) *expressionRefAggregator {
	return &expressionRefAggregator{
		logger:           logger,
		datapointAttrSet: make(map[string]struct{}),
		spanAttrSet:      make(map[string]struct{}),
		metricAttrSet:    make(map[string]struct{}),
		hasValidExpr:     false,
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
// into the corresponding sets (datapointAttrSet, spanAttrSet, metricAttrSet).
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
			if len(ref.Path) >= 2 && ref.Path[0] == attributeMap {
				key := ref.Path[1]
				r.datapointAttrSet[key] = struct{}{}
			}
		case "span":
			if len(ref.Path) >= 2 && ref.Path[0] == attributeMap {
				key := ref.Path[1]
				r.spanAttrSet[key] = struct{}{}
			}
		case "metric":
			if len(ref.Path) >= 2 && ref.Path[0] == attributeMap {
				key := ref.Path[1]
				r.metricAttrSet[key] = struct{}{}
			}
		case "resource", "scope", "vars":
			// resource and scope are ignored (they are always fully included) and
			// vars refer to datapoints, spans or metrics data
		default:
			r.logger.Debug(
				"Unknown expression ref root detected; consider updating walkAST and toSummary",
				zap.String("root", ref.Root),
			)
		}
	}
}

func (r *expressionRefAggregator) toSummary() *types.ExpressionRefSummary {
	if !r.hasValidExpr {
		// no valid expressions were processed → return nil
		return nil
	}

	dp := setKeys(r.datapointAttrSet)
	sp := setKeys(r.spanAttrSet)
	met := setKeys(r.metricAttrSet)

	return types.NewExpressionRefSummary(dp, sp, met)
}

func setKeys(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
