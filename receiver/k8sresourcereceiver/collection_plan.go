package k8sresourcereceiver

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/resourcecollection"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

const (
	collectionAwaitingObservation = "AwaitingObservation"
	collectionPolicyDenied        = "PolicyDenied"
	collectionWatchBudgetExceeded = "WatchBudgetExceeded"
	collectionAPINotAvailable     = "APINotAvailable"
	collectionWatching            = "Watching"
	collectionBaselineDiscovery   = "baseline-discovery"
	collectionBaselineObject      = "baseline-object"
	collectionDeclaration         = "declaration"
)

type collectionContributor struct {
	Kind           string
	Declaration    string
	UID            types.UID
	Generation     int64
	SelectionIndex int
}

type collectionWatchKey struct {
	GVR           schema.GroupVersionResource
	Namespace     string
	LabelSelector string
	FieldSelector string
}

type collectionWatchSpec struct {
	Key          collectionWatchKey
	Namespaced   bool
	Source       ObjectSource
	Contributors []collectionContributor
}

type collectionResolvedSelection struct {
	Resource schema.GroupResource
	GVR      schema.GroupVersionResource
	State    string
	Reason   string
	Message  string
	Watches  []collectionWatchKey
}

type collectionSelectionResult struct {
	// Pending requests retain identity even when no watch can resolve.
	Contributor collectionContributor
	Group       string
	Resource    string
	Version     string
	State       string
	Reason      string
	Message     string
	Resolved    []collectionResolvedSelection
	Scope       collectionSelectionScope
}

type collectionSelectionScope struct {
	Namespaces, LabelSelector, FieldSelector string
}

type collectionDeclarationResult struct {
	Name       string
	UID        types.UID
	Generation int64
	Selections []collectionSelectionResult
	Reason     string
}

type collectionPlan struct {
	Watches               []collectionWatchSpec
	Baseline              []collectionSelectionResult
	Declarations          []collectionDeclarationResult
	DeniedObjects         []ObjectMatcher
	RemovedResources      []schema.GroupResource
	MaxDeclarationWatches int
}

type collectionPlanner struct {
	config             *Config
	catalog            collectionCatalog
	watches            map[collectionWatchKey]collectionWatchSpec
	declarationWatches int
}

// Inputs are snapshots; Config.Validate must have completed before planning.
func buildCollectionPlan(
	cfg *Config, discovery collectionDiscoverySnapshot, declarations []*resourcecollection.ResourceCollection,
) collectionPlan {
	planner := collectionPlanner{
		config: cfg, catalog: newCollectionCatalog(discovery),
		watches: map[collectionWatchKey]collectionWatchSpec{},
	}
	plan := collectionPlan{
		DeniedObjects:         append([]ObjectMatcher(nil), cfg.DeniedObjects...),
		MaxDeclarationWatches: cfg.MaxDeclarationWatches,
	}
	for _, key := range sortedCollectionResourceKeys(planner.catalog.baselineResources) {
		if cfg.shouldWatchAPIGroup(key.Group) {
			plan.Baseline = append(plan.Baseline, planner.resolve(
				collectionContributor{Kind: collectionBaselineDiscovery, SelectionIndex: -1},
				key, planner.catalog.baselineResources[key], nil, "", "",
			))
		}
	}
	for index, selection := range cfg.Objects {
		contributor := collectionContributor{Kind: collectionBaselineObject, SelectionIndex: index}
		gvr, namespaced, err := planner.catalog.resolveBaseline(selection)
		if err != nil {
			state, reason := resourcecollection.Pending, collectionAPINotAvailable
			var resolutionError *collectionResolutionError
			if errors.As(err, &resolutionError) {
				state, reason = resourcecollection.Blocked, resolutionError.reason
			}
			result := unresolvedCollectionSelection(contributor, selection.Group, selection.Name, state, reason, err.Error())
			result.Version = selection.Version
			plan.Baseline = append(plan.Baseline, result)
			continue
		}
		key := gvr.GroupResource()
		resource := planner.catalog.resources[key]
		resource.gvr, resource.namespaced = gvr, namespaced
		result := planner.resolve(
			contributor, key, resource,
			selection.Namespaces, selection.LabelSelector, selection.FieldSelector,
		)
		result.Version = selection.Version
		plan.Baseline = append(plan.Baseline, result)
	}
	ordered := append([]*resourcecollection.ResourceCollection(nil), declarations...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i] == nil || ordered[j] == nil {
			return ordered[i] != nil
		}
		if ordered[i].Name != ordered[j].Name {
			return ordered[i].Name < ordered[j].Name
		}
		return ordered[i].UID < ordered[j].UID
	})
	for _, declaration := range ordered {
		if declaration == nil {
			continue
		}
		result := collectionDeclarationResult{
			Name: declaration.Name, UID: declaration.UID, Generation: declaration.Generation,
		}
		if len(declaration.Spec.Resources) == 0 || len(declaration.Spec.Resources) > resourcecollection.MaxSelections {
			result.Reason = "InvalidDeclaration"
		} else {
			for index, selection := range declaration.Spec.Resources {
				contributor := collectionContributor{
					Kind: collectionDeclaration, Declaration: declaration.Name,
					UID: declaration.UID, Generation: declaration.Generation, SelectionIndex: index,
				}
				resolved := planner.resolveDeclaration(contributor, selection)
				resolved.Scope = collectionDeclarationScope(selection)
				result.Selections = append(result.Selections, resolved)
			}
		}
		plan.Declarations = append(plan.Declarations, result)
	}
	for _, spec := range planner.watches {
		plan.Watches = append(plan.Watches, spec)
	}
	sort.Slice(plan.Watches, func(i, j int) bool {
		return collectionWatchLess(plan.Watches[i].Key, plan.Watches[j].Key)
	})
	return plan
}

func collectionDeclarationScope(selection resourcecollection.Selection) collectionSelectionScope {
	namespaces := append([]string(nil), selection.Namespaces...)
	sort.Strings(namespaces)
	scope := collectionSelectionScope{
		Namespaces:    strings.Join(slices.Compact(namespaces), ","),
		FieldSelector: selection.FieldSelector,
	}
	if selector, err := fields.ParseSelector(selection.FieldSelector); err == nil {
		scope.FieldSelector = selector.String()
	}
	if selection.LabelSelector != nil {
		if selector, err := metav1.LabelSelectorAsSelector(selection.LabelSelector); err == nil {
			scope.LabelSelector = selector.String()
		} else {
			scope.LabelSelector = fmt.Sprintf("%#v", selection.LabelSelector)
		}
	}
	return scope
}

func (p *collectionPlanner) resolveDeclaration(
	contributor collectionContributor, selection resourcecollection.Selection,
) collectionSelectionResult {
	if selection.LabelSelector != nil {
		selection.LabelSelector = selection.LabelSelector.DeepCopy()
	}
	if err := resourcecollection.ValidateSelection(selection); err != nil {
		return unresolvedCollectionSelection(contributor, selection.Group, selection.Resource,
			resourcecollection.Blocked, "InvalidSelection", err.Error())
	}
	labelSelector := ""
	if selection.LabelSelector != nil {
		selector, _ := metav1.LabelSelectorAsSelector(selection.LabelSelector)
		labelSelector = selector.String()
	}
	result := collectionSelectionResult{
		Contributor: contributor, Group: selection.Group, Resource: selection.Resource,
	}
	if selection.Resource != "*" && p.denied(selection.Group, selection.Resource) {
		return unresolvedCollectionSelection(contributor, selection.Group, selection.Resource,
			resourcecollection.Blocked, collectionPolicyDenied, "The agent administrator excludes this resource")
	}
	for _, key := range p.catalog.keys() {
		if key.Group != selection.Group || (selection.Resource != "*" && key.Resource != selection.Resource) {
			continue
		}
		if p.catalog.resources[key].source != ObjectSourceCR {
			if selection.Resource == "*" {
				continue
			}
			return unresolvedCollectionSelection(contributor, selection.Group, selection.Resource,
				resourcecollection.Blocked, "NotCustomResource", "Only CRD-backed custom resources can be declared")
		}
		resource := p.catalog.forDeclaration(key)
		resolved := p.resolve(contributor, key, resource, selection.Namespaces, labelSelector, selection.FieldSelector)
		result.Resolved = append(result.Resolved, resolved.Resolved...)
	}
	if len(result.Resolved) == 0 {
		return unresolvedCollectionSelection(contributor, selection.Group, selection.Resource,
			resourcecollection.Pending, collectionAPINotAvailable, "No matching resource API is available")
	}
	aggregateCollectionResult(&result)
	return result
}

func (p *collectionPlanner) resolve(
	contributor collectionContributor, key schema.GroupResource, resource collectionResource,
	namespaces []string, labelSelector, fieldSelector string,
) collectionSelectionResult {
	result := collectionSelectionResult{Contributor: contributor, Group: key.Group, Resource: key.Resource}
	resolved := collectionResolvedSelection{Resource: key, GVR: resource.gvr}
	switch {
	case p.denied(key.Group, key.Resource):
		resolved.State, resolved.Reason = resourcecollection.Blocked, collectionPolicyDenied
		resolved.Message = "The agent administrator excludes this resource"
	case resource.gvr.Version == "":
		resolved.State, resolved.Reason = resourcecollection.Pending, collectionAPINotAvailable
		resolved.Message = "No usable served API version is available"
		if resource.source == ObjectSourceCR {
			resolved.Message = "No usable served storage-version API is available"
		}
	case !resource.namespaced && len(namespaces) > 0:
		resolved.State, resolved.Reason = resourcecollection.Blocked, "IncompatibleScope"
		resolved.Message = "Namespace filters cannot select a cluster-scoped resource"
	default:
		labelFilter, labelErr := labels.Parse(labelSelector)
		fieldFilter, fieldErr := fields.ParseSelector(fieldSelector)
		if labelErr != nil || fieldErr != nil {
			resolved.State, resolved.Reason = resourcecollection.Blocked, "InvalidSelection"
			resolved.Message = "A selection filter is invalid"
			break
		}
		scope := append([]string(nil), namespaces...)
		if len(scope) == 0 {
			scope = []string{""}
		}
		sort.Strings(scope)
		var keys []collectionWatchKey
		additional := 0
		for i, namespace := range scope {
			if i > 0 && namespace == scope[i-1] {
				continue
			}
			watchKey := collectionWatchKey{
				GVR: resource.gvr, Namespace: namespace,
				LabelSelector: labelFilter.String(), FieldSelector: fieldFilter.String(),
			}
			keys = append(keys, watchKey)
			if _, exists := p.watches[watchKey]; !exists && !sharesDeclarationControlWatch(watchKey) {
				additional++
			}
		}
		if contributor.Kind == collectionDeclaration &&
			p.declarationWatches+additional > p.config.MaxDeclarationWatches {
			// Admit every namespace for this type together; never silently collect a subset.
			resolved.State, resolved.Reason = resourcecollection.Blocked, collectionWatchBudgetExceeded
			resolved.Message = fmt.Sprintf("Resource needs %d additional watches; %d of %d declaration watches are in use. "+
				"Reduce the selection or increase max_declaration_watches.",
				additional, p.declarationWatches, p.config.MaxDeclarationWatches)
			break
		}
		if contributor.Kind == collectionDeclaration {
			p.declarationWatches += additional
		}
		for _, watchKey := range keys {
			spec := p.watches[watchKey]
			spec.Key, spec.Namespaced, spec.Source = watchKey, resource.namespaced, resource.source
			spec.Contributors = append(spec.Contributors, contributor)
			p.watches[watchKey] = spec
			resolved.Watches = append(resolved.Watches, watchKey)
		}
		// Resolution does not claim that the later informer has synchronized.
		resolved.State, resolved.Reason = resourcecollection.Pending, collectionAwaitingObservation
	}
	result.Resolved = []collectionResolvedSelection{resolved}
	aggregateCollectionResult(&result)
	return result
}

func (p *collectionPlanner) denied(group, resource string) bool {
	return p.config.isDeniedResource(group, resource)
}

func unresolvedCollectionSelection(
	contributor collectionContributor, group, resource, state, reason, message string,
) collectionSelectionResult {
	return collectionSelectionResult{
		Contributor: contributor, Group: group, Resource: resource,
		State: state, Reason: reason, Message: message,
	}
}

func aggregateCollectionResult(result *collectionSelectionResult) {
	result.State, result.Reason = resourcecollection.Pending, collectionAwaitingObservation
	for _, resolved := range result.Resolved {
		if resolved.State == resourcecollection.Blocked ||
			(result.State != resourcecollection.Blocked && resolved.Reason != collectionAwaitingObservation) {
			result.State, result.Reason = resolved.State, resolved.Reason
			result.Message = fmt.Sprintf("%s: %s", resolved.Resource.String(), resolved.Message)
			if resolved.State == resourcecollection.Blocked {
				break
			}
		}
	}
}

func collectionWatchLess(a, b collectionWatchKey) bool {
	if a.GVR != b.GVR {
		return formatGVR(a.GVR) < formatGVR(b.GVR)
	}
	if a.Namespace != b.Namespace {
		return a.Namespace < b.Namespace
	}
	if a.LabelSelector != b.LabelSelector {
		return a.LabelSelector < b.LabelSelector
	}
	return a.FieldSelector < b.FieldSelector
}
