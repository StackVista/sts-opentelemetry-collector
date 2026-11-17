package k8sresourcereceiver

import (
	"sort"
	"sync"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/resourcecollection"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

type collectionRegistry interface {
	reconcile([]collectionWatchSpec) error
	snapshot() []collectionWatchSnapshot
}

type collectionContributionKey struct {
	Kind, Declaration string
	UID               types.UID
	SelectionIndex    int
	Resource          schema.GroupResource
}

type collectionDesiredContribution struct {
	Contributor     collectionContributor
	Group, Resource string
	Reason          string
	Watches         []collectionWatchSpec
}

type collectionActiveContribution struct {
	Request collectionDesiredContribution
	Objects map[collectionWatchKey][]resourceObject
}

type collectionEffectiveView struct {
	Objects  map[schema.GroupVersionResource]ObjectGroup
	Active   []collectionContributor
	Complete bool
}

type collectionHandover struct {
	mu       sync.Mutex
	registry collectionRegistry
	desired  map[collectionContributionKey]collectionDesiredContribution
	active   map[collectionContributionKey]collectionActiveContribution
}

func newCollectionHandover(registry collectionRegistry) *collectionHandover {
	return &collectionHandover{
		registry: registry,
		desired:  map[collectionContributionKey]collectionDesiredContribution{},
		active:   map[collectionContributionKey]collectionActiveContribution{},
	}
}

// The caller retains discovery on transient failures; an empty plan means withdrawal.
func (h *collectionHandover) update(plan collectionPlan) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	desired := desiredCollectionContributions(plan)
	for _, declaration := range plan.Declarations {
		if declaration.Reason != "InvalidDeclaration" {
			continue
		}
		for key, previous := range h.active {
			if key.Kind == collectionDeclaration && key.Declaration == declaration.Name && key.UID == declaration.UID {
				pending := previous.Request
				pending.Contributor.Generation = declaration.Generation
				pending.Reason, pending.Watches = declaration.Reason, nil
				desired[key] = pending
			}
		}
	}
	active := make(map[collectionContributionKey]collectionActiveContribution, len(h.active))
	for key, previous := range h.active {
		if !collectionContributionDenied(previous.Request, plan.DeniedObjects) &&
			!collectionContributionRemoved(previous.Request, plan.RemovedResources) &&
			retainCollectionContribution(key, previous.Request, desired) {
			active[key] = previous
		}
	}
	if err := h.registry.reconcile(collectionTransitionWatches(desired, active)); err != nil {
		return err
	}
	h.desired, h.active = desired, active
	return nil
}

func collectionContributionRemoved(request collectionDesiredContribution, removed []schema.GroupResource) bool {
	for _, spec := range request.Watches {
		if spec.Source != ObjectSourceCR {
			continue
		}
		for _, resource := range removed {
			if spec.Key.GVR.GroupResource() == resource {
				return true
			}
		}
	}
	return false
}

func (h *collectionHandover) read() (collectionEffectiveView, error) {
	view, _, err := h.readSnapshot()
	return view, err
}

func (h *collectionHandover) readSnapshot() (collectionEffectiveView, []collectionWatchSnapshot, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	snapshot := h.registry.snapshot()
	observations := map[collectionWatchKey]collectionWatchSnapshot{}
	for _, observation := range snapshot {
		observations[observation.Spec.Key] = observation
	}
	active := make(map[collectionContributionKey]collectionActiveContribution, len(h.active))
	for key, previous := range h.active {
		objects := make(map[collectionWatchKey][]resourceObject, len(previous.Objects))
		for watch, cached := range previous.Objects {
			objects[watch] = cached
		}
		previous.Objects = objects
		active[key] = previous
	}
	complete := true
	for key, desired := range h.desired {
		ready := desired.Reason == collectionAwaitingObservation && len(desired.Watches) > 0
		for _, spec := range desired.Watches {
			ready = ready && collectionWatchObserved(observations[spec.Key])
		}
		if !ready {
			complete = false
			continue
		}
		next := collectionActiveContribution{
			Request: desired, Objects: map[collectionWatchKey][]resourceObject{},
		}
		for _, spec := range desired.Watches {
			next.Objects[spec.Key] = observations[spec.Key].Objects
		}
		active[key] = next
	}
	for key, previous := range active {
		if _, wanted := h.desired[key]; !wanted && collectionFamilyReady(key, h.desired, observations) {
			delete(active, key)
			continue
		}
		for _, spec := range previous.Request.Watches {
			if observation := observations[spec.Key]; collectionWatchObserved(observation) {
				previous.Objects[spec.Key] = observation.Objects
			}
		}
	}
	// Retire old watches only after the replacement's effective view is available.
	if err := h.registry.reconcile(collectionTransitionWatches(h.desired, active)); err != nil {
		return collectionEffectiveView{}, nil, err
	}
	h.active = active
	return collectionView(active, observations, complete), snapshot, nil
}

func collectionWatchObserved(observation collectionWatchSnapshot) bool {
	return observation.Synced && observation.State == resourcecollection.Active
}

func collectionContributionID(
	contributor collectionContributor, resource schema.GroupResource,
) collectionContributionKey {
	return collectionContributionKey{
		Kind: contributor.Kind, Declaration: contributor.Declaration,
		UID: contributor.UID, SelectionIndex: contributor.SelectionIndex, Resource: resource,
	}
}

func sameCollectionFamily(a, b collectionContributionKey) bool {
	a.Resource, b.Resource = schema.GroupResource{}, schema.GroupResource{}
	return a == b
}

func collectionContributionDenied(request collectionDesiredContribution, denied []ObjectMatcher) bool {
	for _, spec := range request.Watches {
		for _, matcher := range denied {
			if matcher.Group == spec.Key.GVR.Group && matcher.Name == spec.Key.GVR.Resource {
				return true
			}
		}
	}
	return false
}

func desiredCollectionContributions(plan collectionPlan) map[collectionContributionKey]collectionDesiredContribution {
	specs := map[collectionWatchKey]collectionWatchSpec{}
	for _, spec := range plan.Watches {
		specs[spec.Key] = spec
	}
	result := map[collectionContributionKey]collectionDesiredContribution{}
	add := func(selection collectionSelectionResult) {
		split := selection.Resource == "*" || selection.Contributor.Kind == collectionBaselineDiscovery
		put := func(resource schema.GroupResource, reason string, keys []collectionWatchKey) {
			desired := collectionDesiredContribution{
				Contributor: selection.Contributor, Group: selection.Group, Resource: selection.Resource, Reason: reason,
			}
			for _, key := range keys {
				spec := copyCollectionWatchSpec(specs[key])
				spec.Contributors = []collectionContributor{selection.Contributor}
				desired.Watches = append(desired.Watches, spec)
			}
			result[collectionContributionID(selection.Contributor, resource)] = desired
		}
		if len(selection.Resolved) == 0 {
			resource := schema.GroupResource{}
			if selection.Contributor.Kind == collectionBaselineDiscovery {
				resource = schema.GroupResource{Group: selection.Group, Resource: selection.Resource}
			}
			put(resource, selection.Reason, nil)
			return
		}
		if split {
			for _, resolved := range selection.Resolved {
				put(resolved.Resource, resolved.Reason, resolved.Watches)
			}
			return
		}
		put(schema.GroupResource{}, selection.Reason, selection.Resolved[0].Watches)
	}
	for _, selection := range plan.Baseline {
		add(selection)
	}
	for _, declaration := range plan.Declarations {
		if declaration.Reason != "" {
			add(collectionSelectionResult{
				Contributor: collectionContributor{
					Kind: collectionDeclaration, Declaration: declaration.Name, UID: declaration.UID,
					Generation: declaration.Generation, SelectionIndex: -1,
				},
				Reason: declaration.Reason,
			})
		}
		for _, selection := range declaration.Selections {
			add(selection)
		}
	}
	return result
}

func retainCollectionContribution(
	key collectionContributionKey, previous collectionDesiredContribution,
	desired map[collectionContributionKey]collectionDesiredContribution,
) bool {
	if current, exists := desired[key]; exists {
		return current.Reason != collectionPolicyDenied
	}
	if key.Kind != collectionDeclaration {
		return false
	}
	for nextKey, next := range desired {
		if sameCollectionFamily(key, nextKey) && next.Reason != collectionPolicyDenied &&
			(next.Group != previous.Group || next.Resource != previous.Resource ||
				(next.Resource == "*" && nextKey.Resource == (schema.GroupResource{}))) {
			return true
		}
	}
	return false
}

func collectionFamilyReady(
	key collectionContributionKey, desired map[collectionContributionKey]collectionDesiredContribution,
	observations map[collectionWatchKey]collectionWatchSnapshot,
) bool {
	found := false
	for nextKey, next := range desired {
		if !sameCollectionFamily(key, nextKey) {
			continue
		}
		found = true
		if next.Reason != collectionAwaitingObservation || len(next.Watches) == 0 {
			return false
		}
		for _, spec := range next.Watches {
			if !collectionWatchObserved(observations[spec.Key]) {
				return false
			}
		}
	}
	return found
}

func collectionTransitionWatches(
	desired map[collectionContributionKey]collectionDesiredContribution,
	active map[collectionContributionKey]collectionActiveContribution,
) []collectionWatchSpec {
	merged := map[collectionWatchKey]collectionWatchSpec{}
	add := func(request collectionDesiredContribution) {
		for _, spec := range request.Watches {
			current := merged[spec.Key]
			contributors := append([]collectionContributor(nil), current.Contributors...)
			spec.Contributors = append(contributors, spec.Contributors...)
			merged[spec.Key] = spec
		}
	}
	for _, previous := range active {
		add(previous.Request)
	}
	for _, next := range desired {
		add(next)
	}
	result := make([]collectionWatchSpec, 0, len(merged))
	for _, spec := range merged {
		sortCollectionContributors(spec.Contributors)
		contributors := spec.Contributors[:0]
		for _, contributor := range spec.Contributors {
			if len(contributors) == 0 || contributor != contributors[len(contributors)-1] {
				contributors = append(contributors, contributor)
			}
		}
		spec.Contributors = contributors
		result = append(result, spec)
	}
	sort.Slice(result, func(i, j int) bool { return collectionWatchLess(result[i].Key, result[j].Key) })
	return result
}

func collectionView(
	active map[collectionContributionKey]collectionActiveContribution,
	observations map[collectionWatchKey]collectionWatchSnapshot, complete bool,
) collectionEffectiveView {
	watches := map[collectionWatchKey]collectionWatchSnapshot{}
	view := collectionEffectiveView{Objects: map[schema.GroupVersionResource]ObjectGroup{}, Complete: complete}
	for _, contribution := range active {
		view.Active = append(view.Active, contribution.Request.Contributor)
		for _, spec := range contribution.Request.Watches {
			watches[spec.Key] = collectionWatchSnapshot{
				Spec: spec, Objects: contribution.Objects[spec.Key],
				State: observations[spec.Key].State, Synced: observations[spec.Key].Synced,
			}
		}
	}
	keys := make([]collectionWatchKey, 0, len(watches))
	for key := range watches {
		keys = append(keys, key)
	}
	// Prefer current observations over retained failure data, then a stable watch order.
	sort.Slice(keys, func(i, j int) bool {
		a, b := collectionWatchObserved(watches[keys[i]]), collectionWatchObserved(watches[keys[j]])
		if a != b {
			return a
		}
		return collectionWatchLess(keys[i], keys[j])
	})
	seen := map[string]bool{}
	for _, key := range keys {
		group, exists := view.Objects[key.GVR]
		if !exists {
			group.Source = watches[key].Spec.Source
		}
		for _, object := range watches[key].Objects {
			identity := objectResourceKey(key.GVR, object.GetNamespace(), object.GetName())
			if !seen[identity] {
				group.Objects = append(group.Objects, object)
				seen[identity] = true
			}
		}
		view.Objects[key.GVR] = group
	}
	for gvr, group := range view.Objects {
		sort.Slice(group.Objects, func(i, j int) bool {
			a, b := group.Objects[i], group.Objects[j]
			if a.GetNamespace() != b.GetNamespace() {
				return a.GetNamespace() < b.GetNamespace()
			}
			return a.GetName() < b.GetName()
		})
		view.Objects[gvr] = group
	}
	sortCollectionContributors(view.Active)
	unique := view.Active[:0]
	for _, contributor := range view.Active {
		if len(unique) == 0 || contributor != unique[len(unique)-1] {
			unique = append(unique, contributor)
		}
	}
	view.Active = unique
	return view
}

func sortCollectionContributors(contributors []collectionContributor) {
	sort.Slice(contributors, func(i, j int) bool {
		a, b := contributors[i], contributors[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Declaration != b.Declaration {
			return a.Declaration < b.Declaration
		}
		if a.UID != b.UID {
			return a.UID < b.UID
		}
		if a.Generation != b.Generation {
			return a.Generation < b.Generation
		}
		return a.SelectionIndex < b.SelectionIndex
	})
}
