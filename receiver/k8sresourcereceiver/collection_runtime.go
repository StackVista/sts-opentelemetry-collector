package k8sresourcereceiver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/resourcecollection"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
)

type collectionReportingSnapshot struct {
	CRDs        []*unstructured.Unstructured
	Objects     map[schema.GroupVersionResource]ObjectGroup
	Complete    bool
	Watched     map[schema.GroupResource]bool
	AllowDelete func(ResourceChange) bool
}

type collectionReportingSource interface {
	collectionSnapshot() collectionReportingSnapshot
}

type collectionStatusRetry struct {
	Contributor collectionContributor
	Next        time.Time
}

type collectionReportingState struct {
	Plan                                     collectionPlan
	Catalog                                  collectionDiscoverySnapshot
	CRDs, Declarations                       []*unstructured.Unstructured
	CRDsReady, DeclarationsReady, Discovered bool
}

type collectionRuntime struct {
	settings   receiver.Settings
	config     *Config
	client     dynamic.Interface
	discovery  discovery.DiscoveryInterfaceWithContext
	metrics    metrics.Recorder
	watchClock clock.Clock

	ctx             context.Context //nolint:containedctx
	cancel          context.CancelFunc
	controls        *collectionWatchRegistry
	registry        *collectionWatchRegistry
	handover        *collectionHandover
	controlRegistry *collectionControlRegistry
	trigger         chan struct{}
	dirty           atomic.Bool
	controlDirty    atomic.Bool
	wg              sync.WaitGroup
	stopOnce        sync.Once
	stopped         chan struct{}
	mu              sync.Mutex
	view            collectionReportingSnapshot
	reporting       *collectionReportingState

	// Reconciliation owns these snapshots; failures retain their last successful values.
	catalog                      collectionDiscoverySnapshot
	discoveryComplete            bool
	crds                         []*unstructured.Unstructured
	declarations                 []*resourcecollection.ResourceCollection
	declarationObjects           []*unstructured.Unstructured
	controlErrors                map[collectionWatchKey]string
	statusRetries                map[string]collectionStatusRetry
	watchHealth                  map[collectionWatchKey]collectionWatchHealth
	baselineHealth               map[int]collectionWatchHealth
	controlHealth                []collectionWatchSnapshot
	crdsReady, declarationsReady bool
	plan                         collectionPlan
}

func newCollectionRuntime(
	settings receiver.Settings, config *Config, client dynamic.Interface, disc discovery.DiscoveryInterface,
	recorder metrics.Recorder,
) *collectionRuntime {
	if recorder == nil {
		recorder = metrics.NoopRecorder{}
	}
	return &collectionRuntime{
		settings: settings, config: config, client: client,
		metrics: recorder, watchHealth: map[collectionWatchKey]collectionWatchHealth{},
		baselineHealth: map[int]collectionWatchHealth{},
		discovery:      discovery.ToDiscoveryInterfaceWithContext(disc),
		trigger:        make(chan struct{}, 1), stopped: make(chan struct{}),
		controlErrors: map[collectionWatchKey]string{}, statusRetries: map[string]collectionStatusRetry{},
		view: collectionReportingSnapshot{
			Objects: map[schema.GroupVersionResource]ObjectGroup{},
			AllowDelete: func(change ResourceChange) bool {
				return !change.IsCRD && config.isDeniedResource(change.GVR.Group, change.GVR.Resource)
			},
		},
	}
}

func (r *collectionRuntime) Start(ctx context.Context) error {
	r.ctx, r.cancel = context.WithCancel(ctx)
	if err := r.ctx.Err(); err != nil {
		return err
	}
	builder := newResourceInformers(r.settings, r.config, r.client, nil, nil, nil)
	r.registry = newCollectionWatchRegistry(r.ctx, r.client, builder.buildInformer)
	r.registry.notify = r.enqueue
	r.controls = newCollectionWatchRegistry(r.ctx, r.client, func(lw *cache.ListWatch) cache.SharedIndexInformer {
		return cache.NewSharedIndexInformer(
			cache.ToListWatcherWithWatchListSemantics(lw, r.client),
			&unstructured.Unstructured{}, 0, cache.Indexers{},
		)
	})
	r.controls.notify = func() { r.controlDirty.Store(true); r.enqueue() }
	if r.watchClock != nil {
		r.registry.clock, r.controls.clock = r.watchClock, r.watchClock
	}
	r.controls.onChange = r.controlChanged
	transform := sanitizeObject(r.config.ConfigMapMaxDataSize)
	if r.config.CompactCache {
		transform = compactTransform(transform)
	}
	r.controlRegistry = &collectionControlRegistry{
		target: r.registry, controls: r.controls, transform: transform,
	}
	r.handover = newCollectionHandover(r.controlRegistry)
	if err := r.controls.reconcile([]collectionWatchSpec{
		{Key: collectionWatchKey{GVR: crdGVR}},
		{Key: collectionWatchKey{GVR: resourcecollection.GVR()}, Source: ObjectSourceCR},
	}); err != nil {
		return err
	}
	r.dirty.Store(true)
	r.enqueue()
	r.wg.Add(1)
	go r.run()
	return nil
}

func (r *collectionRuntime) enqueue() {
	select {
	case r.trigger <- struct{}{}:
	default:
	}
}

func (r *collectionRuntime) controlChanged(old, current interface{}) {
	before, _ := toUnstructured(old)
	after, _ := toUnstructured(current)
	if tombstone, ok := old.(cache.DeletedFinalStateUnknown); ok {
		before, _ = toUnstructured(tombstone.Obj)
	}
	object := after
	if object == nil {
		object = before
	}
	changed := collectionControlIdentityChanged(before, after)
	if object != nil && object.GetKind() == "CustomResourceDefinition" {
		changed = collectionCRDDiscoveryChanged(before, after)
		if changed {
			r.dirty.Store(true)
		}
	}
	if !changed {
		// Reporting and status comparisons read current control objects from their stores.
		return
	}
	r.controlDirty.Store(true)
	r.enqueue()
}

func collectionControlIdentityChanged(before, after *unstructured.Unstructured) bool {
	return before == nil || after == nil || before.GetUID() != after.GetUID() ||
		before.GetGeneration() != after.GetGeneration() ||
		!reflect.DeepEqual(before.GetDeletionTimestamp(), after.GetDeletionTimestamp())
}

func collectionCRDDiscoveryChanged(before, after *unstructured.Unstructured) bool {
	if collectionControlIdentityChanged(before, after) {
		return true
	}
	// Establishment can expose an API without changing the CRD generation.
	conditions := func(object *unstructured.Unstructured) map[string]interface{} {
		result := map[string]interface{}{}
		items, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
		for _, item := range items {
			condition, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			kind, _ := condition["type"].(string)
			if kind == "Established" || kind == "NamesAccepted" {
				result[kind] = condition["status"]
			}
		}
		return result
	}
	return !reflect.DeepEqual(conditions(before), conditions(after))
}

func (r *collectionRuntime) run() {
	defer r.wg.Done()
	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	refresh := time.NewTicker(informerReconcileInterval)
	defer refresh.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-refresh.C:
			r.dirty.Store(true)
		case <-poll.C:
		case <-r.trigger:
		}
		if r.ctx.Err() == nil {
			r.reconcile()
		}
	}
}

func (r *collectionRuntime) reconcile() {
	// Poll health and status cheaply; assemble object snapshots only for reporting.
	controls := r.controls.healthSnapshot()
	reload := r.controlDirty.Swap(false) || !reflect.DeepEqual(controls, r.controlHealth)
	var previousCRDs map[schema.GroupResource]bool
	if reload {
		previousCRDs = collectionCRDKeys(r.catalog.CRDs)
		r.controlHealth = controls
		controls = r.controls.snapshot()
		r.readControls(controls)
	}
	refresh := r.dirty.Swap(false)
	if refresh {
		r.refreshDiscovery()
	}
	if reload || refresh {
		plan := buildCollectionPlan(r.config, r.catalog, r.declarations)
		if r.crdsReady {
			current := collectionCRDKeys(r.catalog.CRDs)
			for key := range previousCRDs {
				if !current[key] {
					plan.RemovedResources = append(plan.RemovedResources, key)
				}
			}
		}
		r.mu.Lock()
		r.registry.invalidateRemovedCRDs(plan.RemovedResources)
		err := r.handover.update(plan)
		if err == nil {
			r.plan = plan
			r.reporting = &collectionReportingState{
				Plan: plan, Catalog: r.catalog, CRDs: r.crds, Declarations: r.declarationObjects,
				CRDsReady: r.crdsReady, DeclarationsReady: r.declarationsReady, Discovered: r.discoveryComplete,
			}
		}
		r.mu.Unlock()
		if err != nil {
			r.controlDirty.Store(true)
			if r.ctx.Err() == nil {
				r.settings.Logger.Warn("Could not reconcile collection watches", zap.Error(err))
			}
			return
		}
	}
	observations := r.controlRegistry.healthSnapshot()
	r.recordWatchHealth(r.plan, controls, observations, reload || refresh)
	if r.declarationsReady {
		r.publishStatuses(r.plan, observations)
	}
}

func (r *collectionRuntime) readControls(controls []collectionWatchSnapshot) {
	r.crdsReady, r.declarationsReady = false, false
	for _, observation := range controls {
		key := observation.Spec.Key
		if message := observation.Message; r.controlErrors[key] != message {
			r.controlErrors[key] = message
			if message != "" {
				r.settings.Logger.Warn("Collection control API is unavailable; retaining known selections",
					zap.String("gvr", formatGVR(key.GVR)), zap.String("reason", observation.Reason),
					zap.String("message", message))
			}
		}
		if !collectionWatchObserved(observation) {
			continue
		}
		switch key.GVR {
		case crdGVR:
			var raw []*unstructured.Unstructured
			var typed []*apiextensionsv1.CustomResourceDefinition
			r.crdsReady = true
			for _, object := range observation.Objects {
				crd, ok := toUnstructured(object)
				if !ok {
					r.crdsReady = false
					break
				}
				var decoded apiextensionsv1.CustomResourceDefinition
				if err := convertUnstructuredToCRD(crd, &decoded); err != nil {
					r.crdsReady = false
					break
				}
				raw, typed = append(raw, crd), append(typed, &decoded)
			}
			if r.crdsReady {
				r.crds, r.catalog.CRDs = raw, typed
			}
		case resourcecollection.GVR():
			r.declarationsReady = true
			var declarations []*resourcecollection.ResourceCollection
			var raw []*unstructured.Unstructured
			for _, object := range observation.Objects {
				obj, ok := toUnstructured(object)
				if !ok {
					r.declarationsReady = false
					break
				}
				raw = append(raw, obj)
				if obj.GetDeletionTimestamp() != nil {
					continue
				}
				decoded, err := resourcecollection.Decode(obj)
				if err != nil {
					decoded = &resourcecollection.ResourceCollection{ObjectMeta: metav1.ObjectMeta{
						Name: obj.GetName(), UID: obj.GetUID(), Generation: obj.GetGeneration(),
					}}
					// Keep valid feedback when an invalid spec cannot decode.
					if status, found, statusErr := unstructured.NestedMap(obj.Object, "status"); found && statusErr == nil {
						_ = runtime.DefaultUnstructuredConverter.FromUnstructured(status, &decoded.Status)
					}
				}
				declarations = append(declarations, decoded)
			}
			if r.declarationsReady {
				r.declarations, r.declarationObjects = declarations, raw
			}
		}
	}
}

func collectionBaselineSynced(plan collectionPlan, observations []collectionWatchSnapshot) bool {
	watches := map[collectionWatchKey]collectionWatchSnapshot{}
	for _, observation := range observations {
		watches[observation.Spec.Key] = observation
	}
	for _, selection := range plan.Baseline {
		if selection.Contributor.Kind != collectionBaselineObject || selection.Reason == collectionPolicyDenied {
			continue
		}
		if len(selection.Resolved) == 0 {
			// Required YAML watches must synchronize before absence is authoritative.
			return false
		}
		for _, resolved := range selection.Resolved {
			if resolved.Reason != collectionAwaitingObservation {
				return false
			}
			for _, key := range resolved.Watches {
				if !collectionWatchObserved(watches[key]) {
					return false
				}
			}
		}
	}
	return true
}

func (r *collectionRuntime) refreshDiscovery() {
	ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	defer cancel()
	groups, served, servedErr := r.discovery.ServerGroupsAndResourcesWithContext(ctx)
	// Reuse client-go's per-resource fallback without fetching discovery twice.
	preferred, preferredErr := preferredCollectionResources(ctx, groups, served, servedErr)
	r.catalog.PreferredResources = mergeCollectionDiscovery(r.catalog.PreferredResources, preferred, preferredErr)
	r.catalog.ServedResources = mergeCollectionDiscovery(r.catalog.ServedResources, served, servedErr)
	r.discoveryComplete = preferredErr == nil && servedErr == nil
	if preferredErr != nil || servedErr != nil {
		r.settings.Logger.Warn("Collection API discovery is incomplete; retaining known API resources",
			zap.Error(preferredErr), zap.NamedError("served_discovery_error", servedErr))
	}
}

type collectionDiscoveryCache struct {
	discovery.DiscoveryInterfaceWithContext
	groups    *metav1.APIGroupList
	resources map[schema.GroupVersion]*metav1.APIResourceList
	failures  map[schema.GroupVersion]error
}

func (c *collectionDiscoveryCache) GroupsAndMaybeResourcesWithContext(
	ctx context.Context,
) (*metav1.APIGroupList, map[schema.GroupVersion]*metav1.APIResourceList, map[schema.GroupVersion]error, error) {
	return c.groups, c.resources, c.failures, ctx.Err()
}

func preferredCollectionResources(
	ctx context.Context, groups []*metav1.APIGroup, resources []*metav1.APIResourceList, err error,
) ([]*metav1.APIResourceList, error) {
	var failed *discovery.ErrGroupDiscoveryFailed
	if err != nil && !errors.As(err, &failed) {
		return nil, err
	}
	cached := &collectionDiscoveryCache{
		groups: &metav1.APIGroupList{}, resources: map[schema.GroupVersion]*metav1.APIResourceList{},
	}
	if failed != nil {
		cached.failures = failed.Groups
	}
	for _, group := range groups {
		cached.groups.Groups = append(cached.groups.Groups, *group)
	}
	for _, list := range resources {
		if list == nil {
			continue
		}
		if gv, parseErr := schema.ParseGroupVersion(list.GroupVersion); parseErr == nil {
			cached.resources[gv] = list
		}
	}
	return discovery.ServerPreferredResourcesWithContext(ctx, cached)
}

func mergeCollectionDiscovery(
	previous, current []*metav1.APIResourceList, err error,
) []*metav1.APIResourceList {
	if err == nil {
		return current
	}
	if !discovery.IsGroupDiscoveryFailedError(err) {
		return previous
	}
	byVersion := map[string]*metav1.APIResourceList{}
	var failed *discovery.ErrGroupDiscoveryFailed
	_ = errors.As(err, &failed)
	for _, list := range previous {
		if list != nil {
			byVersion[list.GroupVersion] = list
		}
	}
	for _, list := range current {
		if list != nil {
			gv, parseErr := schema.ParseGroupVersion(list.GroupVersion)
			if parseErr == nil && failed != nil && failed.Groups[gv] != nil {
				continue
			}
			byVersion[list.GroupVersion] = list
		}
	}
	keys := make([]string, 0, len(byVersion))
	for key := range byVersion {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]*metav1.APIResourceList, 0, len(keys))
	for _, key := range keys {
		result = append(result, byVersion[key])
	}
	return result
}

func collectionCRDKeys(crds []*apiextensionsv1.CustomResourceDefinition) map[schema.GroupResource]bool {
	result := map[schema.GroupResource]bool{}
	for _, crd := range crds {
		result[schema.GroupResource{Group: crd.Spec.Group, Resource: crd.Spec.Names.Plural}] = true
	}
	return result
}

func (r *collectionRuntime) includeDeclarations(
	view *collectionReportingSnapshot, declarations []*unstructured.Unstructured,
) {
	if r.config.isDeniedResource(resourcecollection.Group, resourcecollection.GVR().Resource) {
		return
	}
	group := view.Objects[resourcecollection.GVR()]
	group.Source = ObjectSourceCR
	byName := map[string]resourceObject{}
	for _, object := range group.Objects {
		byName[object.GetName()] = object
	}
	transform := sanitizeObject(r.config.ConfigMapMaxDataSize)
	if r.config.CompactCache {
		transform = compactTransform(transform)
	}
	for _, object := range declarations {
		cached, err := transform(object.DeepCopy())
		if err != nil {
			continue
		}
		if value, ok := toResourceObject(cached); ok {
			byName[value.GetName()] = value
		}
	}
	group.Objects = nil
	for _, object := range byName {
		group.Objects = append(group.Objects, object)
	}
	sort.Slice(group.Objects, func(i, j int) bool { return group.Objects[i].GetName() < group.Objects[j].GetName() })
	view.Objects[resourcecollection.GVR()] = group
	view.Watched[resourcecollection.GVR().GroupResource()] = true
}

func collectionDeletionPolicy(
	cfg *Config, plan collectionPlan, catalog collectionDiscoverySnapshot, observations []collectionWatchSnapshot,
	withdrawn []collectionDesiredContribution, crdsReady, declarationsReady, discoveryComplete bool,
) func(ResourceChange) bool {
	custom := collectionCRDKeys(catalog.CRDs)
	ready := map[schema.GroupVersionResource]bool{}
	watches := map[collectionWatchKey]collectionWatchSnapshot{}
	for _, observation := range observations {
		watches[observation.Spec.Key] = observation
		ready[observation.Spec.Key.GVR] = ready[observation.Spec.Key.GVR] || collectionWatchObserved(observation)
	}
	withdrawnBySelection := map[collectionContributionKey][]collectionDesiredContribution{}
	for _, previous := range withdrawn {
		key := collectionContributionID(previous.Contributor, schema.GroupResource{})
		withdrawnBySelection[key] = append(withdrawnBySelection[key], previous)
	}
	var selections []collectionSelectionResult
	selections = append(selections, plan.Baseline...)
	invalid := false
	for _, declaration := range plan.Declarations {
		invalid = invalid || declaration.Reason != ""
		selections = append(selections, declaration.Selections...)
	}
	return func(change ResourceChange) bool {
		if change.IsCRD {
			return crdsReady
		}
		gvr := change.GVR
		if cfg.isDeniedResource(gvr.Group, gvr.Resource) {
			return true
		}
		// A successful unfiltered watch proves absence in this exact representation.
		for _, observation := range observations {
			key := observation.Spec.Key
			if key.GVR == gvr && key.LabelSelector == "" && key.FieldSelector == "" &&
				(key.Namespace == "" || (change.Obj != nil && key.Namespace == change.Obj.GetNamespace())) &&
				collectionWatchObserved(observation) {
				return true
			}
		}
		if change.Source == ObjectSourceCR {
			if !crdsReady {
				return false
			}
			if !custom[gvr.GroupResource()] && gvr != resourcecollection.GVR() {
				return true
			}
			if !declarationsReady || invalid {
				// Invalid declarations cannot identify which prior filtered results they owned.
				return false
			}
			if gvr == resourcecollection.GVR() {
				return true
			}
		}
		for _, selection := range selections {
			matches := (selection.Group == gvr.Group || selection.Group == "") &&
				(selection.Resource == gvr.Resource || selection.Resource == "*")
			if !matches || selection.Reason == collectionPolicyDenied {
				continue
			}
			key := collectionContributionID(selection.Contributor, schema.GroupResource{})
			if collectionWithdrawnObject(withdrawnBySelection[key], change) {
				continue
			}
			if selection.Contributor.Kind == collectionDeclaration && change.Obj != nil &&
				!collectionSelectionCoversObject(selection.Scope, change.Obj) {
				continue
			}
			if len(selection.Resolved) == 0 {
				return false
			}
			for _, resolved := range selection.Resolved {
				if resolved.Resource != gvr.GroupResource() {
					continue
				}
				if resolved.Reason != collectionAwaitingObservation && resolved.Reason != collectionPolicyDenied {
					return false
				}
				for _, key := range resolved.Watches {
					if !collectionWatchObserved(watches[key]) {
						return false
					}
				}
			}
		}
		// Known declarations can withdraw a scope without waiting for its replacement API.
		return change.Source == ObjectSourceCR || discoveryComplete || ready[gvr]
	}
}

func collectionWithdrawnObject(withdrawn []collectionDesiredContribution, change ResourceChange) bool {
	if change.Obj == nil {
		return false
	}
	for _, previous := range withdrawn {
		for _, spec := range previous.Watches {
			if spec.Key.GVR == change.GVR {
				matches, known := collectionSelectionMatchesObject(previous.Scope, change.Obj)
				if known && matches {
					return true
				}
			}
		}
	}
	return false
}

func collectionSelectionCoversObject(scope collectionSelectionScope, object resourceObject) bool {
	matches, known := collectionSelectionMatchesObject(scope, object)
	return !known || matches
}

func collectionSelectionMatchesObject(scope collectionSelectionScope, object resourceObject) (bool, bool) {
	if scope.Namespaces != "" && !slices.Contains(strings.Split(scope.Namespaces, ","), object.GetNamespace()) {
		return false, true
	}
	if scope.LabelSelector == "" && scope.FieldSelector == "" {
		return true, true
	}
	decoded, err := decodeObject(object)
	if err != nil {
		return false, false
	}
	labelFilter, labelErr := labels.Parse(scope.LabelSelector)
	fieldFilter, fieldErr := fields.ParseSelector(scope.FieldSelector)
	if labelErr != nil || fieldErr != nil {
		return false, false
	}
	if !labelFilter.Matches(labels.Set(decoded.GetLabels())) {
		return false, true
	}
	values := fields.Set{}
	for _, requirement := range fieldFilter.Requirements() {
		value, found, _ := unstructured.NestedFieldNoCopy(decoded.Object, strings.Split(requirement.Field, ".")...)
		if found && value != nil {
			values[requirement.Field] = fmt.Sprint(value)
		}
	}
	return fieldFilter.Matches(values), true
}

func (r *collectionRuntime) publishStatuses(plan collectionPlan, observations []collectionWatchSnapshot) {
	current := map[string]*resourcecollection.ResourceCollection{}
	for _, declaration := range r.declarations {
		current[declaration.Name] = declaration
	}
	for _, result := range plan.Declarations {
		declaration := current[result.Name]
		object := r.controls.cachedObject(collectionWatchKey{GVR: resourcecollection.GVR()}, result.Name)
		if declaration == nil || object == nil || object.GetUID() != result.UID ||
			object.GetGeneration() != result.Generation || object.GetDeletionTimestamp() != nil {
			continue
		}
		var previous resourcecollection.Status
		if raw, found, _ := unstructured.NestedMap(object.Object, "status"); found {
			_ = runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &previous)
		}
		status := collectionDeclarationStatus(result, observations)
		preserveCollectionTransition(&status, previous, time.Now())
		if reflect.DeepEqual(status, previous) {
			delete(r.statusRetries, result.Name)
			continue
		}
		contributor := collectionContributor{Declaration: result.Name, UID: result.UID, Generation: result.Generation}
		retry := r.statusRetries[result.Name]
		if retry.Contributor == contributor && time.Now().Before(retry.Next) {
			continue
		}
		if err := writeCollectionStatus(r.ctx, r.client, result, status); err != nil {
			r.statusRetries[result.Name] = collectionStatusRetry{
				Contributor: contributor, Next: time.Now().Add(informerReconcileInterval),
			}
			if r.ctx.Err() == nil {
				r.settings.Logger.Warn("Could not update ResourceCollection status",
					zap.String("name", result.Name), zap.Error(err))
			}
		}
	}
	for name := range r.statusRetries {
		if current[name] == nil {
			delete(r.statusRetries, name)
		}
	}
}

func (r *collectionRuntime) collectionSnapshot() collectionReportingSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.reporting
	if state == nil {
		return r.view
	}
	effective, observations, err := r.handover.readSnapshot()
	if err != nil {
		return r.view
	}
	view := collectionReportingSnapshot{
		CRDs: state.CRDs, Objects: effective.Objects,
		Complete: collectionBaselineSynced(state.Plan, observations),
		Watched:  map[schema.GroupResource]bool{},
	}
	for _, spec := range state.Plan.Watches {
		if spec.Source == ObjectSourceCR {
			view.Watched[spec.Key.GVR.GroupResource()] = true
		}
	}
	declarations := state.Declarations
	for _, observation := range r.controls.snapshot() {
		if !observation.Synced {
			continue
		}
		objects := make([]*unstructured.Unstructured, 0, len(observation.Objects))
		valid := true
		for _, cached := range observation.Objects {
			object, ok := toUnstructured(cached)
			if !ok {
				valid = false
				break
			}
			objects = append(objects, object)
		}
		if valid {
			switch observation.Spec.Key.GVR {
			case crdGVR:
				view.CRDs = objects
			case resourcecollection.GVR():
				declarations = objects
			}
		}
	}
	r.includeDeclarations(&view, declarations)
	view.AllowDelete = collectionDeletionPolicy(r.config, state.Plan, state.Catalog, observations,
		effective.Withdrawn, state.CRDsReady, state.DeclarationsReady, state.Discovered)
	r.view = view
	return r.view
}

// Legacy accessors expose the last report; only collectionSnapshot advances handover.
func (r *collectionRuntime) ReadCRDs() []*unstructured.Unstructured {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.view.CRDs
}
func (r *collectionRuntime) ReadObjects() map[schema.GroupVersionResource]ObjectGroup {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.view.Objects
}
func (r *collectionRuntime) StaticWatchesSynced() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.view.Complete
}

func (r *collectionRuntime) Shutdown(ctx context.Context) error {
	r.stopOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		go func() {
			r.wg.Wait()
			if r.registry != nil {
				_ = r.registry.shutdown(context.Background())
			}
			if r.controls != nil {
				_ = r.controls.shutdown(context.Background())
			}
			close(r.stopped)
		}()
	})
	select {
	case <-r.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
