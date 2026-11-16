package k8sresourcereceiver

import (
	"context"
	"errors"
	"reflect"
	"sort"
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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
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

type collectionRuntime struct {
	settings  receiver.Settings
	config    *Config
	client    dynamic.Interface
	discovery discovery.DiscoveryInterfaceWithContext
	metrics   metrics.Recorder

	ctx      context.Context //nolint:containedctx
	cancel   context.CancelFunc
	controls *collectionWatchRegistry
	registry *collectionWatchRegistry
	handover *collectionHandover
	trigger  chan struct{}
	dirty    atomic.Bool
	wg       sync.WaitGroup
	stopOnce sync.Once
	stopped  chan struct{}
	mu       sync.RWMutex
	view     collectionReportingSnapshot

	// Reconciliation owns these snapshots; failures retain their last successful values.
	catalog            collectionDiscoverySnapshot
	discoveryComplete  bool
	crds               []*unstructured.Unstructured
	declarations       []*resourcecollection.ResourceCollection
	declarationObjects []*unstructured.Unstructured
	controlErrors      map[collectionWatchKey]string
	statusRetries      map[string]collectionStatusRetry
	watchHealth        map[collectionWatchKey]collectionWatchHealth
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
		discovery: discovery.ToDiscoveryInterfaceWithContext(disc),
		trigger:   make(chan struct{}, 1), stopped: make(chan struct{}),
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
	r.controls.notify = func() { r.dirty.Store(true); r.enqueue() }
	r.controls.onChange = r.controlChanged
	transform := sanitizeObject(r.config.ConfigMapMaxDataSize)
	if r.config.CompactCache {
		transform = compactTransform(transform)
	}
	r.handover = newCollectionHandover(&collectionControlRegistry{
		target: r.registry, controls: r.controls, transform: transform,
	})
	if err := r.controls.reconcile([]collectionWatchSpec{
		{Key: collectionWatchKey{GVR: crdGVR}},
		{Key: collectionWatchKey{GVR: resourcecollection.GVR}, Source: ObjectSourceCR},
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
	if before != nil && after != nil && after.GetKind() == resourcecollection.Kind &&
		before.GetUID() == after.GetUID() && before.GetGeneration() == after.GetGeneration() &&
		reflect.DeepEqual(before.GetDeletionTimestamp(), after.GetDeletionTimestamp()) {
		return
	}
	r.dirty.Store(true)
	r.enqueue()
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
	controls := r.controls.snapshot()
	crdsReady, declarationsReady := false, false
	previousCRDs := collectionCRDKeys(r.catalog.CRDs)
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
			crdsReady = true
			for _, object := range observation.Objects {
				crd, ok := toUnstructured(object)
				if !ok {
					crdsReady = false
					break
				}
				var decoded apiextensionsv1.CustomResourceDefinition
				if err := convertUnstructuredToCRD(crd, &decoded); err != nil {
					crdsReady = false
					break
				}
				raw, typed = append(raw, crd), append(typed, &decoded)
			}
			if crdsReady {
				r.crds, r.catalog.CRDs = raw, typed
			}
		case resourcecollection.GVR:
			declarationsReady = true
			var declarations []*resourcecollection.ResourceCollection
			var raw []*unstructured.Unstructured
			for _, object := range observation.Objects {
				obj, ok := toUnstructured(object)
				if !ok {
					declarationsReady = false
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
				}
				declarations = append(declarations, decoded)
			}
			if declarationsReady {
				r.declarations, r.declarationObjects = declarations, raw
			}
		}
	}
	refresh := r.dirty.Swap(false)
	if refresh {
		r.refreshDiscovery()
	}
	plan := buildCollectionPlan(r.config, r.catalog, r.declarations)
	if crdsReady {
		current := collectionCRDKeys(r.catalog.CRDs)
		for key := range previousCRDs {
			if !current[key] {
				plan.RemovedResources = append(plan.RemovedResources, key)
			}
		}
	}
	r.registry.invalidateRemovedCRDs(plan.RemovedResources)
	if err := r.handover.update(plan); err != nil {
		if r.ctx.Err() == nil {
			r.settings.Logger.Warn("Could not reconcile collection watches", zap.Error(err))
		}
		return
	}
	effective, err := r.handover.read()
	if err != nil {
		return
	}
	observations := r.handover.registry.snapshot()
	r.recordWatchHealth(plan, controls, observations, refresh)
	view := collectionReportingSnapshot{
		CRDs: r.crds, Objects: effective.Objects,
		Complete: collectionBaselineSynced(plan, observations),
		Watched:  map[schema.GroupResource]bool{},
	}
	for _, spec := range plan.Watches {
		if spec.Source == ObjectSourceCR {
			view.Watched[spec.Key.GVR.GroupResource()] = true
		}
	}
	r.includeDeclarations(&view)
	view.AllowDelete = collectionDeletionPolicy(r.config, plan, r.catalog, observations,
		crdsReady, declarationsReady, r.discoveryComplete)
	r.mu.Lock()
	r.view = view
	r.mu.Unlock()
	if declarationsReady {
		r.publishStatuses(plan, observations)
	}
}

func collectionBaselineSynced(plan collectionPlan, observations []collectionWatchSnapshot) bool {
	watches := map[collectionWatchKey]collectionWatchSnapshot{}
	for _, observation := range observations {
		watches[observation.Spec.Key] = observation
	}
	for _, selection := range plan.Baseline {
		if selection.Contributor.Kind != collectionBaselineObject || selection.Reason == "PolicyDenied" {
			continue
		}
		if len(selection.Resolved) == 0 {
			return false
		}
		for _, resolved := range selection.Resolved {
			if resolved.Reason != "AwaitingObservation" {
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
	preferred, preferredErr := r.discovery.ServerPreferredResourcesWithContext(ctx)
	_, served, servedErr := r.discovery.ServerGroupsAndResourcesWithContext(ctx)
	r.catalog.PreferredResources = mergeCollectionDiscovery(r.catalog.PreferredResources, preferred, preferredErr)
	r.catalog.ServedResources = mergeCollectionDiscovery(r.catalog.ServedResources, served, servedErr)
	r.discoveryComplete = preferredErr == nil && servedErr == nil
	if preferredErr != nil || servedErr != nil {
		r.settings.Logger.Warn("Collection API discovery is incomplete; retaining known API resources",
			zap.Error(preferredErr), zap.NamedError("served_discovery_error", servedErr))
	}
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

func (r *collectionRuntime) includeDeclarations(view *collectionReportingSnapshot) {
	if r.config.isDeniedResource(resourcecollection.Group, resourcecollection.GVR.Resource) {
		return
	}
	group := view.Objects[resourcecollection.GVR]
	group.Source = ObjectSourceCR
	byName := map[string]resourceObject{}
	for _, object := range group.Objects {
		byName[object.GetName()] = object
	}
	transform := sanitizeObject(r.config.ConfigMapMaxDataSize)
	if r.config.CompactCache {
		transform = compactTransform(transform)
	}
	for _, object := range r.declarationObjects {
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
	view.Objects[resourcecollection.GVR] = group
	view.Watched[resourcecollection.GVR.GroupResource()] = true
}

func collectionDeletionPolicy(
	cfg *Config, plan collectionPlan, catalog collectionDiscoverySnapshot, observations []collectionWatchSnapshot,
	crdsReady, declarationsReady, discoveryComplete bool,
) func(ResourceChange) bool {
	custom := collectionCRDKeys(catalog.CRDs)
	ready := map[schema.GroupVersionResource]bool{}
	watches := map[collectionWatchKey]collectionWatchSnapshot{}
	for _, observation := range observations {
		watches[observation.Spec.Key] = observation
		ready[observation.Spec.Key.GVR] = ready[observation.Spec.Key.GVR] || collectionWatchObserved(observation)
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
			if !custom[gvr.GroupResource()] && gvr != resourcecollection.GVR {
				return true
			}
			if !declarationsReady || invalid {
				return false
			}
			if gvr == resourcecollection.GVR {
				return true
			}
		}
		for _, selection := range selections {
			matches := (selection.Group == gvr.Group || selection.Group == "") &&
				(selection.Resource == gvr.Resource || selection.Resource == "*")
			if !matches || selection.Reason == "PolicyDenied" {
				continue
			}
			if len(selection.Resolved) == 0 {
				return false
			}
			for _, resolved := range selection.Resolved {
				if resolved.Resource != gvr.GroupResource() {
					continue
				}
				if resolved.Reason != "AwaitingObservation" && resolved.Reason != "PolicyDenied" {
					return false
				}
				for _, key := range resolved.Watches {
					if !collectionWatchObserved(watches[key]) {
						return false
					}
				}
			}
		}
		return discoveryComplete || ready[gvr]
	}
}

func (r *collectionRuntime) publishStatuses(plan collectionPlan, observations []collectionWatchSnapshot) {
	current := map[string]*resourcecollection.ResourceCollection{}
	for _, declaration := range r.declarations {
		current[declaration.Name] = declaration
	}
	for _, result := range plan.Declarations {
		declaration := current[result.Name]
		status := collectionDeclarationStatus(result, observations)
		preserveCollectionTransition(&status, declaration.Status, time.Now())
		if reflect.DeepEqual(status, declaration.Status) {
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
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.view
}

func (r *collectionRuntime) ReadCRDs() []*unstructured.Unstructured {
	return r.collectionSnapshot().CRDs
}
func (r *collectionRuntime) ReadObjects() map[schema.GroupVersionResource]ObjectGroup {
	return r.collectionSnapshot().Objects
}
func (r *collectionRuntime) StaticWatchesSynced() bool { return r.collectionSnapshot().Complete }

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
