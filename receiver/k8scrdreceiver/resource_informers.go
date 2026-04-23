package k8scrdreceiver

import (
	"context"
	"fmt"
	"sync"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/emit"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/tracker"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

// Compile-time check that ResourceInformers implements Informers.
var _ Informers = (*ResourceInformers)(nil)

// Informers abstracts the Kubernetes informer layer for reading CRDs and CRs.
// The collector reads current state through this interface, enabling the emission
// logic to be tested independently of the Kubernetes client.
type Informers interface {
	// ReadCRDs returns all matching CRDs from the CRD informer cache.
	ReadCRDs() []*unstructured.Unstructured

	// ReadCRs returns all CRs from active, synced CR informer caches,
	// grouped by GroupVersionResource.
	ReadCRs() map[schema.GroupVersionResource][]*unstructured.Unstructured

	// Start begins watching for CRDs and their custom resources.
	Start(ctx context.Context) error

	// Shutdown stops all informers and waits for goroutines to exit.
	Shutdown(ctx context.Context) error
}

// crdGVR is the GroupVersionResource for CustomResourceDefinitions.
//
//nolint:gochecknoglobals
var crdGVR = schema.GroupVersionResource{
	Group:    "apiextensions.k8s.io",
	Version:  "v1",
	Resource: "customresourcedefinitions",
}

// crInformerEntry wraps a CR informer with its lifecycle management.
type crInformerEntry struct {
	gvr      schema.GroupVersionResource
	informer cache.SharedIndexInformer
	stopCh   chan struct{}
}

// ResourceInformers manages Kubernetes dynamic informers for CRDs and their custom resources.
// It provides read access to informer caches and lifecycle management.
type ResourceInformers struct {
	settings         receiver.Settings
	config           *Config
	dynamicClient    dynamic.Interface
	forbiddenTracker *tracker.ForbiddenTracker

	// CRD informer
	crdInformer     cache.SharedIndexInformer
	crdInformerStop chan struct{}

	// CR informers — one per matching CRD
	mu          sync.RWMutex
	crInformers map[string]*crInformerEntry // key: emit.FormatGVRKey(gvr)

	// Lifecycle
	wg     sync.WaitGroup
	ctx    context.Context //nolint:containedctx
	cancel context.CancelFunc
}

func newResourceInformers(
	settings receiver.Settings,
	config *Config,
	dynamicClient dynamic.Interface,
	ft *tracker.ForbiddenTracker,
) *ResourceInformers {
	return &ResourceInformers{
		settings:         settings,
		config:           config,
		dynamicClient:    dynamicClient,
		forbiddenTracker: ft,
		crInformers:      make(map[string]*crInformerEntry),
	}
}

func (ri *ResourceInformers) Start(ctx context.Context) error {
	ri.ctx, ri.cancel = context.WithCancel(ctx) //nolint:gosec // cancel is called in Shutdown

	if err := ri.startCRDInformer(); err != nil {
		return fmt.Errorf("failed to start CRD informer: %w", err)
	}

	return nil
}

func (ri *ResourceInformers) Shutdown(_ context.Context) error {
	if ri.cancel != nil {
		ri.cancel()
	}

	// Stop all CR informers
	var crCount int
	func() {
		ri.mu.Lock()
		defer ri.mu.Unlock()
		crCount = len(ri.crInformers)
		for _, entry := range ri.crInformers {
			close(entry.stopCh)
		}
		ri.crInformers = make(map[string]*crInformerEntry)
	}()

	// Stop CRD informer
	if ri.crdInformerStop != nil {
		close(ri.crdInformerStop)
	}

	ri.wg.Wait()

	ri.settings.Logger.Debug("Resource informers shutdown",
		zap.Int("cr_informers_stopped", crCount),
	)

	return nil
}

// startCRDInformer creates and starts the informer that watches CRDs.
// CRD events drive CR informer lifecycle only — log emission is handled by the collector.
func (ri *ResourceInformers) startCRDInformer() error {
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return ri.dynamicClient.Resource(crdGVR).List(ri.ctx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return ri.dynamicClient.Resource(crdGVR).Watch(ri.ctx, options)
		},
	}

	// No resync for CRD informer — the collector handles periodic emission.
	ri.crdInformer = cache.NewSharedIndexInformer(
		lw,
		&unstructured.Unstructured{},
		0,
		cache.Indexers{},
	)

	// CRD handlers manage CR informer lifecycle only (no log emission).
	if _, err := ri.crdInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ri.onCRDAdd,
		UpdateFunc: ri.onCRDUpdate,
		DeleteFunc: ri.onCRDDelete,
	}); err != nil {
		return fmt.Errorf("failed to add CRD event handler: %w", err)
	}

	ri.crdInformerStop = make(chan struct{})

	ri.wg.Add(1)
	go func() {
		defer ri.wg.Done()
		ri.crdInformer.Run(ri.crdInformerStop)
	}()

	// Wait for initial CRD list to sync — this triggers onCRDAdd for each existing CRD,
	// which starts CR informers for matching API groups.
	if !cache.WaitForCacheSync(ri.ctx.Done(), ri.crdInformer.HasSynced) {
		return fmt.Errorf("failed to sync CRD informer cache")
	}

	// Wait for CR informers started during CRD sync to complete their initial list.
	if !ri.waitForCRInformersSync() {
		return fmt.Errorf("failed to sync CR informer caches")
	}

	return nil
}

// waitForCRInformersSync blocks until all active CR informers have completed their initial list.
func (ri *ResourceInformers) waitForCRInformersSync() bool {
	ri.mu.RLock()
	entries := make([]*crInformerEntry, 0, len(ri.crInformers))
	for _, entry := range ri.crInformers {
		entries = append(entries, entry)
	}
	ri.mu.RUnlock()

	for _, entry := range entries {
		if !cache.WaitForCacheSync(ri.ctx.Done(), entry.informer.HasSynced) {
			return false
		}
	}
	return true
}

// --- CRD event handlers (lifecycle management only, no log emission) ---

func (ri *ResourceInformers) onCRDAdd(obj interface{}) {
	crdObj, ok := toUnstructured(obj)
	if !ok {
		ri.settings.Logger.Error("Unexpected object type in CRD add event")
		return
	}

	apiGroup, found, err := unstructured.NestedString(crdObj.Object, "spec", "group")
	if err != nil || !found {
		ri.settings.Logger.Error("Failed to extract API group from CRD", zap.Error(err))
		return
	}

	if !ri.config.shouldWatchAPIGroup(apiGroup) {
		ri.settings.Logger.Debug("Skipping CRD (API group filtered)",
			zap.String("name", crdObj.GetName()),
			zap.String("group", apiGroup),
		)
		return
	}

	gvr, err := ri.crdToGVR(crdObj)
	if err != nil {
		ri.settings.Logger.Error("Failed to extract GVR from CRD",
			zap.String("name", crdObj.GetName()),
			zap.Error(err),
		)
		return
	}

	if err := ri.startCRInformer(gvr); err != nil {
		ri.settings.Logger.Error("Failed to start CR informer",
			zap.String("gvr", emit.FormatGVRKey(gvr)),
			zap.Error(err),
		)
	}
}

func (ri *ResourceInformers) onCRDUpdate(oldObj, newObj interface{}) {
	newCRD, ok := toUnstructured(newObj)
	if !ok {
		ri.settings.Logger.Error("Unexpected object type in CRD update event (new)")
		return
	}
	oldCRD, ok := toUnstructured(oldObj)
	if !ok {
		ri.settings.Logger.Error("Unexpected object type in CRD update event (old)")
		return
	}

	apiGroup, _, _ := unstructured.NestedString(newCRD.Object, "spec", "group")
	if !ri.config.shouldWatchAPIGroup(apiGroup) {
		return
	}

	// Check for storage version change
	oldGVR, oldErr := ri.crdToGVR(oldCRD)
	newGVR, newErr := ri.crdToGVR(newCRD)
	if oldErr != nil || newErr != nil {
		ri.settings.Logger.Debug("Failed to extract GVR for CRD version comparison",
			zap.String("name", newCRD.GetName()),
			zap.NamedError("old_err", oldErr),
			zap.NamedError("new_err", newErr),
		)
		return
	}

	if oldGVR == newGVR {
		return
	}

	// Version changed — restart CR informer
	ri.settings.Logger.Info("CRD storage version changed, restarting CR informer",
		zap.String("crd", newCRD.GetName()),
		zap.String("old_gvr", emit.FormatGVRKey(oldGVR)),
		zap.String("new_gvr", emit.FormatGVRKey(newGVR)),
	)

	ri.stopCRInformer(oldGVR)
	ri.forbiddenTracker.Clear(oldGVR)

	if err := ri.startCRInformer(newGVR); err != nil {
		ri.settings.Logger.Error("Failed to restart CR informer after version change",
			zap.String("gvr", emit.FormatGVRKey(newGVR)),
			zap.Error(err),
		)
	}
}

func (ri *ResourceInformers) onCRDDelete(obj interface{}) {
	crdObj, ok := toUnstructured(obj)
	if !ok {
		// Handle tombstone
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			ri.settings.Logger.Error("Unexpected object type in CRD delete event")
			return
		}
		crdObj, ok = toUnstructured(tombstone.Obj)
		if !ok {
			ri.settings.Logger.Error("Tombstone contained unexpected object type")
			return
		}
	}

	apiGroup, _, _ := unstructured.NestedString(crdObj.Object, "spec", "group")
	if !ri.config.shouldWatchAPIGroup(apiGroup) {
		return
	}

	gvr, err := ri.crdToGVR(crdObj)
	if err != nil {
		ri.settings.Logger.Debug("Failed to extract GVR from deleted CRD",
			zap.String("name", crdObj.GetName()),
			zap.Error(err),
		)
		return
	}

	ri.stopCRInformer(gvr)
	ri.forbiddenTracker.Clear(gvr)
}

// --- CR informer lifecycle ---

// startCRInformer creates and starts an informer for the given GVR.
// The informer has no event handlers — the collector reads its cache directly.
func (ri *ResourceInformers) startCRInformer(gvr schema.GroupVersionResource) error {
	key := emit.FormatGVRKey(gvr)

	if shouldRetry, retryIn := ri.forbiddenTracker.ShouldRetry(gvr); !shouldRetry {
		ri.settings.Logger.Debug("Skipping forbidden resource",
			zap.String("gvr", key),
			zap.Duration("retry_in", retryIn),
		)
		return nil
	}

	// Check if already running
	ri.mu.RLock()
	_, exists := ri.crInformers[key]
	ri.mu.RUnlock()
	if exists {
		return nil
	}

	// Permission pre-check — avoid creating an informer that will fail immediately
	_, err := ri.dynamicClient.Resource(gvr).List(ri.ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		if isPermissionDenied(err) {
			ri.forbiddenTracker.MarkForbidden(gvr)
			ri.settings.Logger.Info("Skipping CR informer - insufficient RBAC permissions",
				zap.String("gvr", key),
			)
			return nil
		}
		return fmt.Errorf("permission pre-check failed for %s: %w", key, err)
	}

	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return ri.dynamicClient.Resource(gvr).List(ri.ctx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return ri.dynamicClient.Resource(gvr).Watch(ri.ctx, options)
		},
	}

	// No resync and no event handlers — the collector reads the cache directly.
	informer := cache.NewSharedIndexInformer(
		lw,
		&unstructured.Unstructured{},
		0,
		cache.Indexers{},
	)

	stopCh := make(chan struct{})
	entry := &crInformerEntry{
		gvr:      gvr,
		informer: informer,
		stopCh:   stopCh,
	}

	// Double-check under write lock
	ri.mu.Lock()
	if _, exists := ri.crInformers[key]; exists {
		ri.mu.Unlock()
		close(stopCh)
		return nil
	}
	ri.crInformers[key] = entry
	ri.mu.Unlock()

	ri.wg.Add(1)
	go func() {
		defer ri.wg.Done()
		informer.Run(stopCh)
	}()

	ri.settings.Logger.Info("Started CR informer", zap.String("gvr", key))
	return nil
}

func (ri *ResourceInformers) stopCRInformer(gvr schema.GroupVersionResource) {
	key := emit.FormatGVRKey(gvr)

	ri.mu.Lock()
	defer ri.mu.Unlock()

	entry, exists := ri.crInformers[key]
	if !exists {
		return
	}
	delete(ri.crInformers, key)

	close(entry.stopCh)
	ri.settings.Logger.Info("Stopped CR informer", zap.String("gvr", key))
}

// --- Cache readers ---

// ReadCRDs reads all matching CRDs from the CRD informer cache.
func (ri *ResourceInformers) ReadCRDs() []*unstructured.Unstructured {
	if ri.crdInformer == nil {
		return nil
	}

	items := ri.crdInformer.GetStore().List()
	result := make([]*unstructured.Unstructured, 0, len(items))
	for _, obj := range items {
		crd, ok := toUnstructured(obj)
		if !ok {
			continue
		}

		apiGroup, _, _ := unstructured.NestedString(crd.Object, "spec", "group")
		if !ri.config.shouldWatchAPIGroup(apiGroup) {
			continue
		}

		result = append(result, crd)
	}
	return result
}

// ReadCRs reads all CRs from active, synced CR informer caches.
func (ri *ResourceInformers) ReadCRs() map[schema.GroupVersionResource][]*unstructured.Unstructured {
	ri.mu.RLock()
	entries := make([]*crInformerEntry, 0, len(ri.crInformers))
	for _, entry := range ri.crInformers {
		entries = append(entries, entry)
	}
	ri.mu.RUnlock()

	result := make(map[schema.GroupVersionResource][]*unstructured.Unstructured)
	for _, entry := range entries {
		// Skip informers that haven't completed initial sync.
		// waitForCRInformersSync only covers informers that existed at startup;
		// new CR informers started at runtime (when a CRD is added later) may
		// not have synced yet when the collector reads their cache.
		if !entry.informer.HasSynced() {
			continue
		}

		if shouldRetry, _ := ri.forbiddenTracker.ShouldRetry(entry.gvr); !shouldRetry {
			continue
		}

		var crs []*unstructured.Unstructured
		for _, obj := range entry.informer.GetStore().List() {
			cr, ok := toUnstructured(obj)
			if !ok {
				continue
			}
			crs = append(crs, cr)
		}
		if len(crs) > 0 {
			result[entry.gvr] = crs
		}
	}
	return result
}

// --- Helpers ---

// crdToGVR extracts the GroupVersionResource from a CRD object.
func (ri *ResourceInformers) crdToGVR(crdObj *unstructured.Unstructured) (schema.GroupVersionResource, error) {
	var crd apiextensionsv1.CustomResourceDefinition
	if err := convertUnstructuredToCRD(crdObj, &crd); err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("failed to convert to CRD: %w", err)
	}

	storageVersion := getStorageVersion(&crd)
	if storageVersion == "" {
		return schema.GroupVersionResource{}, fmt.Errorf("no storage version found for CRD %s", crd.Name)
	}

	return schema.GroupVersionResource{
		Group:    crd.Spec.Group,
		Version:  storageVersion,
		Resource: crd.Spec.Names.Plural,
	}, nil
}

// toUnstructured safely converts an interface{} to *unstructured.Unstructured.
func toUnstructured(obj interface{}) (*unstructured.Unstructured, bool) {
	u, ok := obj.(*unstructured.Unstructured)
	return u, ok
}
