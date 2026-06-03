package k8sresourcereceiver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/emit"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/tracker"
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

const apiExtensionsGroup = "apiextensions.k8s.io"

// Informers abstracts the Kubernetes informer layer for reading CRDs and
// arbitrary Kubernetes objects (CRD-discovered CRs plus statically-configured
// resources). The collector reads current state through this interface, enabling
// the emission logic to be tested independently of the Kubernetes client.
type Informers interface {
	// ReadCRDs returns all matching CRDs from the CRD informer cache.
	ReadCRDs() []*unstructured.Unstructured

	// ReadObjects returns all objects from active, synced informer caches,
	// keyed by GVR. Each entry carries an ObjectSource identifying which
	// informer flavour the bucket came from. Overlap between CR-discovered
	// and static GVRs is rejected at receiver startup, so each GVR appears
	// in exactly one bucket.
	ReadObjects() map[schema.GroupVersionResource]ObjectGroup

	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

// crdGVR is the GroupVersionResource for CustomResourceDefinitions.
//
//nolint:gochecknoglobals
var crdGVR = schema.GroupVersionResource{
	Group:    apiExtensionsGroup,
	Version:  "v1",
	Resource: "customresourcedefinitions",
}

// informerReconcileInterval is how often the reconciler retries informers that
// failed to start (transient API errors, RBAC granted later).
const informerReconcileInterval = 1 * time.Minute

// staticInformerSyncTimeout bounds Start()'s wait for static informer caches.
const staticInformerSyncTimeout = 30 * time.Second

// staticForbiddenRetryInterval is the backoff before re-attempting a static
// informer entry that returned 403.
const staticForbiddenRetryInterval = tracker.DefaultRetryInterval

// resourceInformerEntry holds an informer plus the scoping fields that
// distinguish static-informer entries; CR entries leave them empty.
type resourceInformerEntry struct {
	gvr           schema.GroupVersionResource
	namespace     string // empty = cluster-wide
	labelSelector string
	fieldSelector string
	informer      cache.SharedIndexInformer
	stopCh        chan struct{}
	// source is the ObjectSource stamped on emitted objects. CR entries leave
	// this zero (ReadObjects fills in ObjectSourceCR). Static entries set it
	// at start time, normally ObjectSourceStatic, but ObjectSourceCR when the
	// GVR is backed by a CRD whose group is excluded from CR discovery — so
	// downstream emission keeps the CR-shape log schema.
	source ObjectSource
}

// ResourceInformers manages Kubernetes dynamic informers for CRDs, their custom
// resources, and statically-configured object watches. It provides read access
// to informer caches and lifecycle management.
type ResourceInformers struct {
	settings         receiver.Settings
	config           *Config
	dynamicClient    dynamic.Interface
	forbiddenTracker *tracker.ForbiddenTracker
	metrics          metrics.Recorder

	resolvedObjects []resolvedObjectWatch

	crdInformer     cache.SharedIndexInformer
	crdInformerStop chan struct{}

	// crInformers keyed by emit.FormatGVRKey; staticInformers keyed by
	// formatStaticInformerKey. Same lock so reconcile/read are consistent.
	informersMu     sync.RWMutex
	crInformers     map[string]*resourceInformerEntry
	staticInformers map[string]*resourceInformerEntry

	staticForbiddenMu sync.RWMutex
	staticForbidden   map[string]time.Time

	// Lifecycle
	wg     sync.WaitGroup
	ctx    context.Context //nolint:containedctx
	cancel context.CancelFunc
}

func newResourceInformers(
	settings receiver.Settings,
	config *Config,
	dynamicClient dynamic.Interface,
	resolvedObjects []resolvedObjectWatch,
	forbiddenTracker *tracker.ForbiddenTracker,
	metricsRecorder metrics.Recorder,
) *ResourceInformers {
	if metricsRecorder == nil {
		metricsRecorder = metrics.NoopRecorder{}
	}
	return &ResourceInformers{
		settings:         settings,
		config:           config,
		dynamicClient:    dynamicClient,
		resolvedObjects:  resolvedObjects,
		forbiddenTracker: forbiddenTracker,
		metrics:          metricsRecorder,
		crInformers:      make(map[string]*resourceInformerEntry),
		staticInformers:  make(map[string]*resourceInformerEntry),
		staticForbidden:  make(map[string]time.Time),
	}
}

func (ri *ResourceInformers) Start(ctx context.Context) error {
	ri.ctx, ri.cancel = context.WithCancel(ctx) //nolint:gosec // cancel is called in Shutdown

	if err := ri.startCRDInformer(); err != nil {
		return fmt.Errorf("failed to start CRD informer: %w", err)
	}

	// Per-watch failures are logged; the reconcile loop retries them. Sync wait
	// is bounded so one stuck watch can't hold up Start.
	ri.startStaticInformers()
	ri.waitForStaticInformersSync(staticInformerSyncTimeout)

	ri.wg.Add(1)
	go ri.runReconcileLoop(ri.ctx)

	return nil
}

func (ri *ResourceInformers) Shutdown(ctx context.Context) error {
	if ri.cancel != nil {
		ri.cancel()
	}

	var crCount, staticCount int
	func() {
		ri.informersMu.Lock()
		defer ri.informersMu.Unlock()
		crCount = len(ri.crInformers)
		for _, entry := range ri.crInformers {
			close(entry.stopCh)
		}
		ri.crInformers = make(map[string]*resourceInformerEntry)

		staticCount = len(ri.staticInformers)
		for _, entry := range ri.staticInformers {
			close(entry.stopCh)
		}
		ri.staticInformers = make(map[string]*resourceInformerEntry)
	}()

	// Stop CRD informer
	if ri.crdInformerStop != nil {
		close(ri.crdInformerStop)
	}

	// Bail out if the shutdown context expires — a hung informer must not block
	// the receiver.
	done := make(chan struct{})
	go func() {
		ri.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		ri.settings.Logger.Debug("Resource informers shutdown",
			zap.Int("cr_informers_stopped", crCount),
			zap.Int("static_informers_stopped", staticCount),
		)
		return nil
	case <-ctx.Done():
		ri.settings.Logger.Warn("Resource informers shutdown timed out, exiting with goroutines still running",
			zap.Int("cr_informers_stopped", crCount),
			zap.Int("static_informers_stopped", staticCount),
			zap.Error(ctx.Err()),
		)
		return ctx.Err()
	}
}

// startCRDInformer creates and starts the informer that watches CRDs.
// CRD events drive CR informer lifecycle only — log emission is handled by the collector.
func (ri *ResourceInformers) startCRDInformer() error {
	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return ri.dynamicClient.Resource(crdGVR).List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			return ri.dynamicClient.Resource(crdGVR).Watch(ctx, options)
		},
	}

	// resyncPeriod=0: skip the informer's periodic re-delivery of cached objects to
	// event handlers. The collector polls the informer cache every IncrementInterval,
	// so we don't need event-loop resync semantics on top.
	ri.crdInformer = cache.NewSharedIndexInformer(
		cache.ToListWatcherWithWatchListSemantics(lw, ri.dynamicClient),
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

	// Block until the initial List populates the CRD cache. This is the one-shot
	// startup sync (distinct from the resyncPeriod above), and it triggers
	// onCRDAdd for each existing CRD, which in turn starts CR informers.
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
	ri.informersMu.RLock()
	entries := make([]*resourceInformerEntry, 0, len(ri.crInformers))
	for _, entry := range ri.crInformers {
		entries = append(entries, entry)
	}
	ri.informersMu.RUnlock()

	for _, entry := range entries {
		if !cache.WaitForCacheSync(ri.ctx.Done(), entry.informer.HasSynced) {
			return false
		}
	}
	return true
}

// waitForStaticInformersSync waits up to timeout for static informer caches.
// Best-effort: stragglers keep retrying in the background and we log a Warn —
// blocking Start on one slow watch would be worse than the phantom deletes
// the operator may see on the first emit cycles for those GVRs.
func (ri *ResourceInformers) waitForStaticInformersSync(timeout time.Duration) {
	ri.informersMu.RLock()
	if len(ri.staticInformers) == 0 {
		ri.informersMu.RUnlock()
		return
	}
	entries := make([]*resourceInformerEntry, 0, len(ri.staticInformers))
	for _, entry := range ri.staticInformers {
		entries = append(entries, entry)
	}
	ri.informersMu.RUnlock()

	deadlineCtx, cancel := context.WithTimeout(ri.ctx, timeout)
	defer cancel()

	var wg sync.WaitGroup
	for _, entry := range entries {
		wg.Add(1)
		go func(e *resourceInformerEntry) {
			defer wg.Done()
			cache.WaitForCacheSync(deadlineCtx.Done(), e.informer.HasSynced)
		}(entry)
	}
	wg.Wait()

	var unsynced []string
	for _, e := range entries {
		if !e.informer.HasSynced() {
			unsynced = append(unsynced,
				formatStaticInformerKey(e.gvr, e.namespace, e.labelSelector, e.fieldSelector))
		}
	}
	if len(unsynced) > 0 {
		ri.settings.Logger.Warn("Some static informers did not sync within timeout; "+
			"continuing — they retry in the background, but first emit cycles may "+
			"emit phantom deletes for these GVRs until caches populate",
			zap.Strings("unsynced", unsynced),
			zap.Duration("timeout", timeout),
		)
	}
}

// runReconcileLoop periodically retries informer startup for CRDs and
// resolvedObjects that have no running informer. Recovers from transient
// startup errors and RBAC granted post-start. Informers that started but never
// synced are left alone — the k8s reflector retries those itself.
func (ri *ResourceInformers) runReconcileLoop(ctx context.Context) {
	defer ri.wg.Done()

	ticker := time.NewTicker(informerReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ri.reconcileCRInformers(ctx)
			ri.reconcileStaticInformers(ctx)
		}
	}
}

// reconcileCRInformers walks the CRD informer cache and attempts to start a CR
// informer for any CRD whose GVR is not currently in crInformers.
func (ri *ResourceInformers) reconcileCRInformers(ctx context.Context) {
	if ri.crdInformer == nil || !ri.crdInformer.HasSynced() {
		return
	}

	for _, obj := range ri.crdInformer.GetStore().List() {
		crd, ok := toUnstructured(obj)
		if !ok {
			continue
		}
		apiGroup, _, _ := unstructured.NestedString(crd.Object, "spec", "group")
		if !ri.config.shouldWatchAPIGroup(apiGroup) {
			continue
		}
		gvr, err := ri.crdToGVR(crd)
		if err != nil {
			continue
		}

		outcome, err := ri.startCRInformer(gvr)
		ri.metrics.RecordInformerReconcile(ctx, metrics.InformerKindCR, outcome)
		if err != nil {
			ri.settings.Logger.Debug("Reconciler failed to start CR informer",
				zap.String("gvr", emit.FormatGVRKey(gvr)),
				zap.Error(err),
			)
		}
	}
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

	if _, err := ri.startCRInformer(gvr); err != nil {
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

	if _, err := ri.startCRInformer(newGVR); err != nil {
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
//
// Returns the outcome (started, exists, forbidden, failed) and an error if the
// outcome is failed.
func (ri *ResourceInformers) startCRInformer(gvr schema.GroupVersionResource) (metrics.InformerOutcome, error) {
	key := emit.FormatGVRKey(gvr)

	if shouldRetry, retryIn := ri.forbiddenTracker.ShouldRetry(gvr); !shouldRetry {
		ri.settings.Logger.Debug("Skipping forbidden resource",
			zap.String("gvr", key),
			zap.Duration("retry_in", retryIn),
		)
		return metrics.InformerForbidden, nil
	}

	// Check if already running
	ri.informersMu.RLock()
	_, exists := ri.crInformers[key]
	ri.informersMu.RUnlock()
	if exists {
		return metrics.InformerExists, nil
	}

	// Permission pre-check — avoid creating an informer that will fail immediately
	_, err := ri.dynamicClient.Resource(gvr).List(ri.ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		if isPermissionDenied(err) {
			ri.forbiddenTracker.MarkForbidden(gvr)
			ri.settings.Logger.Info("Skipping CR informer - insufficient RBAC permissions",
				zap.String("gvr", key),
			)
			return metrics.InformerForbidden, nil
		}
		return metrics.InformerFailed, fmt.Errorf("permission pre-check failed for %s: %w", key, err)
	}

	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return ri.dynamicClient.Resource(gvr).List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			return ri.dynamicClient.Resource(gvr).Watch(ctx, options)
		},
	}

	informer := ri.buildInformer(lw)
	stopCh := make(chan struct{})
	entry := &resourceInformerEntry{
		gvr:      gvr,
		informer: informer,
		stopCh:   stopCh,
	}

	// Double-check under write lock
	ri.informersMu.Lock()
	if _, exists := ri.crInformers[key]; exists {
		ri.informersMu.Unlock()
		close(stopCh)
		return metrics.InformerExists, nil
	}
	ri.crInformers[key] = entry
	ri.informersMu.Unlock()

	ri.runInformer(informer, stopCh)

	ri.settings.Logger.Info("Started CR informer", zap.String("gvr", key))
	return metrics.InformerStarted, nil
}

func (ri *ResourceInformers) stopCRInformer(gvr schema.GroupVersionResource) {
	key := emit.FormatGVRKey(gvr)

	ri.informersMu.Lock()
	defer ri.informersMu.Unlock()

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

// ReadObjects merges objects from all synced CR and static informer caches by
// GVR. Unsynced informers are skipped — the collector treats "absent" as
// "deleted", so partial state must never be emitted. CR informers honor the
// GVR-wide ForbiddenTracker; static informers do not — their forbidden state
// is per-entry and enforced at start time only, since rechecking by GVR here
// would let a 403 on (pods, ns-a) suppress (pods, ns-b).
func (ri *ResourceInformers) ReadObjects() map[schema.GroupVersionResource]ObjectGroup {
	ri.informersMu.RLock()
	crEntries := make([]*resourceInformerEntry, 0, len(ri.crInformers))
	for _, entry := range ri.crInformers {
		crEntries = append(crEntries, entry)
	}
	staticEntries := make([]*resourceInformerEntry, 0, len(ri.staticInformers))
	for _, entry := range ri.staticInformers {
		staticEntries = append(staticEntries, entry)
	}
	ri.informersMu.RUnlock()

	result := make(map[schema.GroupVersionResource]ObjectGroup)

	for _, entry := range crEntries {
		// waitForCRInformersSync only covers informers that existed at startup;
		// informers started later (CRD added at runtime, reconciler retry) may not
		// have synced yet when the collector reads.
		if !entry.informer.HasSynced() {
			continue
		}
		if shouldRetry, _ := ri.forbiddenTracker.ShouldRetry(entry.gvr); !shouldRetry {
			continue
		}
		appendStoreObjects(result, entry, ObjectSourceCR)
	}

	for _, entry := range staticEntries {
		if !entry.informer.HasSynced() {
			continue
		}
		appendStoreObjects(result, entry, entry.source)
	}

	return result
}

// appendStoreObjects copies entry.informer.GetStore() into result under
// entry.gvr, tagged with source, skipping any item that isn't
// *unstructured.Unstructured. CR/static overlap is rejected at startup, so
// each GVR is owned by exactly one informer here.
func appendStoreObjects(
	result map[schema.GroupVersionResource]ObjectGroup,
	entry *resourceInformerEntry,
	source ObjectSource,
) {
	group := result[entry.gvr]
	group.Source = source
	for _, obj := range entry.informer.GetStore().List() {
		u, ok := toUnstructured(obj)
		if !ok {
			continue
		}
		group.Objects = append(group.Objects, u)
	}
	result[entry.gvr] = group
}

// --- Static informer lifecycle ---

// startStaticInformers walks resolvedObjects and attempts to start an informer
// for each (watch, namespace) pair. Failures are logged but do not abort
// startup — the reconcile loop retries them on a fixed cadence.
func (ri *ResourceInformers) startStaticInformers() {
	for i := range ri.resolvedObjects {
		ow := &ri.resolvedObjects[i]
		for _, namespace := range expandNamespaces(ow) {
			outcome, err := ri.startStaticInformer(ow, namespace)
			ri.metrics.RecordInformerReconcile(ri.ctx, metrics.InformerKindStatic, outcome)
			if err != nil {
				ri.settings.Logger.Error("Failed to start static informer",
					zap.String("gvr", emit.FormatGVRKey(ow.GVR)),
					zap.String("namespace", namespace),
					zap.Error(err),
				)
			}
		}
	}
}

// reconcileStaticInformers retries startStaticInformer for each resolvedObjects
// entry that does not yet have an informer in the staticInformers map. Mirrors
// reconcileCRInformers — only the "informer was never created" case is covered.
func (ri *ResourceInformers) reconcileStaticInformers(ctx context.Context) {
	for i := range ri.resolvedObjects {
		ow := &ri.resolvedObjects[i]
		for _, namespace := range expandNamespaces(ow) {
			outcome, err := ri.startStaticInformer(ow, namespace)
			ri.metrics.RecordInformerReconcile(ctx, metrics.InformerKindStatic, outcome)
			if err != nil {
				ri.settings.Logger.Debug("Reconciler failed to start static informer",
					zap.String("gvr", emit.FormatGVRKey(ow.GVR)),
					zap.String("namespace", namespace),
					zap.Error(err),
				)
			}
		}
	}
}

// startStaticInformer creates and starts one informer for an ObjectWatch
// scoped to namespace. A 403 marks only the (gvr, namespace, selectors) entry
// forbidden — never the GVR globally — so sibling namespaces stay watchable.
func (ri *ResourceInformers) startStaticInformer(
	ow *resolvedObjectWatch,
	namespace string,
) (metrics.InformerOutcome, error) {
	key := formatStaticInformerKey(ow.GVR, namespace, ow.LabelSelector, ow.FieldSelector)

	if shouldRetry, retryIn := ri.staticShouldRetry(key); !shouldRetry {
		ri.settings.Logger.Debug("Skipping forbidden static informer",
			zap.String("static_informer_key", key),
			zap.Duration("retry_in", retryIn),
		)
		return metrics.InformerForbidden, nil
	}

	ri.informersMu.RLock()
	_, exists := ri.staticInformers[key]
	ri.informersMu.RUnlock()
	if exists {
		return metrics.InformerExists, nil
	}

	// Permission pre-check — scoped to the same namespace + selectors the informer
	// will use, so a granted-in-namespace-A / denied-in-namespace-B split fails the
	// right entry without poisoning the GVR globally.
	listOpts := metav1.ListOptions{
		Limit:         1,
		LabelSelector: ow.LabelSelector,
		FieldSelector: ow.FieldSelector,
	}
	resourceClient := resourceClientFor(ri.dynamicClient, ow.GVR, namespace)
	if _, err := resourceClient.List(ri.ctx, listOpts); err != nil {
		if isPermissionDenied(err) {
			ri.staticMarkForbidden(key)
			ri.settings.Logger.Info("Skipping static informer - insufficient RBAC permissions",
				zap.String("static_informer_key", key),
			)
			return metrics.InformerForbidden, nil
		}
		return metrics.InformerFailed, fmt.Errorf("permission pre-check failed for %s: %w", key, err)
	}

	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			applyStaticSelectors(&options, ow)
			return resourceClientFor(ri.dynamicClient, ow.GVR, namespace).List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			applyStaticSelectors(&options, ow)
			return resourceClientFor(ri.dynamicClient, ow.GVR, namespace).Watch(ctx, options)
		},
	}

	informer := ri.buildInformer(lw)
	stopCh := make(chan struct{})
	source := ObjectSourceStatic
	if ow.CRDBacked {
		source = ObjectSourceCR
	}
	entry := &resourceInformerEntry{
		gvr:           ow.GVR,
		namespace:     namespace,
		labelSelector: ow.LabelSelector,
		fieldSelector: ow.FieldSelector,
		informer:      informer,
		stopCh:        stopCh,
		source:        source,
	}

	ri.informersMu.Lock()
	if _, exists := ri.staticInformers[key]; exists {
		ri.informersMu.Unlock()
		close(stopCh)
		return metrics.InformerExists, nil
	}
	ri.staticInformers[key] = entry
	ri.informersMu.Unlock()

	ri.runInformer(informer, stopCh)

	ri.settings.Logger.Info("Started static informer", zap.String("static_informer_key", key))
	return metrics.InformerStarted, nil
}

// expandNamespaces returns the list of namespaces the watch should expand into.
// A cluster-scoped resource always yields a single empty string. For namespaced
// resources, an empty Namespaces field yields a single empty string ("watch all
// namespaces via the cluster-wide List/Watch endpoint").
func expandNamespaces(ow *resolvedObjectWatch) []string {
	if !ow.Namespaced || len(ow.Namespaces) == 0 {
		return []string{""}
	}
	return ow.Namespaces
}

// resourceClientFor returns the dynamic client scoped to a namespace, or the
// cluster-wide client when namespace is empty.
func resourceClientFor(
	dc dynamic.Interface, gvr schema.GroupVersionResource, namespace string,
) dynamic.ResourceInterface {
	if namespace == "" {
		return dc.Resource(gvr)
	}
	return dc.Resource(gvr).Namespace(namespace)
}

// applyStaticSelectors overlays the watch's label/field selectors onto a
// ListOptions. The informer framework may pass its own LabelSelector/
// FieldSelector for resync filtering; ours take precedence because the watch
// explicitly opted into them.
func applyStaticSelectors(opts *metav1.ListOptions, ow *resolvedObjectWatch) {
	if ow.LabelSelector != "" {
		opts.LabelSelector = ow.LabelSelector
	}
	if ow.FieldSelector != "" {
		opts.FieldSelector = ow.FieldSelector
	}
}

// formatStaticInformerKey produces a stable key for a static informer entry.
// Includes namespace + selectors so two watches differing only by namespace or
// selector each get their own informer (and their own forbidden state).
func formatStaticInformerKey(gvr schema.GroupVersionResource, namespace, labelSelector, fieldSelector string) string {
	return fmt.Sprintf("%s|ns=%s|labels=%s|fields=%s",
		emit.FormatGVRKey(gvr), namespace, labelSelector, fieldSelector)
}

// staticShouldRetry reports whether a static informer entry may be
// (re-)attempted. When false, the returned duration is the remaining backoff.
func (ri *ResourceInformers) staticShouldRetry(key string) (bool, time.Duration) {
	ri.staticForbiddenMu.RLock()
	lastAttempt, isForbidden := ri.staticForbidden[key]
	ri.staticForbiddenMu.RUnlock()

	if !isForbidden {
		return true, 0
	}
	elapsed := time.Since(lastAttempt)
	if elapsed >= staticForbiddenRetryInterval {
		return true, 0
	}
	return false, staticForbiddenRetryInterval - elapsed
}

// staticMarkForbidden records a 403 against an entry so staticShouldRetry can
// enforce the backoff window.
func (ri *ResourceInformers) staticMarkForbidden(key string) {
	ri.staticForbiddenMu.Lock()
	ri.staticForbidden[key] = time.Now()
	ri.staticForbiddenMu.Unlock()
}

// --- Helpers ---

// buildInformer constructs a SharedIndexInformer for unstructured objects with
// no resync (the collector reads the cache directly on its own cadence) and no
// indexers (lookups are full-store scans, not indexed queries).
func (ri *ResourceInformers) buildInformer(lw *cache.ListWatch) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		cache.ToListWatcherWithWatchListSemantics(lw, ri.dynamicClient),
		&unstructured.Unstructured{},
		0,
		cache.Indexers{},
	)
}

// runInformer launches informer.Run in a tracked goroutine so Shutdown can wait
// for it via the shared WaitGroup.
func (ri *ResourceInformers) runInformer(informer cache.SharedIndexInformer, stopCh chan struct{}) {
	ri.wg.Add(1)
	go func() {
		defer ri.wg.Done()
		informer.Run(stopCh)
	}()
}

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
