package k8scrdreceiver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/consumer"
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

// informerManager uses Kubernetes dynamic informers to watch CRDs and their custom resources,
// providing both life-cycle event updates and periodic cache-based snapshots.
type informerManager struct {
	settings         receiver.Settings
	config           *Config
	consumer         consumer.Logs
	dynamicClient    dynamic.Interface
	forbiddenTracker *forbiddenTracker

	// CRD informer
	crdInformer     cache.SharedIndexInformer
	crdInformerStop chan struct{}

	// CR informers — one per matching CRD
	mu          sync.RWMutex
	crInformers map[string]*crInformerEntry // key: formatGVRKey(gvr)

	// Lifecycle
	wg     sync.WaitGroup
	ctx    context.Context //nolint:containedctx
	cancel context.CancelFunc
}

func newInformerManager(
	settings receiver.Settings,
	config *Config,
	cons consumer.Logs,
	dynamicClient dynamic.Interface,
	ft *forbiddenTracker,
) *informerManager {
	return &informerManager{
		settings:         settings,
		config:           config,
		consumer:         cons,
		dynamicClient:    dynamicClient,
		forbiddenTracker: ft,
		crInformers:      make(map[string]*crInformerEntry),
	}
}

func (m *informerManager) Start(ctx context.Context) error {
	m.ctx, m.cancel = context.WithCancel(ctx)

	if err := m.startCRDInformer(); err != nil {
		return fmt.Errorf("failed to start CRD informer: %w", err)
	}

	return nil
}

func (m *informerManager) Shutdown(_ context.Context) error {
	if m.cancel != nil {
		m.cancel()
	}

	// Stop all CR informers
	var crCount int
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		crCount = len(m.crInformers)
		for _, entry := range m.crInformers {
			close(entry.stopCh)
		}
		m.crInformers = make(map[string]*crInformerEntry)
	}()

	// Stop CRD informer
	if m.crdInformerStop != nil {
		close(m.crdInformerStop)
	}

	m.wg.Wait()

	m.settings.Logger.Debug("Informer manager shutdown",
		zap.Int("cr_informers_stopped", crCount),
	)

	return nil
}

// startCRDInformer creates and starts the informer that watches CRDs.
// CRD events drive CR informer lifecycle: add -> start CR informer, delete-> stop it.
func (m *informerManager) startCRDInformer() error {
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return m.dynamicClient.Resource(crdGVR).List(m.ctx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return m.dynamicClient.Resource(crdGVR).Watch(m.ctx, options)
		},
	}

	// No resync for CRD informer — we only care about real events.
	// Snapshot loop handles periodic re-emission.
	m.crdInformer = cache.NewSharedIndexInformer(
		lw,
		&unstructured.Unstructured{},
		0,
		cache.Indexers{},
	)

	if _, err := m.crdInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    m.onCRDAdd,
		UpdateFunc: m.onCRDUpdate,
		DeleteFunc: m.onCRDDelete,
	}); err != nil {
		return fmt.Errorf("failed to add CRD event handler: %w", err)
	}

	m.crdInformerStop = make(chan struct{})

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.crdInformer.Run(m.crdInformerStop)
	}()

	// Wait for initial CRD list to sync — this triggers onCRDAdd for each existing CRD
	if !cache.WaitForCacheSync(m.ctx.Done(), m.crdInformer.HasSynced) {
		return fmt.Errorf("failed to sync CRD informer cache")
	}

	// Start periodic snapshot loop after initial sync
	m.wg.Add(1)
	go m.runSnapshotLoop()

	return nil
}

// --- CRD event handlers ---

func (m *informerManager) onCRDAdd(obj interface{}) {
	crdObj, ok := toUnstructured(obj)
	if !ok {
		m.settings.Logger.Error("Unexpected object type in CRD add event")
		return
	}

	apiGroup, found, err := unstructured.NestedString(crdObj.Object, "spec", "group")
	if err != nil || !found {
		m.settings.Logger.Error("Failed to extract API group from CRD", zap.Error(err))
		return
	}

	if !m.config.shouldWatchAPIGroup(apiGroup) {
		m.settings.Logger.Debug("Skipping CRD (API group filtered)",
			zap.String("name", crdObj.GetName()),
			zap.String("group", apiGroup),
		)
		return
	}

	// Emit CRD log
	if err := emitLog(m.ctx, m.consumer, crdObj, watch.Added, buildCRDLogRecord); err != nil {
		m.settings.Logger.Error("Failed to emit CRD add log", zap.Error(err))
	}

	gvr, err := m.crdToGVR(crdObj)
	if err != nil {
		m.settings.Logger.Error("Failed to extract GVR from CRD",
			zap.String("name", crdObj.GetName()),
			zap.Error(err),
		)
		return
	}

	if err := m.startCRInformer(gvr); err != nil {
		m.settings.Logger.Error("Failed to start CR informer",
			zap.String("gvr", formatGVRKey(gvr)),
			zap.Error(err),
		)
	}
}

func (m *informerManager) onCRDUpdate(oldObj, newObj interface{}) {
	newCRD, ok := toUnstructured(newObj)
	if !ok {
		m.settings.Logger.Error("Unexpected object type in CRD update event (new)")
		return
	}
	oldCRD, ok := toUnstructured(oldObj)
	if !ok {
		m.settings.Logger.Error("Unexpected object type in CRD update event (old)")
		return
	}

	apiGroup, _, _ := unstructured.NestedString(newCRD.Object, "spec", "group")
	if !m.config.shouldWatchAPIGroup(apiGroup) {
		m.settings.Logger.Debug("Skipping CRD update (API group filtered)",
			zap.String("name", newCRD.GetName()),
			zap.String("group", apiGroup),
		)
		return
	}

	// Emit CRD modified log
	if err := emitLog(m.ctx, m.consumer, newCRD, watch.Modified, buildCRDLogRecord); err != nil {
		m.settings.Logger.Error("Failed to emit CRD update log", zap.Error(err))
	}

	// Check for storage version change
	oldGVR, oldErr := m.crdToGVR(oldCRD)
	newGVR, newErr := m.crdToGVR(newCRD)
	if oldErr != nil || newErr != nil {
		m.settings.Logger.Debug("Failed to extract GVR for CRD version comparison",
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
	m.settings.Logger.Info("CRD storage version changed, restarting CR informer",
		zap.String("crd", newCRD.GetName()),
		zap.String("old_gvr", formatGVRKey(oldGVR)),
		zap.String("new_gvr", formatGVRKey(newGVR)),
	)

	m.stopCRInformer(oldGVR)
	m.forbiddenTracker.clear(oldGVR)

	if err := m.startCRInformer(newGVR); err != nil {
		m.settings.Logger.Error("Failed to restart CR informer after version change",
			zap.String("gvr", formatGVRKey(newGVR)),
			zap.Error(err),
		)
	}
}

func (m *informerManager) onCRDDelete(obj interface{}) {
	crdObj, ok := toUnstructured(obj)
	if !ok {
		// Handle tombstone
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			m.settings.Logger.Error("Unexpected object type in CRD delete event")
			return
		}
		crdObj, ok = toUnstructured(tombstone.Obj)
		if !ok {
			m.settings.Logger.Error("Tombstone contained unexpected object type")
			return
		}
	}

	apiGroup, _, _ := unstructured.NestedString(crdObj.Object, "spec", "group")
	if !m.config.shouldWatchAPIGroup(apiGroup) {
		m.settings.Logger.Debug("Skipping CRD delete (API group filtered)",
			zap.String("name", crdObj.GetName()),
			zap.String("group", apiGroup),
		)
		return
	}

	// Emit CRD deleted log
	if err := emitLog(m.ctx, m.consumer, crdObj, watch.Deleted, buildCRDLogRecord); err != nil {
		m.settings.Logger.Error("Failed to emit CRD delete log", zap.Error(err))
	}

	gvr, err := m.crdToGVR(crdObj)
	if err != nil {
		m.settings.Logger.Debug("Failed to extract GVR from deleted CRD",
			zap.String("name", crdObj.GetName()),
			zap.Error(err),
		)
		return
	}

	m.stopCRInformer(gvr)
	m.forbiddenTracker.clear(gvr)
}

// --- CR informer lifecycle ---

// startCRInformer creates and starts an informer for the given GVR.
// It performs a permission pre-check to avoid creating informers that will fail.
func (m *informerManager) startCRInformer(gvr schema.GroupVersionResource) error {
	key := formatGVRKey(gvr)

	if shouldRetry, retryIn := m.forbiddenTracker.shouldRetry(gvr); !shouldRetry {
		m.settings.Logger.Debug("Skipping forbidden resource",
			zap.String("gvr", key),
			zap.Duration("retry_in", retryIn),
		)
		return nil
	}

	// Check if already running
	m.mu.RLock()
	_, exists := m.crInformers[key]
	m.mu.RUnlock()
	if exists {
		return nil
	}

	// Permission pre-check — avoid creating an informer that will fail immediately
	_, err := m.dynamicClient.Resource(gvr).List(m.ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		if isPermissionDenied(err) {
			m.forbiddenTracker.markForbidden(gvr)
			m.settings.Logger.Info("Skipping CR informer - insufficient RBAC permissions",
				zap.String("gvr", key),
			)
			return nil
		}
		return fmt.Errorf("permission pre-check failed for %s: %w", key, err)
	}

	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return m.dynamicClient.Resource(gvr).List(m.ctx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return m.dynamicClient.Resource(gvr).Watch(m.ctx, options)
		},
	}

	informer := cache.NewSharedIndexInformer(
		lw,
		&unstructured.Unstructured{},
		m.config.Interval, // Resync triggers periodic AddFunc calls — serves as snapshot
		cache.Indexers{},
	)

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			m.onCREvent(obj, gvr, watch.Added)
		},
		UpdateFunc: func(_, newObj interface{}) {
			m.onCREvent(newObj, gvr, watch.Modified)
		},
		DeleteFunc: func(obj interface{}) {
			m.onCRDeleteEvent(obj, gvr)
		},
	}); err != nil {
		return fmt.Errorf("failed to add CR event handler for %s: %w", key, err)
	}

	stopCh := make(chan struct{})
	entry := &crInformerEntry{
		gvr:      gvr,
		informer: informer,
		stopCh:   stopCh,
	}

	// Double-check under write lock
	m.mu.Lock()
	if _, exists := m.crInformers[key]; exists {
		m.mu.Unlock()
		close(stopCh)
		return nil
	}
	m.crInformers[key] = entry
	m.mu.Unlock()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		informer.Run(stopCh)
	}()

	m.settings.Logger.Info("Started CR informer", zap.String("gvr", key))
	return nil
}

func (m *informerManager) stopCRInformer(gvr schema.GroupVersionResource) {
	key := formatGVRKey(gvr)

	m.mu.Lock()
	defer m.mu.Unlock()

	entry, exists := m.crInformers[key]
	if !exists {
		return
	}
	delete(m.crInformers, key)

	close(entry.stopCh)
	m.settings.Logger.Info("Stopped CR informer", zap.String("gvr", key))
}

// --- CR event handlers ---

func (m *informerManager) onCREvent(obj interface{}, gvr schema.GroupVersionResource, eventType watch.EventType) {
	cr, ok := toUnstructured(obj)
	if !ok {
		m.settings.Logger.Error("Unexpected object type in CR event",
			zap.String("gvr", formatGVRKey(gvr)),
		)
		return
	}

	if err := emitLog(m.ctx, m.consumer, cr, eventType, buildCRLogRecord); err != nil {
		m.settings.Logger.Error("Failed to emit CR log",
			zap.String("gvr", formatGVRKey(gvr)),
			zap.String("name", cr.GetName()),
			zap.Error(err),
		)
	}
}

func (m *informerManager) onCRDeleteEvent(obj interface{}, gvr schema.GroupVersionResource) {
	// Handle tombstones from DeletedFinalStateUnknown
	if _, ok := obj.(*unstructured.Unstructured); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			m.settings.Logger.Error("Unexpected object type in CR delete event",
				zap.String("gvr", formatGVRKey(gvr)),
			)
			return
		}
		obj = tombstone.Obj
	}

	m.onCREvent(obj, gvr, watch.Deleted)
}

// --- Periodic snapshot ---

// runSnapshotLoop periodically emits all CRDs and CRs from the informer caches.
// This ensures topology components are refreshed before TTL expiry.
func (m *informerManager) runSnapshotLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.performSnapshot()
		}
	}
}

func (m *informerManager) performSnapshot() {
	m.settings.Logger.Debug("Performing snapshot from informer caches")

	// Snapshot CRDs from CRD informer cache
	crdCount := m.snapshotCRDs()

	// Snapshot CRs from CR informer caches
	crCount := m.snapshotCRs()

	m.settings.Logger.Debug("Snapshot complete",
		zap.Int("crds", crdCount),
		zap.Int("crs", crCount),
	)
}

func (m *informerManager) snapshotCRDs() int {
	if m.crdInformer == nil {
		return 0
	}

	count := 0
	for _, obj := range m.crdInformer.GetStore().List() {
		crd, ok := toUnstructured(obj)
		if !ok {
			continue
		}

		apiGroup, _, _ := unstructured.NestedString(crd.Object, "spec", "group")
		if !m.config.shouldWatchAPIGroup(apiGroup) {
			continue
		}

		if err := emitLog(m.ctx, m.consumer, crd, watch.Added, buildCRDLogRecord); err != nil {
			m.settings.Logger.Error("Failed to emit CRD snapshot log",
				zap.String("name", crd.GetName()),
				zap.Error(err),
			)
		}
		count++
	}

	return count
}

func (m *informerManager) snapshotCRs() int {
	m.mu.RLock()
	entries := make([]*crInformerEntry, 0, len(m.crInformers))
	for _, entry := range m.crInformers {
		entries = append(entries, entry)
	}
	m.mu.RUnlock()

	total := 0
	for _, entry := range entries {
		if shouldRetry, _ := m.forbiddenTracker.shouldRetry(entry.gvr); !shouldRetry {
			continue
		}

		for _, obj := range entry.informer.GetStore().List() {
			cr, ok := toUnstructured(obj)
			if !ok {
				continue
			}

			if err := emitLog(m.ctx, m.consumer, cr, watch.Added, buildCRLogRecord); err != nil {
				m.settings.Logger.Error("Failed to emit CR snapshot log",
					zap.String("gvr", formatGVRKey(entry.gvr)),
					zap.String("name", cr.GetName()),
					zap.Error(err),
				)
			}
			total++
		}
	}

	return total
}

// --- Helpers ---

// crdToGVR extracts the GroupVersionResource from a CRD object.
func (m *informerManager) crdToGVR(crdObj *unstructured.Unstructured) (schema.GroupVersionResource, error) {
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
