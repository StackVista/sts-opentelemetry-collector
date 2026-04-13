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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	// initialBackoff is the initial retry delay for CRD watcher failures
	initialBackoff = time.Second
	// maxBackoff is the maximum retry delay for CRD watcher failures
	maxBackoff = 5 * time.Minute
)

// watchMode implements the mode interface for real-time CRD/CR watching.
type watchMode struct {
	settings         receiver.Settings
	config           *Config
	consumer         consumer.Logs
	client           k8sClient
	forbiddenTracker *forbiddenTracker

	mu         sync.Mutex
	crdWatcher watch.Interface
	crWatchers map[string]*crWatcher // key: group/version/resource

	wg     sync.WaitGroup
	stopCh chan struct{}

	//nolint:containedctx
	ctx    context.Context
	cancel context.CancelFunc
}

// crWatcher tracks a single CR watch for a specific CRD.
type crWatcher struct {
	gvr     schema.GroupVersionResource
	watcher watch.Interface
	stopCh  chan struct{}
}

// Stop gracefully stops the CR watcher
func (w *crWatcher) Stop() {
	close(w.stopCh)
	w.watcher.Stop()
}

func newWatchMode(
	settings receiver.Settings,
	config *Config,
	cons consumer.Logs,
	client k8sClient,
	ft *forbiddenTracker,
) *watchMode {
	return &watchMode{
		settings:         settings,
		config:           config,
		consumer:         cons,
		client:           client,
		forbiddenTracker: ft,
		crWatchers:       make(map[string]*crWatcher),
	}
}

func (m *watchMode) Start(ctx context.Context) error {
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.stopCh = make(chan struct{})

	if !m.config.Pull.Enabled {
		m.settings.Logger.Warn("Watch mode without pull mode: resources that fail due to insufficient RBAC permissions will not be retried automatically. Enable pull mode for periodic retry of forbidden resources.")
	}

	return m.startCRDWatcher()
}

func (m *watchMode) Shutdown(_ context.Context) error {
	if m.cancel != nil {
		m.cancel()
	}
	if m.stopCh != nil {
		close(m.stopCh)
	}

	// Stop all CR watchers
	m.mu.Lock()
	for _, crw := range m.crWatchers {
		crw.Stop()
	}
	crWatcherCount := len(m.crWatchers)
	m.crWatchers = make(map[string]*crWatcher)
	m.mu.Unlock()

	// Stop CRD watcher
	if m.crdWatcher != nil {
		m.crdWatcher.Stop()
	}

	m.settings.Logger.Debug("Watch mode shutdown",
		zap.Int("cr_watchers_stopped", crWatcherCount),
	)

	m.wg.Wait()
	return nil
}

// startCRDWatcher begins watching CRDs, listing initial state if configured,
// then running the CRD watch loop with retry in a background goroutine.
func (m *watchMode) startCRDWatcher() error {
	crdGVR := schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}

	watcher, err := m.client.Resource(crdGVR).Watch(m.ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to watch CRDs: %w", err)
	}
	m.crdWatcher = watcher

	// List initial state if configured
	if m.config.Watch.IncludeInitialState {
		if err := m.listInitialCRDs(crdGVR); err != nil {
			m.settings.Logger.Error("Failed to list initial CRDs", zap.Error(err))
		}
	}

	// Run CRD watch loop with retry in background
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.runCRDWatchLoop(crdGVR)
	}()

	return nil
}

// listInitialCRDs lists existing CRDs and processes them as Added events.
func (m *watchMode) listInitialCRDs(crdGVR schema.GroupVersionResource) error {
	list, err := m.client.Resource(crdGVR).List(m.ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for i := range list.Items {
		obj := &list.Items[i]

		shouldEmit, err := m.handleCRDEvent(obj, watch.Added)
		if err != nil {
			m.settings.Logger.Error("Failed to handle initial CRD",
				zap.String("name", obj.GetName()),
				zap.Error(err),
			)
			continue
		}

		if shouldEmit {
			if err := emitLog(m.ctx, m.consumer, obj, watch.Added, buildCRDLogRecord); err != nil {
				m.settings.Logger.Error("Failed to emit initial CRD log", zap.Error(err))
			}
		}
	}

	return nil
}

// runCRDWatchLoop runs the CRD watch loop with exponential backoff retry.
func (m *watchMode) runCRDWatchLoop(crdGVR schema.GroupVersionResource) {
	backoff := initialBackoff

	for {
		if err := m.processCRDEvents(); err != nil {
			m.settings.Logger.Warn("CRD watcher stopped, will retry",
				zap.Error(err),
				zap.Duration("backoff", backoff),
			)

			select {
			case <-m.stopCh:
				return
			case <-m.ctx.Done():
				return
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)

				// Recreate watcher
				newWatcher, err := m.client.Resource(crdGVR).Watch(m.ctx, metav1.ListOptions{})
				if err != nil {
					m.settings.Logger.Error("Failed to recreate CRD watcher", zap.Error(err))
					continue
				}
				m.crdWatcher = newWatcher
				m.settings.Logger.Info("CRD watcher recreated successfully")
				continue
			}
		}
		return // Clean exit (stopCh closed)
	}
}

// processCRDEvents reads from the CRD watcher channel and handles each event.
// Returns nil on clean shutdown (stopCh closed), error on channel closure.
func (m *watchMode) processCRDEvents() error {
	for {
		select {
		case <-m.stopCh:
			return nil

		case event, ok := <-m.crdWatcher.ResultChan():
			if !ok {
				return fmt.Errorf("CRD watcher channel closed")
			}

			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				m.settings.Logger.Error("Unexpected object type in CRD watch event")
				continue
			}

			// shouldEmit and err are independent: a CRD can match filters (shouldEmit=true)
			// even if a side effect like starting a CR watcher fails (err!=nil).
			shouldEmit, err := m.handleCRDEvent(obj, event.Type)
			if err != nil {
				m.settings.Logger.Error("Failed to handle CRD event", zap.Error(err))
			}

			if shouldEmit {
				if err := emitLog(m.ctx, m.consumer, obj, event.Type, buildCRDLogRecord); err != nil {
					m.settings.Logger.Error("Failed to emit CRD log", zap.Error(err))
				}
			}
		}
	}
}

// handleCRDEvent dispatches a CRD event to the appropriate handler.
// Returns whether a log should be emitted for this event.
func (m *watchMode) handleCRDEvent(obj *unstructured.Unstructured, eventType watch.EventType) (bool, error) {
	switch eventType {
	case watch.Added:
		return m.handleCRDAdded(obj)
	case watch.Deleted:
		return m.handleCRDDeleted(obj)
	case watch.Modified:
		return m.handleCRDModified(obj)
	case watch.Error:
		return false, fmt.Errorf("error event in CRD watcher")
	default:
		return false, nil
	}
}

func (m *watchMode) handleCRDAdded(crdObj *unstructured.Unstructured) (bool, error) {
	apiGroup, found, err := unstructured.NestedString(crdObj.Object, "spec", "group")
	if err != nil || !found {
		return false, fmt.Errorf("failed to extract API group from CRD: %w", err)
	}

	if !m.config.shouldWatchAPIGroup(apiGroup) {
		m.settings.Logger.Debug("Skipping CRD (API group not in filters)",
			zap.String("name", crdObj.GetName()),
			zap.String("group", apiGroup),
		)
		return false, nil
	}

	var crd apiextensionsv1.CustomResourceDefinition
	if err := convertUnstructuredToCRD(crdObj, &crd); err != nil {
		return false, fmt.Errorf("failed to convert to CRD: %w", err)
	}

	storageVersion := getStorageVersion(&crd)
	if storageVersion == "" {
		return false, fmt.Errorf("no storage version found for CRD %s", crd.Name)
	}

	gvr := schema.GroupVersionResource{
		Group:    crd.Spec.Group,
		Version:  storageVersion,
		Resource: crd.Spec.Names.Plural,
	}

	if shouldRetry, retryIn := m.forbiddenTracker.shouldRetry(gvr); !shouldRetry {
		m.settings.Logger.Debug("Skipping forbidden resource (requires pull mode or CRD change to retry)",
			zap.String("gvr", formatGVRKey(gvr)),
			zap.Duration("forbidden_ttl_remaining", retryIn),
		)
		return false, nil
	}

	m.settings.Logger.Info("Starting CR watcher for new CRD",
		zap.String("name", crd.Name),
		zap.String("group", crd.Spec.Group),
		zap.String("version", storageVersion),
		zap.String("resource", crd.Spec.Names.Plural),
	)

	if err := m.startCRWatcher(gvr); err != nil {
		return true, err
	}
	return true, nil
}

func (m *watchMode) handleCRDDeleted(crdObj *unstructured.Unstructured) (bool, error) {
	apiGroup, found, err := unstructured.NestedString(crdObj.Object, "spec", "group")
	if err != nil || !found {
		return false, fmt.Errorf("failed to extract API group from CRD: %w", err)
	}

	if !m.config.shouldWatchAPIGroup(apiGroup) {
		return false, nil
	}

	var crd apiextensionsv1.CustomResourceDefinition
	if err := convertUnstructuredToCRD(crdObj, &crd); err != nil {
		return false, fmt.Errorf("failed to convert to CRD: %w", err)
	}

	storageVersion := getStorageVersion(&crd)

	gvr := schema.GroupVersionResource{
		Group:    crd.Spec.Group,
		Version:  storageVersion,
		Resource: crd.Spec.Names.Plural,
	}

	m.settings.Logger.Info("Stopping CR watcher for deleted CRD",
		zap.String("name", crd.Name),
		zap.String("group", crd.Spec.Group),
		zap.String("resource", crd.Spec.Names.Plural),
	)

	m.forbiddenTracker.clear(gvr)

	if err := m.stopCRWatcher(gvr); err != nil {
		return true, err
	}
	return true, nil
}

// handleCRDModified detects storage version changes on CRD modifications.
// If the storage version changed, the existing CR watcher is restarted with the new GVR.
func (m *watchMode) handleCRDModified(crdObj *unstructured.Unstructured) (bool, error) {
	apiGroup, found, err := unstructured.NestedString(crdObj.Object, "spec", "group")
	if err != nil || !found {
		return false, fmt.Errorf("failed to extract API group from CRD: %w", err)
	}

	if !m.config.shouldWatchAPIGroup(apiGroup) {
		return false, nil
	}

	var crd apiextensionsv1.CustomResourceDefinition
	if err := convertUnstructuredToCRD(crdObj, &crd); err != nil {
		return false, fmt.Errorf("failed to convert to CRD: %w", err)
	}

	newStorageVersion := getStorageVersion(&crd)
	if newStorageVersion == "" {
		return false, fmt.Errorf("no storage version found for CRD %s", crd.Name)
	}

	newGVR := schema.GroupVersionResource{
		Group:    crd.Spec.Group,
		Version:  newStorageVersion,
		Resource: crd.Spec.Names.Plural,
	}
	newKey := formatGVRKey(newGVR)

	// Find existing watcher, stop it if version changed — single lock to avoid races.
	var oldGVR schema.GroupVersionResource
	var versionChanged, versionUnchanged bool
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for key, crw := range m.crWatchers {
			if crw.gvr.Group == crd.Spec.Group && crw.gvr.Resource == crd.Spec.Names.Plural {
				if key == newKey {
					// Version unchanged
					versionUnchanged = true
					return
				}
				// Version changed — stop old watcher while we hold the lock
				oldGVR = crw.gvr
				crw.Stop()
				delete(m.crWatchers, key)
				versionChanged = true
				return
			}
		}
	}()

	if versionUnchanged {
		return true, nil
	}

	if !versionChanged {
		// No existing watcher for this CRD — nothing to restart
		return true, nil
	}

	m.settings.Logger.Info("CRD storage version changed, restarting CR watcher",
		zap.String("name", crd.Name),
		zap.String("old_gvr", formatGVRKey(oldGVR)),
		zap.String("new_gvr", newKey),
	)

	// Clear the old GVR from forbidden tracker — the old version is no longer relevant
	m.forbiddenTracker.clear(oldGVR)

	// Start new watcher with updated version
	if err := m.startCRWatcher(newGVR); err != nil {
		return true, fmt.Errorf("failed to restart CR watcher after version change: %w", err)
	}

	return true, nil
}

// startCRWatcher creates a watcher for custom resources of the given GVR,
// lists initial state if configured, and runs the CR watch loop in a background goroutine.
func (m *watchMode) startCRWatcher(gvr schema.GroupVersionResource) error {
	key := formatGVRKey(gvr)

	m.mu.Lock()
	if _, exists := m.crWatchers[key]; exists {
		m.mu.Unlock()
		m.settings.Logger.Debug("Already watching CR", zap.String("gvr", key))
		return nil
	}
	m.mu.Unlock()

	watcher, err := m.client.Resource(gvr).Watch(m.ctx, metav1.ListOptions{})
	if err != nil {
		if isPermissionDenied(err) {
			m.forbiddenTracker.markForbidden(gvr)
			m.settings.Logger.Info("Skipping CR watcher - insufficient RBAC permissions",
				zap.String("gvr", key),
				zap.String("group", gvr.Group),
				zap.String("version", gvr.Version),
				zap.String("resource", gvr.Resource),
			)
			return nil
		}
		return fmt.Errorf("failed to watch CR %s: %w", key, err)
	}

	// Check under lock before inserting
	m.mu.Lock()
	if _, exists := m.crWatchers[key]; exists {
		m.mu.Unlock()
		watcher.Stop()
		m.settings.Logger.Debug("Already watching CR", zap.String("gvr", key))
		return nil
	}

	stopCh := make(chan struct{})
	crw := &crWatcher{
		gvr:     gvr,
		watcher: watcher,
		stopCh:  stopCh,
	}
	m.crWatchers[key] = crw
	m.mu.Unlock()

	// List initial state if configured
	if m.config.Watch.IncludeInitialState {
		if err := m.listInitialCRs(gvr); err != nil {
			m.settings.Logger.Error("Failed to list initial CRs",
				zap.String("resource", gvr.Resource),
				zap.Error(err),
			)
		}
	}

	// Run CR watch loop in background (no retry — CRD watcher manages CR watcher lifecycle)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.runCRWatchLoop(gvr, watcher, stopCh)
	}()

	return nil
}

// listInitialCRs lists existing custom resources for a GVR and emits logs for each.
func (m *watchMode) listInitialCRs(gvr schema.GroupVersionResource) error {
	list, err := m.client.Resource(gvr).List(m.ctx, metav1.ListOptions{})
	if err != nil {
		if isPermissionDenied(err) {
			m.forbiddenTracker.markForbidden(gvr)
			m.settings.Logger.Info("Skipping initial CR list - insufficient RBAC permissions",
				zap.String("resource", gvr.Resource),
				zap.String("group", gvr.Group),
			)
			return nil
		}
		return err
	}

	for i := range list.Items {
		cr := &list.Items[i]
		if err := emitLog(m.ctx, m.consumer, cr, watch.Added, buildCRLogRecord); err != nil {
			m.settings.Logger.Error("Failed to emit initial CR log", zap.Error(err))
		}
	}

	return nil
}

// runCRWatchLoop reads CR events and emits logs. No retry — if the channel closes,
// the loop exits and the CRD watcher will manage recreation if needed.
func (m *watchMode) runCRWatchLoop(gvr schema.GroupVersionResource, watcher watch.Interface, stopCh chan struct{}) {
	key := formatGVRKey(gvr)

	for {
		select {
		case <-stopCh:
			return

		case event, ok := <-watcher.ResultChan():
			if !ok {
				m.settings.Logger.Warn("CR watcher channel closed", zap.String("gvr", key))
				return
			}

			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				m.settings.Logger.Error("Unexpected object type in CR watch event",
					zap.String("gvr", key),
				)
				continue
			}

			if err := emitLog(m.ctx, m.consumer, obj, event.Type, buildCRLogRecord); err != nil {
				m.settings.Logger.Error("Failed to emit CR log",
					zap.String("gvr", key),
					zap.Error(err),
				)
			}
		}
	}
}

func (m *watchMode) stopCRWatcher(gvr schema.GroupVersionResource) error {
	key := formatGVRKey(gvr)

	m.mu.Lock()
	defer m.mu.Unlock()

	crw, exists := m.crWatchers[key]
	if !exists {
		return nil
	}

	crw.Stop()
	delete(m.crWatchers, key)

	return nil
}
