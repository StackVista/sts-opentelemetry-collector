package k8scrdreceiver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

type k8scrdReceiver struct {
	settings receiver.Settings
	config   *Config
	consumer consumer.Logs
	client   k8sClient

	mu         sync.Mutex
	crdWatcher watch.Interface
	crWatchers map[string]*crWatcher // key: group/version/resource
	stopCh     chan struct{}
	wg         sync.WaitGroup

	// Track resources with permission denied to avoid repeated attempts
	forbiddenTracker *forbiddenTracker

	// ctx is set once in Start() and never modified after, safe to read without locks.
	// Storing context in struct is intentional - it's the receiver's lifecycle context.
	//nolint:containedctx
	ctx    context.Context
	cancel context.CancelFunc
}

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

// Key returns the unique identifier for this watcher
func (w *crWatcher) Key() string {
	return formatGVRKey(w.gvr)
}

const (
	// defaultForbiddenRetryInterval is how long to wait before retrying a resource that returned permission denied
	defaultForbiddenRetryInterval = 1 * time.Hour
)

// isPermissionDenied checks if an error is a Kubernetes RBAC permission denied error
func isPermissionDenied(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err)
}

func newReceiver(params receiver.Settings, config *Config, consumer consumer.Logs) (receiver.Logs, error) {
	return &k8scrdReceiver{
		settings:         params,
		config:           config,
		consumer:         consumer,
		crWatchers:       make(map[string]*crWatcher),
		forbiddenTracker: newForbiddenTracker(defaultForbiddenRetryInterval),
	}, nil
}

func (r *k8scrdReceiver) Start(ctx context.Context, _ component.Host) error {
	// Create client if not already set (for testing, client can be injected)
	if r.client == nil {
		// Dynamic client is required because we discover CRDs at runtime and need to watch
		// arbitrary custom resources without knowing their Go types at compile time.
		// Unlike typed clients (e.g., clientset.CoreV1().Pods()), dynamic client works with
		// any GVR (GroupVersionResource) and returns unstructured.Unstructured objects.
		dynamicClient, err := r.config.getDynamicClient()
		if err != nil {
			return fmt.Errorf("failed to create dynamic client: %w", err)
		}
		r.client = newDynamicClientWrapper(dynamicClient)
	}

	r.ctx, r.cancel = context.WithCancel(ctx)
	r.stopCh = make(chan struct{})

	// Start watching CRDs
	if err := r.startCRDWatcher(); err != nil {
		return fmt.Errorf("failed to start CRD watcher: %w", err)
	}

	r.settings.Logger.Info("K8s CRD Receiver started",
		zap.String("discovery_mode", string(r.config.DiscoveryMode)),
		zap.Any("api_group_filters", r.config.APIGroupFilters),
	)

	return nil
}

func (r *k8scrdReceiver) Shutdown(ctx context.Context) error {
	r.settings.Logger.Info("Shutting down K8s CRD Receiver")

	// Signal all goroutines to stop
	if r.cancel != nil {
		r.cancel()
	}
	close(r.stopCh)

	// Stop all CR watchers
	r.mu.Lock()
	for _, crw := range r.crWatchers {
		crw.Stop()
	}
	r.crWatchers = make(map[string]*crWatcher)
	r.mu.Unlock()

	// Stop CRD watcher
	if r.crdWatcher != nil {
		r.crdWatcher.Stop()
	}

	// Wait for all goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		r.settings.Logger.Info("All watchers stopped gracefully")
		return nil
	case <-ctx.Done():
		r.settings.Logger.Warn("Shutdown timeout, some watchers may not have stopped cleanly")
		return ctx.Err()
	}
}

func (r *k8scrdReceiver) startCRDWatcher() error {
	// Watch CustomResourceDefinitions
	crdGVR := schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}

	watcher, err := r.client.Resource(crdGVR).Watch(r.ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to watch CRDs: %w", err)
	}

	r.crdWatcher = watcher

	// Create resource watcher with CRD-specific configuration
	rw := &resourceWatcher{
		gvr:    crdGVR,
		client: r.client,
		ctx:    r.ctx,
		logger: r.settings.Logger,
		config: watcherConfig{
			listInitialState:         r.config.IncludeInitialState,
			enableRetry:              true,
			gracefulPermissionDenied: false,
			recreateOnClose:          true,
			resourceType:             "CRD",
		},
		handlers: watcherHandlers{
			buildLog:   buildCRDLogRecord,
			consumeLog: r.consumer.ConsumeLogs,
			handleEvent: func(obj *unstructured.Unstructured, eventType watch.EventType) (bool, error) {
				switch eventType {
				case watch.Added:
					// Emit log and handle CRD addition
					return true, r.handleCRDAdded(obj)
				case watch.Deleted:
					// Emit log and handle CRD deletion
					return true, r.handleCRDDeleted(obj)
				case watch.Modified:
					// CRD modified - we might need to handle version changes, but for now, ignore
					// Don't emit log for modifications
					return false, nil
				case watch.Error:
					return false, fmt.Errorf("error event in CRD watcher")
				default:
					return false, nil
				}
			},
			recreateWatcher: func() (watch.Interface, error) {
				newWatcher, err := r.client.Resource(crdGVR).Watch(r.ctx, metav1.ListOptions{})
				if err != nil {
					return nil, err
				}
				r.crdWatcher = newWatcher
				return newWatcher, nil
			},
		},
		watcher: watcher,
		stopCh:  r.stopCh,
	}

	// List initial state if configured
	if err := rw.listInitialState(); err != nil {
		r.settings.Logger.Error("Failed to list initial CRDs", zap.Error(err))
	}

	// Start watching in background
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		rw.watch()
	}()

	return nil
}

func (r *k8scrdReceiver) handleCRDAdded(crdObj *unstructured.Unstructured) error {
	// Extract API group directly from unstructured object
	apiGroup, found, err := unstructured.NestedString(crdObj.Object, "spec", "group")
	if err != nil || !found {
		return fmt.Errorf("failed to extract API group from CRD: %w", err)
	}

	// Check if we should watch this API group (fast filter with cached regexes)
	if !r.config.shouldWatchAPIGroup(apiGroup) {
		r.settings.Logger.Debug("Skipping CRD (API group not in filters)",
			zap.String("name", crdObj.GetName()),
			zap.String("group", apiGroup),
		)
		return nil
	}

	// Parse full CRD only if we need to watch it
	var crd apiextensionsv1.CustomResourceDefinition
	if err := convertUnstructuredToCRD(crdObj, &crd); err != nil {
		return fmt.Errorf("failed to convert to CRD: %w", err)
	}

	// Find the storage version
	storageVersion := getStorageVersion(&crd)
	if storageVersion == "" {
		return fmt.Errorf("no storage version found for CRD %s", crd.Name)
	}

	gvr := schema.GroupVersionResource{
		Group:    crd.Spec.Group,
		Version:  storageVersion,
		Resource: crd.Spec.Names.Plural,
	}

	// Check if this resource was previously forbidden
	if shouldRetry, retryIn := r.forbiddenTracker.shouldRetry(gvr); !shouldRetry {
		r.settings.Logger.Debug("Skipping forbidden resource (will retry later)",
			zap.String("gvr", formatGVRKey(gvr)),
			zap.Duration("retry_in", retryIn),
		)
		return nil
	}

	// Emit CRD event log for topology
	if err := r.emitCRDLog(crdObj, watch.Added); err != nil {
		r.settings.Logger.Error("Failed to emit CRD log", zap.Error(err))
		// Don't fail - continue to start CR watcher
	}

	r.settings.Logger.Info("Starting CR watcher for new CRD",
		zap.String("name", crd.Name),
		zap.String("group", crd.Spec.Group),
		zap.String("version", storageVersion),
		zap.String("resource", crd.Spec.Names.Plural),
	)

	return r.startCRWatcher(gvr)
}

func (r *k8scrdReceiver) handleCRDDeleted(crdObj *unstructured.Unstructured) error {
	var crd apiextensionsv1.CustomResourceDefinition
	if err := convertUnstructuredToCRD(crdObj, &crd); err != nil {
		return fmt.Errorf("failed to convert to CRD: %w", err)
	}

	// Find the storage version
	storageVersion := getStorageVersion(&crd)

	gvr := schema.GroupVersionResource{
		Group:    crd.Spec.Group,
		Version:  storageVersion,
		Resource: crd.Spec.Names.Plural,
	}

	// Emit CRD deletion event log for topology
	if err := r.emitCRDLog(crdObj, watch.Deleted); err != nil {
		r.settings.Logger.Error("Failed to emit CRD log", zap.Error(err))
		// Don't fail - continue to stop CR watcher
	}

	r.settings.Logger.Info("Stopping CR watcher for deleted CRD",
		zap.String("name", crd.Name),
		zap.String("group", crd.Spec.Group),
		zap.String("resource", crd.Spec.Names.Plural),
	)

	// Clean up forbidden tracking if this CRD is deleted
	r.forbiddenTracker.clear(gvr)

	return r.stopCRWatcher(gvr)
}

func (r *k8scrdReceiver) startCRWatcher(gvr schema.GroupVersionResource) error {
	key := formatGVRKey(gvr)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if already watching
	if _, exists := r.crWatchers[key]; exists {
		r.settings.Logger.Debug("Already watching CR", zap.String("gvr", key))
		return nil
	}

	// Start watching CR instances
	watcher, err := r.client.Resource(gvr).Watch(r.ctx, metav1.ListOptions{})
	if err != nil {
		if isPermissionDenied(err) {
			// Track as forbidden and log once at INFO level
			r.forbiddenTracker.markForbidden(gvr)

			r.settings.Logger.Info("Skipping CR watcher - insufficient RBAC permissions",
				zap.String("gvr", key),
				zap.String("group", gvr.Group),
				zap.String("version", gvr.Version),
				zap.String("resource", gvr.Resource),
				zap.String("reason", "ClusterRole permissions do not allow watching this resource"),
				zap.String("hint", "Grant watch permission for this API group or use wildcard apiGroups: ['*']"),
			)
			return nil // Don't fail - gracefully skip this resource
		}
		return fmt.Errorf("failed to watch CR %s: %w", key, err)
	}

	stopCh := make(chan struct{})

	// Create resource watcher with CR-specific configuration
	rw := &resourceWatcher{
		gvr:    gvr,
		client: r.client,
		ctx:    r.ctx,
		logger: r.settings.Logger,
		config: watcherConfig{
			listInitialState:         r.config.IncludeInitialState,
			enableRetry:              false, // CR watchers don't retry - they're recreated by CRD watcher
			gracefulPermissionDenied: true,
			recreateOnClose:          false,
			resourceType:             "CR",
		},
		handlers: watcherHandlers{
			buildLog:   buildCRLogRecord,
			consumeLog: r.consumer.ConsumeLogs,
			// handleEvent is nil - just emit logs for all events
			onPermissionDenied: func(_ error) {
				r.forbiddenTracker.markForbidden(gvr)
			},
		},
		watcher: watcher,
		stopCh:  stopCh,
	}

	crw := &crWatcher{
		gvr:     gvr,
		watcher: watcher,
		stopCh:  stopCh,
	}

	r.crWatchers[key] = crw

	// List initial state if configured
	if err := rw.listInitialState(); err != nil {
		r.settings.Logger.Error("Failed to list initial CRs",
			zap.String("resource", gvr.Resource),
			zap.Error(err),
		)
	}

	// Start watching in background
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		rw.watch()
	}()

	return nil
}

func (r *k8scrdReceiver) stopCRWatcher(gvr schema.GroupVersionResource) error {
	key := formatGVRKey(gvr)

	r.mu.Lock()
	defer r.mu.Unlock()

	crw, exists := r.crWatchers[key]
	if !exists {
		return nil
	}

	crw.Stop()
	delete(r.crWatchers, key)

	return nil
}

func (r *k8scrdReceiver) emitCRDLog(crd *unstructured.Unstructured, eventType watch.EventType) error {
	logs, err := buildCRDLogRecord(crd, eventType, time.Now())
	if err != nil {
		return err
	}

	return r.consumer.ConsumeLogs(r.ctx, logs)
}
