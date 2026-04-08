package k8scrdreceiver

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	// initialBackoff is the initial retry delay for watcher failures
	initialBackoff = time.Second

	// maxBackoff is the maximum retry delay for watcher failures
	maxBackoff = 5 * time.Minute
)

// resourceWatcher encapsulates the common pattern of watching Kubernetes resources
type resourceWatcher struct {
	gvr    schema.GroupVersionResource
	client k8sClient
	// ctx is the lifecycle context for this watcher
	//nolint:containedctx
	ctx    context.Context
	logger *zap.Logger

	// Configuration
	config watcherConfig

	// Callbacks for resource-specific logic
	handlers watcherHandlers

	// State
	watcher watch.Interface
	stopCh  chan struct{}
}

// watcherConfig holds configuration for a resource watcher
type watcherConfig struct {
	// Whether to list initial state before watching
	listInitialState bool

	// Whether to retry on failures with exponential backoff
	enableRetry bool

	// Whether to handle permission denied gracefully
	gracefulPermissionDenied bool

	// Whether to recreate watcher on channel closure
	recreateOnClose bool

	// Resource name for logging (e.g., "CRD", "CR")
	resourceType string
}

// watcherHandlers contains callbacks for resource-specific operations
type watcherHandlers struct {
	// buildLog creates a log record from a resource and event type
	buildLog func(*unstructured.Unstructured, watch.EventType, time.Time) (plog.Logs, error)

	// consumeLog sends the log to the consumer
	consumeLog func(context.Context, plog.Logs) error

	// handleEvent processes an event (optional - if nil, just emits log)
	// Return true to also emit log, false to skip log emission
	handleEvent func(*unstructured.Unstructured, watch.EventType) (bool, error)

	// recreateWatcher recreates the watcher after channel closure (optional)
	recreateWatcher func() (watch.Interface, error)

	// onPermissionDenied handles permission denied errors (optional)
	onPermissionDenied func(error)
}

// listInitialState lists and processes existing resources
func (w *resourceWatcher) listInitialState() error {
	if !w.config.listInitialState {
		return nil
	}

	list, err := w.client.Resource(w.gvr).List(w.ctx, metav1.ListOptions{})
	if err != nil {
		// Handle permission denied gracefully if configured
		if w.config.gracefulPermissionDenied && isPermissionDenied(err) {
			if w.handlers.onPermissionDenied != nil {
				w.handlers.onPermissionDenied(err)
			}
			w.logger.Info(
				fmt.Sprintf("Skipping initial %s list - insufficient RBAC permissions", w.config.resourceType),
				zap.String("resource", w.gvr.Resource),
				zap.String("group", w.gvr.Group),
			)
			return nil
		}
		return err
	}

	for i := range list.Items {
		obj := &list.Items[i]

		// For initial state, always use Added event type
		shouldEmitLog := true
		if w.handlers.handleEvent != nil {
			emit, err := w.handlers.handleEvent(obj, watch.Added)
			if err != nil {
				w.logger.Error(
					fmt.Sprintf("Failed to handle initial %s", w.config.resourceType),
					zap.String("name", obj.GetName()),
					zap.Error(err),
				)
				continue
			}
			shouldEmitLog = emit
		}

		// Emit log if requested (respects filtering from handleEvent)
		if shouldEmitLog {
			if err := w.emitLog(obj, watch.Added); err != nil {
				w.logger.Error(
					fmt.Sprintf("Failed to emit initial %s log", w.config.resourceType),
					zap.Error(err),
				)
			}
		}
	}

	return nil
}

// watch starts the main watch loop
func (w *resourceWatcher) watch() {
	if w.config.enableRetry {
		w.watchWithRetry()
	} else {
		_ = w.watchOnce() // Error logged internally, graceful shutdown expected
	}
}

// watchWithRetry implements exponential backoff retry logic
func (w *resourceWatcher) watchWithRetry() {
	backoff := initialBackoff

	for {
		if err := w.watchOnce(); err != nil {
			w.logger.Warn(
				fmt.Sprintf("%s watcher stopped, will retry", w.config.resourceType),
				zap.Error(err),
				zap.Duration("backoff", backoff),
			)

			select {
			case <-w.stopCh:
				return
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
				continue
			}
		}
		return // Clean exit
	}
}

// watchOnce runs a single iteration of the watch loop
func (w *resourceWatcher) watchOnce() error {
	for {
		select {
		case <-w.stopCh:
			return nil

		case event, ok := <-w.watcher.ResultChan():
			if !ok {
				return w.handleChannelClosed()
			}

			if err := w.handleWatchEvent(event); err != nil {
				w.logger.Error(
					fmt.Sprintf("Failed to handle %s event", w.config.resourceType),
					zap.Error(err),
				)
			}
		}
	}
}

// handleChannelClosed handles watcher channel closure
func (w *resourceWatcher) handleChannelClosed() error {
	if !w.config.recreateOnClose {
		w.logger.Warn(
			fmt.Sprintf("%s watcher channel closed", w.config.resourceType),
			zap.String("gvr", formatGVRKey(w.gvr)),
		)
		return nil // Exit loop
	}

	// Recreate watcher
	if w.handlers.recreateWatcher == nil {
		return fmt.Errorf("recreateWatcher handler not provided")
	}

	newWatcher, err := w.handlers.recreateWatcher()
	if err != nil {
		return fmt.Errorf("failed to recreate watcher: %w", err)
	}

	w.watcher = newWatcher
	w.logger.Info(fmt.Sprintf("%s watcher recreated successfully", w.config.resourceType))
	return nil
}

// handleWatchEvent processes a single watch event
func (w *resourceWatcher) handleWatchEvent(event watch.Event) error {
	obj, ok := event.Object.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("unexpected object type in watch event")
	}

	// Call custom handler if provided
	shouldEmitLog := true
	if w.handlers.handleEvent != nil {
		emit, err := w.handlers.handleEvent(obj, event.Type)
		if err != nil {
			return err
		}
		shouldEmitLog = emit
	}

	// Emit log if requested
	if shouldEmitLog {
		return w.emitLog(obj, event.Type)
	}

	return nil
}

// emitLog builds and consumes a log record
func (w *resourceWatcher) emitLog(obj *unstructured.Unstructured, eventType watch.EventType) error {
	logs, err := w.handlers.buildLog(obj, eventType, time.Now())
	if err != nil {
		return err
	}

	return w.handlers.consumeLog(w.ctx, logs)
}
