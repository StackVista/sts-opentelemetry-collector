package k8sresourcereceiver

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/collectiondeclaration"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/tracker"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
)

type collectionWatchSnapshot struct {
	Spec                   collectionWatchSpec
	State, Reason, Message string
	Synced                 bool
	Objects                []resourceObject
}

type collectionRegistryEntry struct {
	spec     collectionWatchSpec
	informer cache.SharedIndexInformer
	cancel   context.CancelFunc
	mu       sync.Mutex
	lastErr  error
	retryAt  time.Time
}

type collectionObservedWatch struct {
	incoming watch.Interface
	result   chan watch.Event
	cancel   context.CancelFunc
	once     sync.Once
}

func (w *collectionObservedWatch) Stop() {
	w.once.Do(func() {
		w.cancel()
		w.incoming.Stop()
	})
}

func (w *collectionObservedWatch) ResultChan() <-chan watch.Event { return w.result }

type collectionWatchRegistry struct {
	client        dynamic.Interface
	buildInformer func(*cache.ListWatch) cache.SharedIndexInformer
	ctx           context.Context //nolint:containedctx
	cancel        context.CancelFunc
	mu            sync.RWMutex
	entries       map[collectionWatchKey]*collectionRegistryEntry
	wg            sync.WaitGroup
	stopped       chan struct{}
	closed        bool
	notify        func()
	onChange      func(old, current interface{})
	clock         clock.Clock
}

func newCollectionWatchRegistry(
	ctx context.Context, client dynamic.Interface,
	buildInformer func(*cache.ListWatch) cache.SharedIndexInformer,
) *collectionWatchRegistry {
	ctx, cancel := context.WithCancel(ctx)
	return &collectionWatchRegistry{
		client: client, buildInformer: buildInformer, ctx: ctx, cancel: cancel,
		entries: map[collectionWatchKey]*collectionRegistryEntry{},
		stopped: make(chan struct{}), clock: clock.RealClock{},
	}
}

func (r *collectionWatchRegistry) reconcile(specs []collectionWatchSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return context.Canceled
	}
	if err := r.ctx.Err(); err != nil {
		return err
	}
	desired := make(map[collectionWatchKey]bool, len(specs))
	for _, spec := range specs {
		desired[spec.Key] = true
	}
	// Cancel retired watches before starting replacements at the watch limit.
	for key, entry := range r.entries {
		if !desired[key] {
			entry.cancel()
			delete(r.entries, key)
		}
	}
	for _, spec := range specs {
		if entry, exists := r.entries[spec.Key]; exists {
			entry.spec = copyCollectionWatchSpec(spec)
			continue
		}
		entry, err := r.start(spec)
		if err != nil {
			return err
		}
		r.entries[spec.Key] = entry
	}
	return nil
}

func (r *collectionWatchRegistry) start(spec collectionWatchSpec) (*collectionRegistryEntry, error) {
	ctx, cancel := context.WithCancel(r.ctx)
	entry := &collectionRegistryEntry{spec: copyCollectionWatchSpec(spec), cancel: cancel}
	watchClock := r.clock
	resource := resourceClientFor(r.client, spec.Key.GVR, spec.Key.Namespace)
	record := func(err error) {
		entry.mu.Lock()
		if ctx.Err() == nil {
			entry.lastErr = err
			if isPermissionDenied(err) {
				entry.retryAt = watchClock.Now().Add(tracker.DefaultRetryInterval)
			} else if err == nil {
				entry.retryAt = time.Time{}
			}
		}
		entry.mu.Unlock()
		if r.notify != nil && ctx.Err() == nil {
			r.notify()
		}
	}
	selectors := func(options *metav1.ListOptions) {
		options.LabelSelector = spec.Key.LabelSelector
		options.FieldSelector = spec.Key.FieldSelector
	}
	entry.informer = r.buildInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			if err := entry.waitForRetry(ctx, watchClock); err != nil {
				return nil, err
			}
			selectors(&options)
			result, err := resource.List(ctx, options)
			record(err)
			return result, err
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			if err := entry.waitForRetry(ctx, watchClock); err != nil {
				return nil, err
			}
			selectors(&options)
			result, err := resource.Watch(ctx, options)
			record(err)
			if err == nil {
				result = r.observeWatch(ctx, result, record)
			}
			return result, err
		},
	})
	if r.onChange != nil {
		_, err := entry.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { r.onChange(nil, obj) },
			UpdateFunc: r.onChange,
			DeleteFunc: func(obj interface{}) { r.onChange(obj, nil) },
		})
		if err != nil {
			cancel()
			return nil, err
		}
	}
	if err := entry.informer.SetWatchErrorHandlerWithContext(func(_ context.Context, _ *cache.Reflector, err error) {
		record(err)
	}); err != nil {
		cancel()
		return nil, fmt.Errorf("configure collection watch %s: %w", formatGVR(spec.Key.GVR), err)
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		entry.informer.RunWithContext(ctx)
	}()
	return entry, nil
}

func (e *collectionRegistryEntry) waitForRetry(ctx context.Context, watchClock clock.Clock) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		e.mu.Lock()
		delay := e.retryAt.Sub(watchClock.Now())
		e.mu.Unlock()
		if delay <= 0 {
			return nil
		}
		// Delay only this watch's requests; keep its cache and failure status readable.
		timer := watchClock.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C():
			timer.Stop()
		}
	}
}

func (r *collectionWatchRegistry) observeWatch(
	ctx context.Context, incoming watch.Interface, record func(error),
) watch.Interface {
	ctx, cancel := context.WithCancel(ctx)
	observed := &collectionObservedWatch{incoming: incoming, result: make(chan watch.Event), cancel: cancel}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer close(observed.result)
		defer observed.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case event, open := <-incoming.ResultChan():
				if !open {
					return
				}
				// Reflectors can consume watch ERROR events without calling their error handler.
				if event.Type == watch.Error {
					record(apierrors.FromObject(event.Object))
				}
				select {
				case observed.result <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return observed
}

func (r *collectionWatchRegistry) snapshot() []collectionWatchSnapshot {
	return r.readSnapshot(true)
}

func (r *collectionWatchRegistry) healthSnapshot() []collectionWatchSnapshot {
	return r.readSnapshot(false)
}

func (r *collectionWatchRegistry) cachedObject(key collectionWatchKey, name string) *unstructured.Unstructured {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry := r.entries[key]
	if entry == nil {
		return nil
	}
	if key.Namespace != "" {
		name = key.Namespace + "/" + name
	}
	object, exists, err := entry.informer.GetStore().GetByKey(name)
	if err != nil || !exists {
		return nil
	}
	current, _ := toUnstructured(object)
	return current
}

func (r *collectionWatchRegistry) readSnapshot(objects bool) []collectionWatchSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.ctx.Err() != nil {
		return nil
	}
	result := make([]collectionWatchSnapshot, 0, len(r.entries))
	for _, entry := range r.entries {
		entry.mu.Lock()
		observation := collectionWatchSnapshot{
			Spec: copyCollectionWatchSpec(entry.spec), Synced: entry.informer.HasSynced(),
			State: collectiondeclaration.Pending, Reason: collectionAwaitingObservation,
		}
		switch err := entry.lastErr; {
		case err != nil:
			observation.State, observation.Reason, observation.Message = collectionObservationError(err)
		case observation.Synced:
			observation.State, observation.Reason = collectiondeclaration.Active, collectionWatching
		}
		// Keep known objects beside failure state; consumers must not infer an empty observation.
		if objects && observation.Synced {
			for _, item := range entry.informer.GetStore().List() {
				if object, ok := toResourceObject(item); ok {
					observation.Objects = append(observation.Objects, object)
				}
			}
		}
		entry.mu.Unlock()
		result = append(result, observation)
	}
	sort.Slice(result, func(i, j int) bool {
		return collectionWatchLess(result[i].Spec.Key, result[j].Spec.Key)
	})
	return result
}

func (r *collectionWatchRegistry) invalidateRemovedCRDs(removed []schema.GroupResource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, entry := range r.entries {
		if entry.spec.Source != ObjectSourceCR {
			continue
		}
		for _, resource := range removed {
			if key.GVR.GroupResource() == resource {
				entry.cancel()
				delete(r.entries, key)
				break
			}
		}
	}
}

func (r *collectionWatchRegistry) shutdown(ctx context.Context) error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		r.cancel()
		r.entries = map[collectionWatchKey]*collectionRegistryEntry{}
		go func() {
			r.wg.Wait()
			close(r.stopped)
		}()
	}
	r.mu.Unlock()
	select {
	case <-r.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func collectionObservationError(err error) (string, string, string) {
	state, reason := collectiondeclaration.Pending, "ObservationFailed"
	switch {
	case isPermissionDenied(err):
		state, reason = collectiondeclaration.Blocked, "AccessDenied"
	case apierrors.IsNotFound(err):
		reason = collectionAPINotAvailable
	case apierrors.IsBadRequest(err) || apierrors.IsInvalid(err):
		state, reason = collectiondeclaration.Blocked, "InvalidSelection"
	}
	return state, reason, err.Error()
}

func copyCollectionWatchSpec(spec collectionWatchSpec) collectionWatchSpec {
	spec.Contributors = append([]collectionContributor(nil), spec.Contributors...)
	return spec
}
