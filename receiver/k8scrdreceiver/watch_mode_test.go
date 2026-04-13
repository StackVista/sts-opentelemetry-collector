//nolint:testpackage // Tests require access to internal functions
package k8scrdreceiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

func newTestWatchMode(client k8sClient, sink *consumertest.LogsSink, config *Config) *watchMode {
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: zap.NewNop(),
		},
	}
	return newWatchMode(settings, config, sink, client, newForbiddenTracker(1*time.Hour))
}

func TestWatchMode_HandleCRDAdded(t *testing.T) {
	crGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}
	fw := newFakeWatcher()

	client := newFakeClient().
		withWatcher(crGVR, fw).
		withList(crGVR, &unstructured.UnstructuredList{})

	config := &Config{
		Watch:         WatchConfig{Enabled: true, IncludeInitialState: false},
		DiscoveryMode: DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: []string{"example.com"},
			Exclude: []string{},
		},
	}
	require.NoError(t, config.Validate())

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(client, sink, config)
	wm.ctx, wm.cancel = context.WithCancel(context.Background())
	wm.stopCh = make(chan struct{})
	t.Cleanup(func() {
		close(fw.ch)
		close(wm.stopCh)
		wm.cancel()
		wm.wg.Wait()
	})

	crd := makeTestCRD("test.example.com", "example.com", "TestResource", "testresources")

	shouldEmit, err := wm.handleCRDAdded(crd)
	require.NoError(t, err)
	assert.True(t, shouldEmit, "should emit log for added CRD")

	// Verify a CR watcher was started
	wm.mu.Lock()
	_, exists := wm.crWatchers[formatGVRKey(crGVR)]
	wm.mu.Unlock()
	assert.True(t, exists, "expected CR watcher to be started")
}

func TestWatchMode_HandleCRDAdded_SkipsFilteredGroup(t *testing.T) {
	config := &Config{
		Watch:         WatchConfig{Enabled: true},
		DiscoveryMode: DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: []string{"included.com"},
			Exclude: []string{},
		},
	}
	require.NoError(t, config.Validate())

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(newFakeClient(), sink, config)
	wm.ctx = context.Background()

	crd := makeTestCRD("excluded.crd", "excluded.com", "Excluded", "excludes")

	shouldEmit, err := wm.handleCRDAdded(crd)
	require.NoError(t, err)
	assert.False(t, shouldEmit, "should not emit log for filtered group")

	// No CR watcher should be started
	wm.mu.Lock()
	assert.Empty(t, wm.crWatchers)
	wm.mu.Unlock()
}

// newWatchModeWithExistingCRWatcher creates a watchMode with example.com API group filter
// and a pre-populated CR watcher for the given GVR.
func newWatchModeWithExistingCRWatcher(
	t *testing.T, gvr schema.GroupVersionResource,
) *watchMode {
	t.Helper()

	config := &Config{
		Watch:         WatchConfig{Enabled: true},
		DiscoveryMode: DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: []string{"example.com"},
			Exclude: []string{},
		},
	}
	require.NoError(t, config.Validate())

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(newFakeClient(), sink, config)
	wm.ctx = context.Background()
	wm.stopCh = make(chan struct{})

	wm.crWatchers[formatGVRKey(gvr)] = &crWatcher{
		gvr:     gvr,
		watcher: newFakeWatcher(),
		stopCh:  make(chan struct{}),
	}

	return wm
}

func TestWatchMode_HandleCRDDeleted(t *testing.T) {
	crGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}
	wm := newWatchModeWithExistingCRWatcher(t, crGVR)

	crd := makeTestCRD("test.example.com", "example.com", "TestResource", "testresources")

	shouldEmit, err := wm.handleCRDDeleted(crd)
	require.NoError(t, err)
	assert.True(t, shouldEmit, "should emit log for deleted CRD")

	// CR watcher should be removed
	wm.mu.Lock()
	_, exists := wm.crWatchers[formatGVRKey(crGVR)]
	wm.mu.Unlock()
	assert.False(t, exists, "expected CR watcher to be removed")
}

func TestWatchMode_HandleCRDModified_VersionChange(t *testing.T) {
	oldGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}
	newGVR := schema.GroupVersionResource{Group: "example.com", Version: "v2", Resource: "testresources"}

	newCRWatcher := newFakeWatcher()
	client := newFakeClient().
		withWatcher(newGVR, newCRWatcher).
		withList(newGVR, &unstructured.UnstructuredList{})

	config := &Config{
		Watch:         WatchConfig{Enabled: true, IncludeInitialState: false},
		DiscoveryMode: DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: []string{"example.com"},
			Exclude: []string{},
		},
	}
	require.NoError(t, config.Validate())

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(client, sink, config)
	wm.ctx, wm.cancel = context.WithCancel(context.Background())
	wm.stopCh = make(chan struct{})
	t.Cleanup(func() {
		close(newCRWatcher.ch)
		close(wm.stopCh)
		wm.cancel()
		wm.wg.Wait()
	})

	// Pre-populate old CR watcher
	oldWatcher := newFakeWatcher()
	wm.crWatchers[formatGVRKey(oldGVR)] = &crWatcher{
		gvr:     oldGVR,
		watcher: oldWatcher,
		stopCh:  make(chan struct{}),
	}

	// CRD modified with new storage version v2
	crd := makeTestCRDWithVersions("test.example.com", "example.com", "TestResource", "testresources", []map[string]interface{}{
		{"name": "v1", "storage": false},
		{"name": "v2", "storage": true},
	})

	shouldEmit, err := wm.handleCRDModified(crd)
	require.NoError(t, err)
	assert.True(t, shouldEmit, "should emit log for modified CRD")

	// Old watcher should be removed, new one started
	wm.mu.Lock()
	_, oldExists := wm.crWatchers[formatGVRKey(oldGVR)]
	_, newExists := wm.crWatchers[formatGVRKey(newGVR)]
	wm.mu.Unlock()
	assert.False(t, oldExists, "old CR watcher should be removed")
	assert.True(t, newExists, "new CR watcher should be started")
}

func TestWatchMode_HandleCRDModified_NoVersionChange(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}
	wm := newWatchModeWithExistingCRWatcher(t, gvr)

	// CRD modified but storage version unchanged
	crd := makeTestCRD("test.example.com", "example.com", "TestResource", "testresources")

	shouldEmit, err := wm.handleCRDModified(crd)
	require.NoError(t, err)
	assert.True(t, shouldEmit, "should emit log for modified CRD even without version change")

	// Watcher should still exist (not restarted)
	wm.mu.Lock()
	_, exists := wm.crWatchers[formatGVRKey(gvr)]
	wm.mu.Unlock()
	assert.True(t, exists, "watcher should still exist")
}

func TestWatchMode_HandleCRDModified_SkipsFilteredGroup(t *testing.T) {
	config := &Config{
		Watch:         WatchConfig{Enabled: true},
		DiscoveryMode: DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: []string{"included.com"},
			Exclude: []string{},
		},
	}
	require.NoError(t, config.Validate())

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(newFakeClient(), sink, config)
	wm.ctx = context.Background()

	crd := makeTestCRD("excluded.crd", "excluded.com", "Excluded", "excludes")

	shouldEmit, err := wm.handleCRDModified(crd)
	require.NoError(t, err)
	assert.False(t, shouldEmit, "should not emit log for filtered group")
}

func TestWatchMode_StartCRWatcher_PermissionDenied(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "forbidden.com", Version: "v1", Resource: "forbiddens"}

	client := newFakeClient().
		withWatchError(gvr, apierrors.NewForbidden(
			schema.GroupResource{Group: "forbidden.com", Resource: "forbiddens"},
			"",
			errors.New("permission denied"),
		))

	config := &Config{
		Watch: WatchConfig{Enabled: true},
	}

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(client, sink, config)
	wm.ctx = context.Background()

	err := wm.startCRWatcher(gvr)
	require.NoError(t, err, "should not return error for permission denied")

	// Should not have a watcher
	wm.mu.Lock()
	_, exists := wm.crWatchers[formatGVRKey(gvr)]
	wm.mu.Unlock()
	assert.False(t, exists)

	// Should be tracked as forbidden
	shouldRetry, _ := wm.forbiddenTracker.shouldRetry(gvr)
	assert.False(t, shouldRetry)
}

func TestWatchMode_StartCRWatcher_AlreadyWatching(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "tests"}

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(newFakeClient(), sink, &Config{
		Watch: WatchConfig{Enabled: true},
	})
	wm.ctx = context.Background()
	wm.stopCh = make(chan struct{})

	// Pre-populate
	wm.crWatchers[formatGVRKey(gvr)] = &crWatcher{
		gvr:     gvr,
		watcher: newFakeWatcher(),
		stopCh:  make(chan struct{}),
	}

	err := wm.startCRWatcher(gvr)
	require.NoError(t, err, "should not error for duplicate watcher")

	wm.mu.Lock()
	assert.Len(t, wm.crWatchers, 1, "should still have exactly one watcher")
	wm.mu.Unlock()
}

func TestWatchMode_ListInitialCRDs_RespectsFilters(t *testing.T) {
	// CRDs from two groups — only "allowed.com" should get a CR watcher started
	crd1 := makeTestCRD("allowed-crd", "allowed.com", "Allowed", "alloweds")
	crd2 := makeTestCRD("blocked-crd", "blocked.com", "Blocked", "blockeds")

	allowedCRGVR := schema.GroupVersionResource{Group: "allowed.com", Version: "v1", Resource: "alloweds"}
	allowedCRWatcher := newFakeWatcher()

	client := newFakeClient().
		withList(crdGVR, &unstructured.UnstructuredList{
			Items: []unstructured.Unstructured{*crd1, *crd2},
		}).
		withWatcher(allowedCRGVR, allowedCRWatcher).
		withList(allowedCRGVR, &unstructured.UnstructuredList{})

	config := &Config{
		Watch:         WatchConfig{Enabled: true, IncludeInitialState: true},
		DiscoveryMode: DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: []string{"allowed.com"},
			Exclude: []string{},
		},
	}
	require.NoError(t, config.Validate())

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(client, sink, config)
	wm.ctx, wm.cancel = context.WithCancel(context.Background())
	wm.stopCh = make(chan struct{})
	t.Cleanup(func() {
		close(allowedCRWatcher.ch)
		close(wm.stopCh)
		wm.cancel()
		wm.wg.Wait()
	})

	err := wm.listInitialCRDs(crdGVR)
	require.NoError(t, err)

	// Only the allowed CRD should emit a log (API group filter applies to CRDs too)
	assert.Equal(t, 1, sink.LogRecordCount(), "expected 1 CRD log emitted")

	// Only allowed group should have a CR watcher
	wm.mu.Lock()
	_, allowedExists := wm.crWatchers[formatGVRKey(allowedCRGVR)]
	watcherCount := len(wm.crWatchers)
	wm.mu.Unlock()
	assert.True(t, allowedExists, "expected CR watcher for allowed group")
	assert.Equal(t, 1, watcherCount, "expected exactly 1 CR watcher")
}

func TestWatchMode_ListInitialCRs(t *testing.T) {
	crGVR := schema.GroupVersionResource{Group: "example.com", Version: "v2", Resource: "tests"}
	cr1 := makeTestCR("cr-1", "example.com", "Test", "v2")
	cr2 := makeTestCR("cr-2", "example.com", "Test", "v2")

	client := newFakeClient().
		withList(crGVR, &unstructured.UnstructuredList{
			Items: []unstructured.Unstructured{*cr1, *cr2},
		})

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(client, sink, &Config{
		Watch: WatchConfig{Enabled: true, IncludeInitialState: true},
	})
	wm.ctx = context.Background()

	err := wm.listInitialCRs(crGVR)
	require.NoError(t, err)

	assert.Equal(t, 2, sink.LogRecordCount(), "expected 2 CR logs emitted")
}

func TestWatchMode_ListInitialCRs_PermissionDenied(t *testing.T) {
	crGVR := schema.GroupVersionResource{Group: "forbidden.com", Version: "v1", Resource: "forbiddens"}

	client := newFakeClient().
		withListError(crGVR, apierrors.NewForbidden(
			schema.GroupResource{Group: "forbidden.com", Resource: "forbiddens"},
			"",
			errors.New("permission denied"),
		))

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(client, sink, &Config{
		Watch: WatchConfig{Enabled: true},
	})
	wm.ctx = context.Background()

	err := wm.listInitialCRs(crGVR)
	require.NoError(t, err, "should handle permission denied gracefully")

	assert.Equal(t, 0, sink.LogRecordCount())

	shouldRetry, _ := wm.forbiddenTracker.shouldRetry(crGVR)
	assert.False(t, shouldRetry, "should be marked as forbidden")
}

func TestWatchMode_CRWatcherRetriesOnChannelClose(t *testing.T) {
	crGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}

	// First watcher will have its channel closed to simulate failure.
	// Second watcher is the reconnected one that receives an event.
	firstWatcher := newFakeWatcher()
	secondWatcher := newFakeWatcher()

	client := newFakeClient().
		withWatcherQueue(crGVR, firstWatcher, secondWatcher).
		withList(crGVR, &unstructured.UnstructuredList{})

	config := &Config{
		Watch:         WatchConfig{Enabled: true, IncludeInitialState: false},
		DiscoveryMode: DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: []string{"example.com"},
			Exclude: []string{},
		},
	}
	require.NoError(t, config.Validate())

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(client, sink, config)
	wm.ctx, wm.cancel = context.WithCancel(context.Background())
	wm.stopCh = make(chan struct{})

	// Start a CR watcher — this will use firstWatcher
	err := wm.startCRWatcher(crGVR)
	require.NoError(t, err)

	// Close first watcher's channel to simulate API server disconnect
	close(firstWatcher.ch)

	// Wait for retry to kick in and the second watcher to be installed.
	// The retry uses initialBackoff (1s), so we wait a bit longer.
	assert.Eventually(t, func() bool {
		wm.mu.Lock()
		defer wm.mu.Unlock()
		crw, exists := wm.crWatchers[formatGVRKey(crGVR)]
		if !exists {
			return false
		}
		// The watcher reference should be updated to secondWatcher
		return crw.watcher == secondWatcher
	}, 5*time.Second, 100*time.Millisecond, "CR watcher should be recreated after channel close")

	// Send an event on the second watcher to verify it's actually processing
	cr := makeTestCR("test-cr", "example.com", "TestResource", "v1")
	secondWatcher.ch <- watch.Event{
		Type:   watch.Added,
		Object: cr,
	}

	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() > 0
	}, 2*time.Second, 100*time.Millisecond, "should receive events on recreated watcher")

	// Clean shutdown — stop the CR watcher via its stopCh, then cancel context
	wm.mu.Lock()
	if crw, exists := wm.crWatchers[formatGVRKey(crGVR)]; exists {
		crw.Stop()
	}
	wm.mu.Unlock()
	wm.cancel()
	wm.wg.Wait()
}

func TestWatchMode_CRWatcherStopsDuringRetry(t *testing.T) {
	crGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}

	firstWatcher := newFakeWatcher()

	client := newFakeClient().
		withWatcher(crGVR, firstWatcher).
		withList(crGVR, &unstructured.UnstructuredList{})

	config := &Config{
		Watch:         WatchConfig{Enabled: true, IncludeInitialState: false},
		DiscoveryMode: DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: []string{"example.com"},
			Exclude: []string{},
		},
	}
	require.NoError(t, config.Validate())

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(client, sink, config)
	wm.ctx, wm.cancel = context.WithCancel(context.Background())
	wm.stopCh = make(chan struct{})

	err := wm.startCRWatcher(crGVR)
	require.NoError(t, err)

	// Close first watcher's channel to trigger retry
	close(firstWatcher.ch)

	// Simulate CRD deletion removing the entry during the backoff wait.
	// Give the goroutine a moment to enter the retry backoff.
	time.Sleep(100 * time.Millisecond)
	wm.mu.Lock()
	crw, exists := wm.crWatchers[formatGVRKey(crGVR)]
	if exists {
		crw.Stop()
		delete(wm.crWatchers, formatGVRKey(crGVR))
	}
	wm.mu.Unlock()

	// The retry goroutine should detect the removed entry and exit cleanly.
	// Cancel context to unblock any remaining backoff wait.
	wm.cancel()
	close(wm.stopCh)

	// wg.Wait should return promptly — the goroutine should not hang.
	done := make(chan struct{})
	go func() {
		wm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("goroutine did not exit after CRD deletion during retry")
	}
}

func TestWatchMode_Shutdown(t *testing.T) {
	crdFakeWatcher := newFakeWatcher()
	client := newFakeClient().
		withWatcher(crdGVR, crdFakeWatcher).
		withList(crdGVR, &unstructured.UnstructuredList{})

	config := &Config{
		Watch:         WatchConfig{Enabled: true, IncludeInitialState: false},
		DiscoveryMode: DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: []string{"*"},
			Exclude: []string{},
		},
	}
	require.NoError(t, config.Validate())

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(client, sink, config)

	err := wm.Start(context.Background())
	require.NoError(t, err)

	// Shutdown stops the CRD watcher which closes the channel — the goroutine will exit
	err = wm.Shutdown(context.Background())
	require.NoError(t, err)
}
