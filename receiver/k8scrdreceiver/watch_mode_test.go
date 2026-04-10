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

	err := wm.handleCRDAdded(crd)
	require.NoError(t, err)

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

	err := wm.handleCRDAdded(crd)
	require.NoError(t, err)

	// No CR watcher should be started
	wm.mu.Lock()
	assert.Empty(t, wm.crWatchers)
	wm.mu.Unlock()
}

func TestWatchMode_HandleCRDDeleted(t *testing.T) {
	crGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}

	sink := &consumertest.LogsSink{}
	wm := newTestWatchMode(newFakeClient(), sink, &Config{
		Watch: WatchConfig{Enabled: true},
	})
	wm.ctx = context.Background()
	wm.stopCh = make(chan struct{})

	// Pre-populate a CR watcher
	wm.crWatchers[formatGVRKey(crGVR)] = &crWatcher{
		gvr:     crGVR,
		watcher: newFakeWatcher(),
		stopCh:  make(chan struct{}),
	}

	crd := makeTestCRD("test.example.com", "example.com", "TestResource", "testresources")

	err := wm.handleCRDDeleted(crd)
	require.NoError(t, err)

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

	// Pre-populate watcher
	wm.crWatchers[formatGVRKey(gvr)] = &crWatcher{
		gvr:     gvr,
		watcher: newFakeWatcher(),
		stopCh:  make(chan struct{}),
	}

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

	// Both CRDs should emit logs (all CRDs are emitted regardless of filter)
	assert.Equal(t, 2, sink.LogRecordCount(), "expected 2 CRD logs emitted")

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
