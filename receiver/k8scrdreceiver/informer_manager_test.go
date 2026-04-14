//nolint:testpackage // Tests require access to internal types
package k8scrdreceiver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap/zaptest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// testScheme returns a runtime.Scheme with the GVRs registered that the fake client needs.
func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	// Register the GVRs we'll use so the fake client can handle them
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinitionList"},
		&unstructured.UnstructuredList{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"},
		&unstructured.Unstructured{},
	)
	return s
}

// registerCRGVR registers a CR GVR in the scheme so the fake client can handle it.
//
//nolint:unparam
func registerCRGVR(s *runtime.Scheme, group, version, kind string) {
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: group, Version: version, Kind: kind + "List"},
		&unstructured.UnstructuredList{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: group, Version: version, Kind: kind},
		&unstructured.Unstructured{},
	)
}

func testSettings(t *testing.T) receiver.Settings {
	t.Helper()
	return receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: zaptest.NewLogger(t),
		},
	}
}

func testConfig(includes []string, excludes []string) *Config {
	cfg := &Config{
		Interval:            1 * time.Hour,
		IncludeInitialState: true,
		DiscoveryMode:       DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: includes,
			Exclude: excludes,
		},
	}
	// Validate compiles regex patterns
	_ = cfg.Validate()
	return cfg
}

func makeTestCRDUnstructured(name, group, kind, plural string) *unstructured.Unstructured {
	return makeTestCRDWithVersions(name, group, kind, plural, []map[string]interface{}{
		{"name": "v1", "storage": true, "served": true},
	})
}

func makeTestCRDWithVersions(name, group, kind, plural string, versions []map[string]interface{}) *unstructured.Unstructured {
	versionList := make([]interface{}, len(versions))
	for i, v := range versions {
		versionList[i] = v
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apiextensions.k8s.io/v1",
			"kind":       "CustomResourceDefinition",
			"metadata": map[string]interface{}{
				"name": name,
			},
			"spec": map[string]interface{}{
				"group": group,
				"names": map[string]interface{}{
					"kind":   kind,
					"plural": plural,
				},
				"scope":    "Namespaced",
				"versions": versionList,
			},
		},
	}
}

//nolint:unparam
func makeTestCR(name, namespace, group, version, kind string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": group + "/" + version,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"name": name,
			},
			"spec": map[string]interface{}{},
		},
	}
	if namespace != "" {
		//nolint:forcetypeassert
		obj.Object["metadata"].(map[string]interface{})["namespace"] = namespace
	}
	return obj
}

func TestInformerManager_CRDAddStartsCRInformer(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "example.com", "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", "example.com", "TestResource", "testresources")
	cr := makeTestCR("my-resource", "default", "example.com", "v1", "TestResource")

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}: "CustomResourceDefinitionList",
			{Group: "example.com", Version: "v1", Resource: "testresources"}:                      "TestResourceList",
		},
		crd, cr,
	)

	sink := &consumertest.LogsSink{}
	config := testConfig([]string{"example.com"}, nil)
	ft := newForbiddenTracker(1 * time.Hour)
	mgr := newInformerManager(testSettings(t), config, sink, client, ft)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Shutdown(ctx)) }()

	// Wait for CR informer to start and initial events to be processed
	assert.Eventually(t, func() bool {
		mgr.mu.RLock()
		defer mgr.mu.RUnlock()
		_, exists := mgr.crInformers["example.com/v1/testresources"]
		return exists
	}, 5*time.Second, 100*time.Millisecond, "CR informer should be started")

	// Should have emitted logs: 1 CRD + 1 CR (from informer initial sync AddFunc)
	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 2
	}, 5*time.Second, 100*time.Millisecond, "should emit CRD and CR logs")
}

func TestInformerManager_FiltersAPIGroups(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "allowed.com", "v1", "AllowedResource")
	registerCRGVR(scheme, "blocked.com", "v1", "BlockedResource")

	allowedCRD := makeTestCRDUnstructured("alloweds.allowed.com", "allowed.com", "AllowedResource", "alloweds")
	blockedCRD := makeTestCRDUnstructured("blockeds.blocked.com", "blocked.com", "BlockedResource", "blockeds")

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: "CustomResourceDefinitionList",
			{Group: "allowed.com", Version: "v1", Resource: "alloweds"}: "AllowedResourceList",
			{Group: "blocked.com", Version: "v1", Resource: "blockeds"}: "BlockedResourceList",
		},
		allowedCRD, blockedCRD,
	)

	sink := &consumertest.LogsSink{}
	config := testConfig([]string{"allowed.com"}, nil)
	ft := newForbiddenTracker(1 * time.Hour)
	mgr := newInformerManager(testSettings(t), config, sink, client, ft)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Shutdown(ctx)) }()

	// Wait for informers to start
	assert.Eventually(t, func() bool {
		mgr.mu.RLock()
		defer mgr.mu.RUnlock()
		return len(mgr.crInformers) > 0
	}, 5*time.Second, 100*time.Millisecond)

	// Only allowed group should have CR informer
	mgr.mu.RLock()
	_, allowedExists := mgr.crInformers["allowed.com/v1/alloweds"]
	_, blockedExists := mgr.crInformers["blocked.com/v1/blockeds"]
	mgr.mu.RUnlock()

	assert.True(t, allowedExists, "allowed group should have CR informer")
	assert.False(t, blockedExists, "blocked group should not have CR informer")
}

func TestInformerManager_CRDDeleteStopsCRInformer(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "example.com", "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", "example.com", "TestResource", "testresources")

	crGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: "CustomResourceDefinitionList",
			crGVR:  "TestResourceList",
		},
		crd,
	)

	sink := &consumertest.LogsSink{}
	config := testConfig([]string{"example.com"}, nil)
	ft := newForbiddenTracker(1 * time.Hour)
	mgr := newInformerManager(testSettings(t), config, sink, client, ft)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Shutdown(ctx)) }()

	// Wait for CR informer to start
	assert.Eventually(t, func() bool {
		mgr.mu.RLock()
		defer mgr.mu.RUnlock()
		_, exists := mgr.crInformers["example.com/v1/testresources"]
		return exists
	}, 5*time.Second, 100*time.Millisecond, "CR informer should be started")

	// Delete the CRD
	err = client.Resource(crdGVR).Delete(ctx, "testresources.example.com", metav1.DeleteOptions{})
	require.NoError(t, err)

	// CR informer should be stopped
	assert.Eventually(t, func() bool {
		mgr.mu.RLock()
		defer mgr.mu.RUnlock()
		_, exists := mgr.crInformers["example.com/v1/testresources"]
		return !exists
	}, 5*time.Second, 100*time.Millisecond, "CR informer should be stopped after CRD deletion")
}

func TestInformerManager_CRLifecycleEvents(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "example.com", "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", "example.com", "TestResource", "testresources")
	crGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: "CustomResourceDefinitionList",
			crGVR:  "TestResourceList",
		},
		crd,
	)

	sink := &consumertest.LogsSink{}
	config := testConfig([]string{"example.com"}, nil)
	ft := newForbiddenTracker(1 * time.Hour)
	mgr := newInformerManager(testSettings(t), config, sink, client, ft)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Shutdown(ctx)) }()

	// Wait for CR informer to be ready
	assert.Eventually(t, func() bool {
		mgr.mu.RLock()
		defer mgr.mu.RUnlock()
		_, exists := mgr.crInformers["example.com/v1/testresources"]
		return exists
	}, 5*time.Second, 100*time.Millisecond)

	// Clear the sink to isolate CR events
	initialCount := sink.LogRecordCount()

	// Create a CR
	cr := makeTestCR("test-cr", "default", "example.com", "v1", "TestResource")
	_, err = client.Resource(crGVR).Namespace("default").Create(ctx, cr, metav1.CreateOptions{})
	require.NoError(t, err)

	// Should emit an ADDED log
	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() > initialCount
	}, 5*time.Second, 100*time.Millisecond, "should emit CR add log")

	countAfterAdd := sink.LogRecordCount()

	// Update the CR
	cr.Object["spec"] = map[string]interface{}{"updated": true}
	_, err = client.Resource(crGVR).Namespace("default").Update(ctx, cr, metav1.UpdateOptions{})
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() > countAfterAdd
	}, 5*time.Second, 100*time.Millisecond, "should emit CR update log")

	countAfterUpdate := sink.LogRecordCount()

	// Delete the CR
	err = client.Resource(crGVR).Namespace("default").Delete(ctx, "test-cr", metav1.DeleteOptions{})
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() > countAfterUpdate
	}, 5*time.Second, 100*time.Millisecond, "should emit CR delete log")
}

func TestInformerManager_SnapshotFromCache(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "example.com", "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", "example.com", "TestResource", "testresources")
	cr1 := makeTestCR("cr-1", "default", "example.com", "v1", "TestResource")
	cr2 := makeTestCR("cr-2", "default", "example.com", "v1", "TestResource")

	crGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: "CustomResourceDefinitionList",
			crGVR:  "TestResourceList",
		},
		crd, cr1, cr2,
	)

	sink := &consumertest.LogsSink{}
	config := testConfig([]string{"example.com"}, nil)
	ft := newForbiddenTracker(1 * time.Hour)
	mgr := newInformerManager(testSettings(t), config, sink, client, ft)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Shutdown(ctx)) }()

	// Wait for informers to sync
	assert.Eventually(t, func() bool {
		mgr.mu.RLock()
		defer mgr.mu.RUnlock()
		entry, exists := mgr.crInformers["example.com/v1/testresources"]
		return exists && entry.informer.HasSynced()
	}, 5*time.Second, 100*time.Millisecond)

	// Clear sink and trigger a manual snapshot
	sink.Reset()

	mgr.performSnapshot()

	// Should have 1 CRD + 2 CRs in snapshot
	assert.Equal(t, 3, sink.LogRecordCount(), "snapshot should emit 1 CRD + 2 CRs")
}

func TestInformerManager_ForbiddenTrackerSkipsResource(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "example.com", "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", "example.com", "TestResource", "testresources")
	crGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: "CustomResourceDefinitionList",
			crGVR:  "TestResourceList",
		},
		crd,
	)

	sink := &consumertest.LogsSink{}
	config := testConfig([]string{"example.com"}, nil)

	// Pre-mark the GVR as forbidden
	ft := newForbiddenTracker(1 * time.Hour)
	ft.markForbidden(crGVR)

	mgr := newInformerManager(testSettings(t), config, sink, client, ft)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Shutdown(ctx)) }()

	// Wait for CRD to be processed
	time.Sleep(500 * time.Millisecond)

	// CR informer should NOT be started because the GVR is forbidden
	mgr.mu.RLock()
	_, exists := mgr.crInformers["example.com/v1/testresources"]
	mgr.mu.RUnlock()

	assert.False(t, exists, "CR informer should not start for forbidden resource")

	// CRD log should still be emitted
	assert.True(t, sink.LogRecordCount() >= 1, "CRD add log should still be emitted")
}

func TestInformerManager_Shutdown(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "example.com", "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", "example.com", "TestResource", "testresources")

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: "CustomResourceDefinitionList",
			{Group: "example.com", Version: "v1", Resource: "testresources"}: "TestResourceList",
		},
		crd,
	)

	sink := &consumertest.LogsSink{}
	config := testConfig([]string{"example.com"}, nil)
	ft := newForbiddenTracker(1 * time.Hour)
	mgr := newInformerManager(testSettings(t), config, sink, client, ft)

	ctx, cancel := context.WithCancel(context.Background())

	err := mgr.Start(ctx)
	require.NoError(t, err)

	// Wait for informers to start
	assert.Eventually(t, func() bool {
		mgr.mu.RLock()
		defer mgr.mu.RUnlock()
		return len(mgr.crInformers) > 0
	}, 5*time.Second, 100*time.Millisecond)

	// Shutdown should complete cleanly
	cancel()
	err = mgr.Shutdown(ctx)
	require.NoError(t, err)

	// All CR informers should be cleaned up
	mgr.mu.RLock()
	assert.Empty(t, mgr.crInformers)
	mgr.mu.RUnlock()
}

func TestInformerManager_ExcludeFilter(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "included.com", "v1", "IncludedResource")
	registerCRGVR(scheme, "excluded.com", "v1", "ExcludedResource")

	includedCRD := makeTestCRDUnstructured("includes.included.com", "included.com", "IncludedResource", "includes")
	excludedCRD := makeTestCRDUnstructured("excludes.excluded.com", "excluded.com", "ExcludedResource", "excludes")

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: "CustomResourceDefinitionList",
			{Group: "included.com", Version: "v1", Resource: "includes"}: "IncludedResourceList",
			{Group: "excluded.com", Version: "v1", Resource: "excludes"}: "ExcludedResourceList",
		},
		includedCRD, excludedCRD,
	)

	sink := &consumertest.LogsSink{}
	config := testConfig([]string{"*"}, []string{"excluded.com"})
	ft := newForbiddenTracker(1 * time.Hour)
	mgr := newInformerManager(testSettings(t), config, sink, client, ft)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Shutdown(ctx)) }()

	// Wait for informers
	assert.Eventually(t, func() bool {
		mgr.mu.RLock()
		defer mgr.mu.RUnlock()
		return len(mgr.crInformers) > 0
	}, 5*time.Second, 100*time.Millisecond)

	mgr.mu.RLock()
	_, includedExists := mgr.crInformers["included.com/v1/includes"]
	_, excludedExists := mgr.crInformers["excluded.com/v1/excludes"]
	mgr.mu.RUnlock()

	assert.True(t, includedExists, "included group should have CR informer")
	assert.False(t, excludedExists, "excluded group should not have CR informer")
}

func TestConfig_Simplified(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := &Config{
			Interval:            5 * time.Minute,
			IncludeInitialState: true,
			DiscoveryMode:       DiscoveryModeAPIGroups,
			APIGroupFilters: &APIGroupFilters{
				Include: []string{"example.com"},
				Exclude: []string{},
			},
		}
		require.NoError(t, cfg.Validate())
		assert.Equal(t, 5*time.Minute, cfg.Interval)
	})

	t.Run("default interval", func(t *testing.T) {
		cfg := &Config{
			DiscoveryMode: DiscoveryModeAPIGroups,
			APIGroupFilters: &APIGroupFilters{
				Include: []string{"*"},
			},
		}
		require.NoError(t, cfg.Validate())
		assert.Equal(t, 5*time.Minute, cfg.Interval)
	})

	t.Run("interval too short", func(t *testing.T) {
		cfg := &Config{
			Interval: 30 * time.Second,
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least 1 minute")
	})

	t.Run("discovery mode all", func(t *testing.T) {
		cfg := &Config{
			Interval:      5 * time.Minute,
			DiscoveryMode: DiscoveryModeAll,
		}
		require.NoError(t, cfg.Validate())
		assert.True(t, cfg.shouldWatchAPIGroup("anything.io"))
	})

	t.Run("default api group filters", func(t *testing.T) {
		cfg := &Config{
			DiscoveryMode: DiscoveryModeAPIGroups,
		}
		require.NoError(t, cfg.Validate())
		require.NotNil(t, cfg.APIGroupFilters)
		assert.Equal(t, []string{"*"}, cfg.APIGroupFilters.Include)
	})

	t.Run("empty include", func(t *testing.T) {
		cfg := &Config{
			DiscoveryMode: DiscoveryModeAPIGroups,
			APIGroupFilters: &APIGroupFilters{
				Include: []string{},
			},
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be empty")
	})
}
