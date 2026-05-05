//nolint:testpackage
package k8scrdreceiver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/tracker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// recordingReconcileRecorder embeds NoopRecorder and captures CR informer
// reconcile outcomes so tests can assert on them.
type recordingReconcileRecorder struct {
	metrics.NoopRecorder

	mu       sync.Mutex
	outcomes []metrics.CRInformerOutcome
}

func (r *recordingReconcileRecorder) RecordCRInformerReconcile(_ context.Context, outcome metrics.CRInformerOutcome) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outcomes = append(r.outcomes, outcome)
}

func (r *recordingReconcileRecorder) snapshot() []metrics.CRInformerOutcome {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]metrics.CRInformerOutcome(nil), r.outcomes...)
}

func TestResourceInformers_ReadCRDs(t *testing.T) {
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

	config := testConfig([]string{"example.com"}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	crds := ri.ReadCRDs()
	require.Len(t, crds, 1)
	assert.Equal(t, "testresources.example.com", crds[0].GetName())
}

func TestResourceInformers_ReadCRs(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "example.com", "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", "example.com", "TestResource", "testresources")
	cr := makeTestCR("my-resource", "default", "example.com", "v1", "TestResource")

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: "CustomResourceDefinitionList",
			{Group: "example.com", Version: "v1", Resource: "testresources"}: "TestResourceList",
		},
		crd, cr,
	)

	config := testConfig([]string{"example.com"}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}
	crs := ri.ReadCRs()
	require.Contains(t, crs, gvr)
	require.Len(t, crs[gvr], 1)
	assert.Equal(t, "my-resource", crs[gvr][0].GetName())
}

func TestResourceInformers_CRDAddStartsCRInformer(t *testing.T) {
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

	config := testConfig([]string{"example.com"}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.mu.RLock()
	_, exists := ri.crInformers["example.com/v1/testresources"]
	ri.mu.RUnlock()

	assert.True(t, exists, "CR informer should be started for matching CRD")
}

func TestResourceInformers_CRDDeleteStopsCRInformer(t *testing.T) {
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

	config := testConfig([]string{"example.com"}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	// Verify CR informer is running
	ri.mu.RLock()
	_, exists := ri.crInformers["example.com/v1/testresources"]
	ri.mu.RUnlock()
	require.True(t, exists, "CR informer should be started")

	// Delete the CRD
	err = client.Resource(crdGVR).Delete(ctx, "testresources.example.com", metav1.DeleteOptions{})
	require.NoError(t, err)

	// CR informer should be stopped
	assert.Eventually(t, func() bool {
		ri.mu.RLock()
		defer ri.mu.RUnlock()
		_, exists := ri.crInformers["example.com/v1/testresources"]
		return !exists
	}, 10*time.Second, 100*time.Millisecond, "CR informer should be stopped after CRD deletion")
}

func TestResourceInformers_FiltersAPIGroups(t *testing.T) {
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

	config := testConfig([]string{"allowed.com"}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.mu.RLock()
	_, allowedExists := ri.crInformers["allowed.com/v1/alloweds"]
	_, blockedExists := ri.crInformers["blocked.com/v1/blockeds"]
	ri.mu.RUnlock()

	assert.True(t, allowedExists, "allowed group should have CR informer")
	assert.False(t, blockedExists, "blocked group should not have CR informer")

	// ReadCRDs should only return the allowed CRD
	crds := ri.ReadCRDs()
	require.Len(t, crds, 1)
	assert.Equal(t, "alloweds.allowed.com", crds[0].GetName())
}

func TestResourceInformers_ExcludeFilter(t *testing.T) {
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

	config := testConfig([]string{"*"}, []string{"excluded.com"})
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.mu.RLock()
	_, includedExists := ri.crInformers["included.com/v1/includes"]
	_, excludedExists := ri.crInformers["excluded.com/v1/excludes"]
	ri.mu.RUnlock()

	assert.True(t, includedExists, "included group should have CR informer")
	assert.False(t, excludedExists, "excluded group should not have CR informer")
}

func TestResourceInformers_ForbiddenTrackerSkipsResource(t *testing.T) {
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

	config := testConfig([]string{"example.com"}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ft.MarkForbidden(crGVR)

	ri := newResourceInformers(testSettings(t), config, client, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.mu.RLock()
	_, exists := ri.crInformers["example.com/v1/testresources"]
	ri.mu.RUnlock()

	assert.False(t, exists, "CR informer should not start for forbidden resource")
}

func TestResourceInformers_Shutdown(t *testing.T) {
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

	config := testConfig([]string{"example.com"}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())

	err := ri.Start(ctx)
	require.NoError(t, err)

	// Verify informer is running
	ri.mu.RLock()
	require.NotEmpty(t, ri.crInformers)
	ri.mu.RUnlock()

	cancel()
	// Use a fresh context for Shutdown — the caller's ctx is already cancelled,
	// which Shutdown treats as "exit now without waiting for goroutines".
	err = ri.Shutdown(context.Background())
	require.NoError(t, err)

	// All CR informers should be cleaned up
	ri.mu.RLock()
	assert.Empty(t, ri.crInformers)
	ri.mu.RUnlock()
}

func TestResourceInformers_ReconcileStartsMissingInformer(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "example.com", "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", "example.com", "TestResource", "testresources")
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}
	key := "example.com/v1/testresources"

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: "CustomResourceDefinitionList",
			gvr:    "TestResourceList",
		},
		crd,
	)

	config := testConfig([]string{"example.com"}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	rec := &recordingReconcileRecorder{}
	ri := newResourceInformers(testSettings(t), config, client, ft, rec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, ri.Start(ctx))
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	// Simulate a CR informer that failed during initial startup: drop it from the
	// map without going through stopCRInformer (which would also stop the goroutine).
	ri.mu.Lock()
	entry := ri.crInformers[key]
	delete(ri.crInformers, key)
	ri.mu.Unlock()
	require.NotNil(t, entry, "expected initial CR informer for example.com to be running")
	close(entry.stopCh)

	ri.reconcileCRInformers(ctx)

	ri.mu.RLock()
	_, exists := ri.crInformers[key]
	ri.mu.RUnlock()
	assert.True(t, exists, "reconciler should have started the missing CR informer")

	outcomes := rec.snapshot()
	require.Len(t, outcomes, 1)
	assert.Equal(t, metrics.CRInformerStarted, outcomes[0])
}

func TestResourceInformers_ReconcileNoOpForRunningInformer(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "example.com", "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", "example.com", "TestResource", "testresources")
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: "CustomResourceDefinitionList",
			gvr:    "TestResourceList",
		},
		crd,
	)

	config := testConfig([]string{"example.com"}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	rec := &recordingReconcileRecorder{}
	ri := newResourceInformers(testSettings(t), config, client, ft, rec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, ri.Start(ctx))
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.reconcileCRInformers(ctx)

	outcomes := rec.snapshot()
	require.Len(t, outcomes, 1)
	assert.Equal(t, metrics.CRInformerExists, outcomes[0])
}

func TestResourceInformers_ReconcileSkipsForbidden(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "example.com", "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", "example.com", "TestResource", "testresources")
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: "CustomResourceDefinitionList",
			gvr:    "TestResourceList",
		},
		crd,
	)

	config := testConfig([]string{"example.com"}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ft.MarkForbidden(gvr)

	rec := &recordingReconcileRecorder{}
	ri := newResourceInformers(testSettings(t), config, client, ft, rec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, ri.Start(ctx))
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.reconcileCRInformers(ctx)

	outcomes := rec.snapshot()
	require.Len(t, outcomes, 1)
	assert.Equal(t, metrics.CRInformerForbidden, outcomes[0])
}
