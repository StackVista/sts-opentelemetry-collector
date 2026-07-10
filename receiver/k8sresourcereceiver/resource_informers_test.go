//nolint:testpackage
package k8sresourcereceiver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/tracker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

// reconcileRecord captures one informer-reconcile metric emission.
type reconcileRecord struct {
	kind    metrics.InformerKind
	outcome metrics.InformerOutcome
}

// recordingReconcileRecorder embeds NoopRecorder and captures informer reconcile
// emissions so tests can assert on outcome (and, when relevant, kind).
type recordingReconcileRecorder struct {
	metrics.NoopRecorder

	mu      sync.Mutex
	records []reconcileRecord
}

func (r *recordingReconcileRecorder) RecordInformerReconcile(
	_ context.Context, kind metrics.InformerKind, outcome metrics.InformerOutcome,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, reconcileRecord{kind: kind, outcome: outcome})
}

func (r *recordingReconcileRecorder) snapshot() []reconcileRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]reconcileRecord(nil), r.records...)
}

func TestResourceInformers_ReadCRDs(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, testExampleGroup, "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", testExampleGroup, "TestResource", testTestResources)

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: testCRDListKind,
			{Group: testExampleGroup, Version: "v1", Resource: testTestResources}: testResourceListKind,
		},
		crd,
	)

	config := testConfig([]string{testExampleGroup}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, nil, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	crds := ri.ReadCRDs()
	require.Len(t, crds, 1)
	assert.Equal(t, "testresources.example.com", crds[0].GetName())
}

func TestResourceInformers_ReadObjects_CRD(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, testExampleGroup, "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", testExampleGroup, "TestResource", testTestResources)
	cr := makeTestCR("my-resource", "default", testExampleGroup, "v1", "TestResource")

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: testCRDListKind,
			{Group: testExampleGroup, Version: "v1", Resource: testTestResources}: testResourceListKind,
		},
		crd, cr,
	)

	config := testConfig([]string{testExampleGroup}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, nil, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	gvr := schema.GroupVersionResource{Group: testExampleGroup, Version: "v1", Resource: testTestResources}
	objs := ri.ReadObjects()
	require.Contains(t, objs, gvr)
	require.Len(t, objs[gvr].Objects, 1)
	assert.Equal(t, "my-resource", objs[gvr].Objects[0].GetName())
	assert.Equal(t, ObjectSourceCR, objs[gvr].Source)
}

func TestResourceInformers_CRDAddStartsCRInformer(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, testExampleGroup, "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", testExampleGroup, "TestResource", testTestResources)

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: testCRDListKind,
			{Group: testExampleGroup, Version: "v1", Resource: testTestResources}: testResourceListKind,
		},
		crd,
	)

	config := testConfig([]string{testExampleGroup}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, nil, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.informersMu.RLock()
	_, exists := ri.crInformers["example.com/v1/testresources"]
	ri.informersMu.RUnlock()

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
	ri := newResourceInformers(testSettings(t), config, client, nil, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	// Verify CR informer is running
	ri.informersMu.RLock()
	_, exists := ri.crInformers["example.com/v1/testresources"]
	ri.informersMu.RUnlock()
	require.True(t, exists, "CR informer should be started")

	// Delete the CRD
	err = client.Resource(crdGVR).Delete(ctx, "testresources.example.com", metav1.DeleteOptions{})
	require.NoError(t, err)

	// CR informer should be stopped
	assert.Eventually(t, func() bool {
		ri.informersMu.RLock()
		defer ri.informersMu.RUnlock()
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
	ri := newResourceInformers(testSettings(t), config, client, nil, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.informersMu.RLock()
	_, allowedExists := ri.crInformers["allowed.com/v1/alloweds"]
	_, blockedExists := ri.crInformers["blocked.com/v1/blockeds"]
	ri.informersMu.RUnlock()

	assert.True(t, allowedExists, "allowed group should have CR informer")
	assert.False(t, blockedExists, "blocked group should not have CR informer")

	// ReadCRDs returns all CRDs; CR filtering only controls CR informers.
	crds := ri.ReadCRDs()
	require.Len(t, crds, 2)
	assert.ElementsMatch(t, []string{"alloweds.allowed.com", "blockeds.blocked.com"}, []string{crds[0].GetName(), crds[1].GetName()})
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
	ri := newResourceInformers(testSettings(t), config, client, nil, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.informersMu.RLock()
	_, includedExists := ri.crInformers["included.com/v1/includes"]
	_, excludedExists := ri.crInformers["excluded.com/v1/excludes"]
	ri.informersMu.RUnlock()

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

	ri := newResourceInformers(testSettings(t), config, client, nil, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ri.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.informersMu.RLock()
	_, exists := ri.crInformers["example.com/v1/testresources"]
	ri.informersMu.RUnlock()

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
	ri := newResourceInformers(testSettings(t), config, client, nil, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())

	err := ri.Start(ctx)
	require.NoError(t, err)

	// Verify informer is running
	ri.informersMu.RLock()
	require.NotEmpty(t, ri.crInformers)
	ri.informersMu.RUnlock()

	cancel()
	// Use a fresh context for Shutdown — the caller's ctx is already cancelled,
	// which Shutdown treats as "exit now without waiting for goroutines".
	err = ri.Shutdown(context.Background())
	require.NoError(t, err)

	// All CR informers should be cleaned up
	ri.informersMu.RLock()
	assert.Empty(t, ri.crInformers)
	ri.informersMu.RUnlock()
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
	ri := newResourceInformers(testSettings(t), config, client, nil, ft, rec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, ri.Start(ctx))
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	// Simulate a CR informer that failed during initial startup: drop it from the
	// map without going through stopCRInformer (which would also stop the goroutine).
	ri.informersMu.Lock()
	entry := ri.crInformers[key]
	delete(ri.crInformers, key)
	ri.informersMu.Unlock()
	require.NotNil(t, entry, "expected initial CR informer for example.com to be running")
	close(entry.stopCh)

	ri.reconcileCRInformers(ctx)

	ri.informersMu.RLock()
	_, exists := ri.crInformers[key]
	ri.informersMu.RUnlock()
	assert.True(t, exists, "reconciler should have started the missing CR informer")

	records := rec.snapshot()
	require.Len(t, records, 1)
	assert.Equal(t, reconcileRecord{kind: metrics.InformerKindCR, outcome: metrics.InformerStarted}, records[0])
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
	ri := newResourceInformers(testSettings(t), config, client, nil, ft, rec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, ri.Start(ctx))
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.reconcileCRInformers(ctx)

	records := rec.snapshot()
	require.Len(t, records, 1)
	assert.Equal(t, reconcileRecord{kind: metrics.InformerKindCR, outcome: metrics.InformerExists}, records[0])
}

// --- Static informer tests ---

// podsGVR is the GVR used by static informer tests below.
//
//nolint:gochecknoglobals
var podsGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}

//nolint:unparam
func makeTestPod(name, namespace string, labels map[string]interface{}) *unstructured.Unstructured {
	meta := map[string]interface{}{
		testNameKey: name,
	}
	if namespace != "" {
		meta["namespace"] = namespace
	}
	if labels != nil {
		meta["labels"] = labels
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			testAPIVersionKey: "v1",
			testKindKey:       "Pod",
			testMetadataKey:   meta,
			testSpecKey:       map[string]interface{}{},
		},
	}
}

// staticPodClient returns a fake dynamic client preloaded with the given pods
// and the CRD GVR (required so the CRD informer can start without errors).
func staticPodClient(pods ...*unstructured.Unstructured) *dynamicfake.FakeDynamicClient {
	s := testScheme()
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "", Version: "v1", Kind: "PodList"},
		&unstructured.UnstructuredList{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"},
		&unstructured.Unstructured{},
	)
	objs := make([]runtime.Object, 0, len(pods))
	for _, p := range pods {
		objs = append(objs, p)
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(s,
		map[schema.GroupVersionResource]string{
			crdGVR:  testCRDListKind,
			podsGVR: "PodList",
		},
		objs...,
	)
}

func TestResourceInformers_StaticInformer_StartsAndReads(t *testing.T) {
	pod := makeTestPod("nginx", "default", nil)
	client := staticPodClient(pod)

	resolved := []resolvedObjectWatch{{
		ObjectWatch: ObjectWatch{Name: "pods"},
		GVR:         podsGVR,
		Namespaced:  true,
	}}

	config := testConfig([]string{testExampleGroup}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, resolved, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, ri.Start(ctx))
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.informersMu.RLock()
	require.Len(t, ri.staticInformers, 1, "one static informer per resolvedObject when Namespaces is empty")
	ri.informersMu.RUnlock()

	objs := ri.ReadObjects()
	require.Contains(t, objs, podsGVR)
	require.Len(t, objs[podsGVR].Objects, 1)
	assert.Equal(t, "nginx", objs[podsGVR].Objects[0].GetName())
	assert.Equal(t, ObjectSourceStatic, objs[podsGVR].Source)
}

func TestResourceInformers_StaticInformer_NamespaceExpansion(t *testing.T) {
	podA := makeTestPod("a", "ns-a", nil)
	podB := makeTestPod("b", "ns-b", nil)
	podC := makeTestPod("c", "ns-c", nil)
	client := staticPodClient(podA, podB, podC)

	resolved := []resolvedObjectWatch{{
		ObjectWatch: ObjectWatch{Name: "pods", Namespaces: []string{"ns-a", "ns-b"}},
		GVR:         podsGVR,
		Namespaced:  true,
	}}

	config := testConfig([]string{testExampleGroup}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, resolved, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, ri.Start(ctx))
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.informersMu.RLock()
	require.Len(t, ri.staticInformers, 2, "one static informer per requested namespace")
	ri.informersMu.RUnlock()

	objs := ri.ReadObjects()
	require.Contains(t, objs, podsGVR)
	names := make([]string, 0, len(objs[podsGVR].Objects))
	for _, p := range objs[podsGVR].Objects {
		names = append(names, p.GetName())
	}
	assert.ElementsMatch(t, []string{"a", "b"}, names, "ns-c pod must not appear")
}

func TestResourceInformers_StaticInformer_ClusterScoped(t *testing.T) {
	nsObj := makeTestPod("kube-system", "", nil)
	// Reuse staticPodClient and just register namespaces as a side resource.
	s := testScheme()
	nsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "NamespaceList"}, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"}, &unstructured.Unstructured{})
	nsObj.Object[testKindKey] = "Namespace"
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(s,
		map[schema.GroupVersionResource]string{
			crdGVR: testCRDListKind,
			nsGVR:  "NamespaceList",
		},
		nsObj,
	)

	resolved := []resolvedObjectWatch{{
		ObjectWatch: ObjectWatch{Name: "namespaces"},
		GVR:         nsGVR,
		Namespaced:  false,
	}}

	config := testConfig([]string{testExampleGroup}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, resolved, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, ri.Start(ctx))
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.informersMu.RLock()
	require.Len(t, ri.staticInformers, 1)
	for _, entry := range ri.staticInformers {
		assert.Equal(t, "", entry.namespace, "cluster-scoped informer must have empty namespace")
	}
	ri.informersMu.RUnlock()

	objs := ri.ReadObjects()
	require.Contains(t, objs, nsGVR)
	require.Len(t, objs[nsGVR].Objects, 1)
}

func TestResourceInformers_StaticInformer_ReadObjectsMergesWithCRs(t *testing.T) {
	registerCRGVR := func(s *runtime.Scheme) {
		s.AddKnownTypeWithName(
			schema.GroupVersionKind{Group: testExampleGroup, Version: "v1", Kind: "TestResourceList"},
			&unstructured.UnstructuredList{},
		)
		s.AddKnownTypeWithName(
			schema.GroupVersionKind{Group: testExampleGroup, Version: "v1", Kind: "TestResource"},
			&unstructured.Unstructured{},
		)
	}
	s := testScheme()
	registerCRGVR(s)
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "PodList"}, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}, &unstructured.Unstructured{})

	crd := makeTestCRDUnstructured("testresources.example.com", testExampleGroup, "TestResource", testTestResources)
	cr := makeTestCR("my-resource", "default", testExampleGroup, "v1", "TestResource")
	pod := makeTestPod("nginx", "default", nil)

	crGVR := schema.GroupVersionResource{Group: testExampleGroup, Version: "v1", Resource: testTestResources}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(s,
		map[schema.GroupVersionResource]string{
			crdGVR:  testCRDListKind,
			crGVR:   testResourceListKind,
			podsGVR: "PodList",
		},
		crd, cr, pod,
	)

	resolved := []resolvedObjectWatch{{
		ObjectWatch: ObjectWatch{Name: "pods"},
		GVR:         podsGVR,
		Namespaced:  true,
	}}

	config := testConfig([]string{testExampleGroup}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, resolved, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, ri.Start(ctx))
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	objs := ri.ReadObjects()
	require.Contains(t, objs, crGVR, "CR-discovered GVR must appear")
	require.Contains(t, objs, podsGVR, "static-watch GVR must appear")
	assert.Equal(t, "my-resource", objs[crGVR].Objects[0].GetName())
	assert.Equal(t, ObjectSourceCR, objs[crGVR].Source)
	assert.Equal(t, "nginx", objs[podsGVR].Objects[0].GetName())
	assert.Equal(t, ObjectSourceStatic, objs[podsGVR].Source)
}

func TestResourceInformers_StaticInformer_Shutdown(t *testing.T) {
	pod := makeTestPod("nginx", "default", nil)
	client := staticPodClient(pod)

	resolved := []resolvedObjectWatch{{
		ObjectWatch: ObjectWatch{Name: "pods"},
		GVR:         podsGVR,
		Namespaced:  true,
	}}

	config := testConfig([]string{testExampleGroup}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, resolved, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, ri.Start(ctx))

	ri.informersMu.RLock()
	require.NotEmpty(t, ri.staticInformers)
	ri.informersMu.RUnlock()

	cancel()
	require.NoError(t, ri.Shutdown(context.Background()))

	ri.informersMu.RLock()
	assert.Empty(t, ri.staticInformers, "Shutdown must clear staticInformers map")
	ri.informersMu.RUnlock()
}

func TestResourceInformers_StaticInformer_ReconcileStartsMissingInformer(t *testing.T) {
	pod := makeTestPod("nginx", "default", nil)
	client := staticPodClient(pod)

	resolved := []resolvedObjectWatch{{
		ObjectWatch: ObjectWatch{Name: "pods"},
		GVR:         podsGVR,
		Namespaced:  true,
	}}

	config := testConfig([]string{testExampleGroup}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	rec := &recordingReconcileRecorder{}
	ri := newResourceInformers(testSettings(t), config, client, resolved, ft, rec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, ri.Start(ctx))
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	// Drop the entry to simulate a missed startup (same trick as the CR informer test).
	key := formatStaticInformerKey(podsGVR, "", "", "")
	ri.informersMu.Lock()
	entry := ri.staticInformers[key]
	delete(ri.staticInformers, key)
	ri.informersMu.Unlock()
	require.NotNil(t, entry)
	close(entry.stopCh)

	ri.reconcileStaticInformers(ctx)

	ri.informersMu.RLock()
	_, exists := ri.staticInformers[key]
	ri.informersMu.RUnlock()
	assert.True(t, exists, "reconciler should have re-started the missing static informer")
}

// TestResourceInformers_StaticInformer_PerEntryForbidden is the regression
// guard for the pre-fix bug where a 403 on (pods, ns-a) called
// ForbiddenTracker.MarkForbidden(podsGVR) and thereby suppressed pods/ns-b too.
// The fix uses per-entry forbidden state; this test asserts both halves of
// that contract: ns-a is forbidden, ns-b still emits data, and the GVR-wide
// tracker is untouched.
func TestResourceInformers_StaticInformer_PerEntryForbidden(t *testing.T) {
	podA := makeTestPod("a", "ns-a", nil)
	podB := makeTestPod("b", "ns-b", nil)
	client := staticPodClient(podA, podB)

	client.PrependReactor("list", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetNamespace() == "ns-a" {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Group: "", Resource: "pods"},
				"",
				errors.New("not allowed in ns-a"),
			)
		}
		return false, nil, nil
	})

	resolved := []resolvedObjectWatch{{
		ObjectWatch: ObjectWatch{Name: "pods", Namespaces: []string{"ns-a", "ns-b"}},
		GVR:         podsGVR,
		Namespaced:  true,
	}}

	config := testConfig([]string{testExampleGroup}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	ri := newResourceInformers(testSettings(t), config, client, resolved, ft, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, ri.Start(ctx))
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	nsAKey := formatStaticInformerKey(podsGVR, "ns-a", "", "")
	nsBKey := formatStaticInformerKey(podsGVR, "ns-b", "", "")

	ri.informersMu.RLock()
	_, hasA := ri.staticInformers[nsAKey]
	_, hasB := ri.staticInformers[nsBKey]
	ri.informersMu.RUnlock()
	assert.False(t, hasA, "ns-a entry must be skipped after 403")
	assert.True(t, hasB, "ns-b entry must still start")

	ri.staticForbiddenMu.RLock()
	_, aForbidden := ri.staticForbidden[nsAKey]
	_, bForbidden := ri.staticForbidden[nsBKey]
	ri.staticForbiddenMu.RUnlock()
	assert.True(t, aForbidden, "ns-a key must be recorded forbidden")
	assert.False(t, bForbidden, "ns-b key must not be recorded forbidden")

	shouldRetry, _ := ft.ShouldRetry(podsGVR)
	assert.True(t, shouldRetry, "GVR-wide ForbiddenTracker must NOT be marked from a static-informer 403")

	objs := ri.ReadObjects()
	require.Contains(t, objs, podsGVR, "ns-b pods must surface despite ns-a being forbidden")
	require.Len(t, objs[podsGVR].Objects, 1)
	assert.Equal(t, "b", objs[podsGVR].Objects[0].GetName())
}

func TestFormatStaticInformerKey_Distinct(t *testing.T) {
	gvr := podsGVR
	keys := map[string]struct{}{
		formatStaticInformerKey(gvr, "", "", ""):                 {},
		formatStaticInformerKey(gvr, "ns-a", "", ""):             {},
		formatStaticInformerKey(gvr, "ns-b", "", ""):             {},
		formatStaticInformerKey(gvr, "", "app=foo", ""):          {},
		formatStaticInformerKey(gvr, "", "", "status.phase=Run"): {},
		formatStaticInformerKey(gvr, "ns-a", "app=foo", ""):      {},
	}
	assert.Len(t, keys, 6, "each combination must produce a distinct key")
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
	ri := newResourceInformers(testSettings(t), config, client, nil, ft, rec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, ri.Start(ctx))
	defer func() { require.NoError(t, ri.Shutdown(ctx)) }()

	ri.reconcileCRInformers(ctx)

	records := rec.snapshot()
	require.Len(t, records, 1)
	assert.Equal(t, reconcileRecord{kind: metrics.InformerKindCR, outcome: metrics.InformerForbidden}, records[0])
}
