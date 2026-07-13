//nolint:testpackage
package k8sresourcereceiver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/emit"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/tracker"
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
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// testScheme returns a runtime.Scheme with the GVRs registered that the fake client needs.
func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	// Register the GVRs we'll use so the fake client can handle them
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: apiExtensionsGroup, Version: "v1", Kind: testCRDListKind},
		&unstructured.UnstructuredList{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: apiExtensionsGroup, Version: "v1", Kind: testCRDKind},
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
		IncrementInterval: 1 * time.Second,
		SnapshotInterval:  1 * time.Hour, // Long to prevent snapshots during tests
		DiscoveryMode:     DiscoveryModeAPIGroups,
		CustomResourceAPIGroups: &APIGroups{
			Include: includes,
			Exclude: excludes,
		},
	}
	// Validate compiles regex patterns — panic on failure to catch test config bugs
	if err := cfg.Validate(); err != nil {
		panic("test config validation failed: " + err.Error())
	}
	return cfg
}

func makeTestCRDUnstructured(name, group, kind, plural string) *unstructured.Unstructured {
	return makeTestCRDWithVersions(name, group, kind, plural, []map[string]interface{}{
		{testNameKey: "v1", "storage": true, "served": true},
	})
}

func makeTestCRDWithVersions(name, group, kind, plural string, versions []map[string]interface{}) *unstructured.Unstructured {
	versionList := make([]interface{}, len(versions))
	for i, v := range versions {
		versionList[i] = v
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			testAPIVersionKey: apiExtensionsGroup + "/v1",
			testKindKey:       testCRDKind,
			testMetadataKey: map[string]interface{}{
				testNameKey: name,
			},
			testSpecKey: map[string]interface{}{
				testGroupKey: group,
				"names": map[string]interface{}{
					testKindKey: kind,
					"plural":    plural,
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
			testAPIVersionKey: group + "/" + version,
			testKindKey:       kind,
			testMetadataKey: map[string]interface{}{
				testNameKey: name,
			},
			testSpecKey: map[string]interface{}{},
		},
	}
	if namespace != "" {
		//nolint:forcetypeassert
		obj.Object["metadata"].(map[string]interface{})["namespace"] = namespace
	}
	return obj
}

// waitForInitialEmissions waits for the first increment loop to emit initial state.
func waitForInitialEmissions(t *testing.T, sink *consumertest.LogsSink, minCount int) {
	t.Helper()
	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() >= minCount
	}, 10*time.Second, 100*time.Millisecond, "should emit initial state")
}

// newTestCollector creates a resourceCollector backed by a real ResourceInformers for integration tests.
func newTestCollector(
	t *testing.T,
	config *Config,
	sink *consumertest.LogsSink,
	client *dynamicfake.FakeDynamicClient,
	ft *tracker.ForbiddenTracker,
) *resourceCollector {
	t.Helper()
	return newTestCollectorWithPeerStore(t, config, sink, client, ft, nil)
}

// newTestCollectorWithPeerStore is like newTestCollector but lets the caller inject a
// custom PeerStore implementation, e.g. to record ApplyDelta invocations.
func newTestCollectorWithPeerStore(
	t *testing.T,
	config *Config,
	sink *consumertest.LogsSink,
	client *dynamicfake.FakeDynamicClient,
	ft *tracker.ForbiddenTracker,
	peerStore PeerStore,
) *resourceCollector {
	t.Helper()
	settings := testSettings(t)
	informerSet := newResourceInformers(settings, config, client, nil, ft, nil)
	return newResourceCollector(settings.Logger, config, sink, informerSet, peerStore, nil, nil)
}

// recordingPeerStore captures each ApplyDelta invocation as a snapshot of the cache
// contents (resource names + versions) plus LastSnapshotTime. Used to assert two-phase
// save ordering and snapshot-timestamp continuity.
type recordingPeerStore struct {
	mu               sync.Mutex
	cache            *resourceCache
	lastSnapshotTime time.Time
	saves            []recordedSave
}

type payloadRecorder struct {
	metrics.NoopRecorder
	mu      sync.Mutex
	dropped []payloadDropRecord
	sizes   []payloadSizeRecord
	budgets []payloadBudgetRecord
}

type payloadDropRecord struct {
	source   metrics.PayloadSource
	apiGroup string
	kind     string
}

type payloadSizeRecord struct {
	source   metrics.PayloadSource
	outcome  metrics.PayloadOutcome
	apiGroup string
	kind     string
	size     int64
}

type payloadBudgetRecord struct {
	source metrics.PayloadSource
	budget int64
	used   int64
}

func (r *payloadRecorder) RecordPayloadDropped(_ context.Context, source metrics.PayloadSource, apiGroup, kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dropped = append(r.dropped, payloadDropRecord{source: source, apiGroup: apiGroup, kind: kind})
}

func (r *payloadRecorder) RecordPayloadSize(
	_ context.Context, source metrics.PayloadSource, outcome metrics.PayloadOutcome, apiGroup, kind string, sizeBytes int64,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sizes = append(r.sizes, payloadSizeRecord{source: source, outcome: outcome, apiGroup: apiGroup, kind: kind, size: sizeBytes})
}

func (r *payloadRecorder) RecordPayloadBudget(_ context.Context, source metrics.PayloadSource, budgetBytes, usedBytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.budgets = append(r.budgets, payloadBudgetRecord{source: source, budget: budgetBytes, used: usedBytes})
}

func (r *payloadRecorder) Dropped() []payloadDropRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]payloadDropRecord(nil), r.dropped...)
}

func (r *payloadRecorder) Sizes() []payloadSizeRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]payloadSizeRecord(nil), r.sizes...)
}

func (r *payloadRecorder) Budgets() []payloadBudgetRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]payloadBudgetRecord(nil), r.budgets...)
}

type recordedSave struct {
	crds             map[string]string // CRD name -> resourceVersion
	crs              map[string]string // CR key  -> resourceVersion
	lastSnapshotTime time.Time
}

type stubResourceAttributeEnricher struct {
	gvr   schema.GroupVersionResource
	attrs map[string]string
}

func (s stubResourceAttributeEnricher) AttributesFor(gvr schema.GroupVersionResource) map[string]string {
	if gvr == s.gvr {
		return s.attrs
	}
	return nil
}

func newRecordingPeerStore() *recordingPeerStore {
	return &recordingPeerStore{cache: newResourceCache()}
}

func (r *recordingPeerStore) Start(_ context.Context) error     { return nil }
func (r *recordingPeerStore) Stop()                             {}
func (r *recordingPeerStore) Bootstrap(_ context.Context) error { return nil }
func (r *recordingPeerStore) SetLeader(_ bool)                  {}

func (r *recordingPeerStore) ComputeChanges(
	currentCRDs []*unstructured.Unstructured,
	currentObjects map[schema.GroupVersionResource]ObjectGroup,
) []ResourceChange {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cache.computeChanges(currentCRDs, currentObjects)
}

func (r *recordingPeerStore) ApplyDelta(_ context.Context, delta *PeerSyncDelta) error {
	r.mu.Lock()
	r.cache.applyDelta(delta.Changes)
	if !delta.LastSnapshotTime.IsZero() {
		r.lastSnapshotTime = delta.LastSnapshotTime
	}

	snap := recordedSave{
		crds:             make(map[string]string, len(r.cache.CRDs)),
		crs:              make(map[string]string, len(r.cache.Objects)),
		lastSnapshotTime: r.lastSnapshotTime,
	}
	for name, crd := range r.cache.CRDs {
		snap.crds[name] = crd.GetResourceVersion()
	}
	for k, cr := range r.cache.Objects {
		snap.crs[k] = cr.Obj.GetResourceVersion()
	}
	r.saves = append(r.saves, snap)
	r.mu.Unlock()
	return nil
}

func (r *recordingPeerStore) IsEmpty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cache.isEmpty()
}

func (r *recordingPeerStore) LastSnapshotTime() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastSnapshotTime
}

func (r *recordingPeerStore) snapshot() []recordedSave {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedSave, len(r.saves))
	copy(out, r.saves)
	return out
}

func TestResourceCollector_EmitsInitialCRDAndCR(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "example.com", "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", "example.com", "TestResource", "testresources")
	cr := makeTestCR("my-resource", "default", "example.com", "v1", "TestResource")

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			{Group: apiExtensionsGroup, Version: "v1", Resource: "customresourcedefinitions"}: testCRDListKind,
			{Group: testExampleGroup, Version: "v1", Resource: testTestResources}:             testResourceListKind,
		},
		crd, cr,
	)

	sink := &consumertest.LogsSink{}
	config := testConfig([]string{testExampleGroup}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	collector := newTestCollector(t, config, sink, client, ft)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := collector.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, collector.Shutdown(ctx)) }()

	// Should have emitted logs from first increment (snapshot): 1 CRD + 1 CR
	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 2
	}, 10*time.Second, 100*time.Millisecond, "should emit CRD and CR logs")
}

func TestResourceCollector_CRLifecycleEvents(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, testExampleGroup, "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", testExampleGroup, "TestResource", "testresources")
	crGVR := schema.GroupVersionResource{Group: testExampleGroup, Version: "v1", Resource: "testresources"}

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: testCRDListKind,
			crGVR:  testResourceListKind,
		},
		crd,
	)

	sink := &consumertest.LogsSink{}
	config := testConfig([]string{testExampleGroup}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	collector := newTestCollector(t, config, sink, client, ft)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := collector.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, collector.Shutdown(ctx)) }()

	// Wait for initial state emission (first increment = snapshot with just the CRD)
	waitForInitialEmissions(t, sink, 1)

	// Let the increment loop settle, then capture baseline
	time.Sleep(1500 * time.Millisecond)
	initialCount := sink.LogRecordCount()

	// Create a CR — set explicit resourceVersion because the fake tracker doesn't auto-assign them.
	cr := makeTestCR("test-cr", "default", "example.com", "v1", "TestResource")
	cr.SetResourceVersion("1")
	_, err = client.Resource(crGVR).Namespace("default").Create(ctx, cr, metav1.CreateOptions{})
	require.NoError(t, err)

	// Should emit an ADDED log via the increment loop
	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() > initialCount
	}, 10*time.Second, 100*time.Millisecond, "should emit CR add log")

	countAfterAdd := sink.LogRecordCount()

	// Update the CR — bump resourceVersion so the resource cache detects the modification.
	cr.SetResourceVersion("2")
	cr.Object["spec"] = map[string]interface{}{"updated": true}
	_, err = client.Resource(crGVR).Namespace("default").Update(ctx, cr, metav1.UpdateOptions{})
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() > countAfterAdd
	}, 10*time.Second, 100*time.Millisecond, "should emit CR update log")

	countAfterUpdate := sink.LogRecordCount()

	// Delete the CR
	err = client.Resource(crGVR).Namespace("default").Delete(ctx, "test-cr", metav1.DeleteOptions{})
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() > countAfterUpdate
	}, 10*time.Second, 100*time.Millisecond, "should emit CR delete log")
}

func TestResourceCollector_SnapshotEmitsAllResources(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, testExampleGroup, "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", testExampleGroup, "TestResource", "testresources")
	cr1 := makeTestCR("cr-1", "default", testExampleGroup, "v1", "TestResource")
	cr2 := makeTestCR("cr-2", "default", testExampleGroup, "v1", "TestResource")

	crGVR := schema.GroupVersionResource{Group: testExampleGroup, Version: "v1", Resource: "testresources"}

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: testCRDListKind,
			crGVR:  testResourceListKind,
		},
		crd, cr1, cr2,
	)

	sink := &consumertest.LogsSink{}
	config := testConfig([]string{testExampleGroup}, nil)
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	collector := newTestCollector(t, config, sink, client, ft)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := collector.Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, collector.Shutdown(ctx)) }()

	// The first increment is a snapshot (cache starts empty).
	// Should emit 1 CRD + 2 CRs = 3 records.
	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 3
	}, 10*time.Second, 100*time.Millisecond, "snapshot should emit 1 CRD + 2 CRs")
}

// TestResourceCollector_TwoPhaseSaveOrdering verifies the asymmetric apply/emit ordering in
// the increment loop:
//   - adds/mods: ApplyDelta runs BEFORE emit, so peers see the new state first
//   - deletes:   ApplyDelta runs AFTER emit, so peers retain the resource until the platform
//     has been notified
//
// We assert this by recording each ApplyDelta invocation along with the cache contents at
// that moment, then checking the number of applies and what each apply contained per cycle.
func TestResourceCollector_TwoPhaseSaveOrdering(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, testExampleGroup, "v1", "TestResource")

	crd := makeTestCRDUnstructured("testresources.example.com", testExampleGroup, "TestResource", "testresources")
	crGVR := schema.GroupVersionResource{Group: testExampleGroup, Version: "v1", Resource: "testresources"}

	keepCR := makeTestCR("keep", "default", testExampleGroup, "v1", "TestResource")
	keepCR.SetResourceVersion("1")
	dropCR := makeTestCR("drop", "default", testExampleGroup, "v1", "TestResource")
	dropCR.SetResourceVersion("1")

	tests := []struct {
		name string
		// mutate is invoked after the initial snapshot save has been recorded.
		mutate func(t *testing.T, ctx context.Context, client *dynamicfake.FakeDynamicClient)
		// expectedSavesAfterMutation is the number of ApplyDelta calls expected in addition
		// to the one that happens after the initial snapshot.
		expectedSavesAfterMutation int
		// assertSaves runs against the recorded saves *after* the initial snapshot save.
		// Empty for the no-changes case.
		assertSaves func(t *testing.T, saves []recordedSave)
	}{
		{
			name:                       "no changes: increment cycle does not save",
			mutate:                     func(*testing.T, context.Context, *dynamicfake.FakeDynamicClient) {},
			expectedSavesAfterMutation: 0,
			assertSaves:                func(*testing.T, []recordedSave) {},
		},
		{
			name: "only adds: single save with the added CR present",
			mutate: func(t *testing.T, ctx context.Context, client *dynamicfake.FakeDynamicClient) {
				newCR := makeTestCR("new", "default", testExampleGroup, "v1", "TestResource")
				newCR.SetResourceVersion("1")
				_, err := client.Resource(crGVR).Namespace("default").Create(ctx, newCR, metav1.CreateOptions{})
				require.NoError(t, err)
			},
			expectedSavesAfterMutation: 1,
			assertSaves: func(t *testing.T, saves []recordedSave) {
				require.Len(t, saves, 1, "phase-1 should produce exactly one save")
				assert.Contains(t, saves[0].crs, objectResourceKey(crGVR, "default", "new"), "added CR should appear in save")
			},
		},
		{
			name: "only deletes: single save with the deleted CR removed",
			mutate: func(t *testing.T, ctx context.Context, client *dynamicfake.FakeDynamicClient) {
				err := client.Resource(crGVR).Namespace("default").Delete(ctx, "drop", metav1.DeleteOptions{})
				require.NoError(t, err)
			},
			expectedSavesAfterMutation: 1,
			assertSaves: func(t *testing.T, saves []recordedSave) {
				require.Len(t, saves, 1, "phase-2 should produce exactly one save")
				assert.NotContains(t, saves[0].crs, objectResourceKey(crGVR, "default", "drop"), "deleted CR must be absent in save")
				assert.Contains(t, saves[0].crs, objectResourceKey(crGVR, "default", "keep"), "kept CR should still be present")
			},
		},
		{
			name: "mixed: phase-1 save retains to-be-deleted CR; phase-2 save removes it",
			mutate: func(t *testing.T, ctx context.Context, client *dynamicfake.FakeDynamicClient) {
				newCR := makeTestCR("new", "default", testExampleGroup, "v1", "TestResource")
				newCR.SetResourceVersion("1")
				_, err := client.Resource(crGVR).Namespace("default").Create(ctx, newCR, metav1.CreateOptions{})
				require.NoError(t, err)
				require.NoError(t, client.Resource(crGVR).Namespace("default").Delete(ctx, "drop", metav1.DeleteOptions{}))
			},
			expectedSavesAfterMutation: 2,
			assertSaves: func(t *testing.T, saves []recordedSave) {
				require.Len(t, saves, 2, "mixed cycle should produce exactly two saves")
				dropKey := objectResourceKey(crGVR, "default", "drop")
				newKey := objectResourceKey(crGVR, "default", "new")

				// Phase 1 (adds first): added CR present; soon-to-be-deleted CR still present.
				assert.Contains(t, saves[0].crs, newKey, "phase-1 save should contain the added CR")
				assert.Contains(t, saves[0].crs, dropKey, "phase-1 save should still contain the to-be-deleted CR")

				// Phase 2 (deletes after): added CR still present; deleted CR removed.
				assert.Contains(t, saves[1].crs, newKey, "phase-2 save should still contain the added CR")
				assert.NotContains(t, saves[1].crs, dropKey, "phase-2 save must not contain the deleted CR")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
				map[schema.GroupVersionResource]string{
					crdGVR: testCRDListKind,
					crGVR:  testResourceListKind,
				},
				crd, keepCR.DeepCopy(), dropCR.DeepCopy(),
			)

			sink := &consumertest.LogsSink{}
			config := testConfig([]string{testExampleGroup}, nil)
			ft := tracker.NewForbiddenTracker(1 * time.Hour)
			peerStore := newRecordingPeerStore()
			collector := newTestCollectorWithPeerStore(t, config, sink, client, ft, peerStore)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			require.NoError(t, collector.Start(ctx))
			defer func() { require.NoError(t, collector.Shutdown(ctx)) }()

			// Wait for the initial snapshot save.
			assert.Eventually(t, func() bool {
				return len(peerStore.snapshot()) >= 1
			}, 10*time.Second, 50*time.Millisecond, "expected initial snapshot save")
			savesBeforeMutation := len(peerStore.snapshot())

			tt.mutate(t, ctx, client)

			if tt.expectedSavesAfterMutation == 0 {
				// Wait long enough for at least one increment cycle to complete and assert nothing saved.
				time.Sleep(2 * config.IncrementInterval)
				assert.Equal(t, savesBeforeMutation, len(peerStore.snapshot()),
					"increment cycle with no changes must not call ApplyDelta")
				return
			}

			expectedTotal := savesBeforeMutation + tt.expectedSavesAfterMutation
			assert.Eventually(t, func() bool {
				return len(peerStore.snapshot()) >= expectedTotal
			}, 10*time.Second, 50*time.Millisecond, "expected %d saves after mutation, got %d",
				tt.expectedSavesAfterMutation, len(peerStore.snapshot())-savesBeforeMutation)

			// Give one extra tick to confirm we don't get extra unexpected saves.
			time.Sleep(config.IncrementInterval + 200*time.Millisecond)
			all := peerStore.snapshot()
			assert.Equal(t, expectedTotal, len(all), "unexpected number of saves")

			tt.assertSaves(t, all[savesBeforeMutation:])
		})
	}
}

// TestResourceCollector_EmitObject_SourceDrivesEventName verifies that the
// collector picks the downstream event name from each object's ObjectSource:
// Source=cr keeps the CR-shape event name, Source=static uses the neutral
// object-shape event name. Covers both the snapshot path (currentObjects) and
// the increment path (ResourceChange).
func TestResourceCollector_EmitObject_SourceDrivesEventName(t *testing.T) {
	crGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	staticGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}

	crObj := makeTestCR("widget-1", "default", "example.com", "v1", "Widget")
	staticObj := makeTestCR("pod-1", "default", "", "v1", "Pod")

	cfg := &Config{ClusterName: "test-cluster"}

	t.Run("snapshot path", func(t *testing.T) {
		sink := &consumertest.LogsSink{}
		c := &resourceCollector{
			logger:   zaptest.NewLogger(t),
			config:   cfg,
			consumer: sink,
			metrics:  metrics.NoopRecorder{},
			enricher: noopResourceAttributeEnricher{},
		}
		c.emitSnapshot(
			context.Background(),
			nil, // no CRDs
			map[schema.GroupVersionResource]ObjectGroup{
				crGVR:     {Source: ObjectSourceCR, Objects: []*unstructured.Unstructured{crObj}},
				staticGVR: {Source: ObjectSourceStatic, Objects: []*unstructured.Unstructured{staticObj}},
			},
			nil, // no deletes
		)
		assertEventNamesByObjectName(t, sink, map[string]string{
			"widget-1": emit.EventNameCR,
			"pod-1":    emit.EventNameObject,
		})
	})

	t.Run("increment path", func(t *testing.T) {
		sink := &consumertest.LogsSink{}
		c := &resourceCollector{
			logger:   zaptest.NewLogger(t),
			config:   cfg,
			consumer: sink,
			metrics:  metrics.NoopRecorder{},
			enricher: noopResourceAttributeEnricher{},
		}
		c.emitChanges(context.Background(), []ResourceChange{
			{Obj: crObj, EventType: watch.Modified, GVR: crGVR, Source: ObjectSourceCR},
			{Obj: staticObj, EventType: watch.Added, GVR: staticGVR, Source: ObjectSourceStatic},
		})
		assertEventNamesByObjectName(t, sink, map[string]string{
			"widget-1": emit.EventNameCR,
			"pod-1":    emit.EventNameObject,
		})
	})
}

func TestResourceCollector_EmitObject_AddsEnrichedResourceAttributes(t *testing.T) {
	crGVR := schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachines"}
	crObj := makeTestCR("vm-1", "default", "kubevirt.io", "v1", "VirtualMachine")
	sink := &consumertest.LogsSink{}
	c := &resourceCollector{
		logger:   zaptest.NewLogger(t),
		config:   &Config{ClusterName: "test-cluster"},
		consumer: sink,
		metrics:  metrics.NoopRecorder{},
		enricher: stubResourceAttributeEnricher{
			gvr:   crGVR,
			attrs: map[string]string{"rancher.manager.url": "https://rancher.example.com"},
		},
	}

	c.emitChanges(context.Background(), []ResourceChange{
		{Obj: crObj, EventType: watch.Added, GVR: crGVR, Source: ObjectSourceCR},
	})

	require.Equal(t, 1, sink.LogRecordCount())
	attrs := sink.AllLogs()[0].ResourceLogs().At(0).Resource().Attributes()
	value, exists := attrs.Get("rancher.manager.url")
	require.True(t, exists)
	assert.Equal(t, "https://rancher.example.com", value.Str())
}

func TestResourceCollector_EmitsAllCRDsButOnlyFilteredCRs(t *testing.T) {
	scheme := testScheme()
	registerCRGVR(scheme, "allowed.com", "v1", "TestResource")
	registerCRGVR(scheme, "blocked.com", "v1", "TestResource")

	allowedCRD := makeTestCRDUnstructured("testresources.allowed.com", "allowed.com", "TestResource", "testresources")
	blockedCRD := makeTestCRDUnstructured("testresources.blocked.com", "blocked.com", "TestResource", "testresources")
	allowedCR := makeTestCR("allowed-1", "default", "allowed.com", "v1", "TestResource")
	blockedCR := makeTestCR("blocked-1", "default", "blocked.com", "v1", "TestResource")

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			crdGVR: testCRDListKind,
			{Group: "allowed.com", Version: "v1", Resource: "testresources"}: testResourceListKind,
			{Group: "blocked.com", Version: "v1", Resource: "testresources"}: testResourceListKind,
		},
		allowedCRD, blockedCRD, allowedCR, blockedCR,
	)

	sink := &consumertest.LogsSink{}
	collector := newTestCollector(t, testConfig([]string{"allowed.com"}, nil), sink, client, tracker.NewForbiddenTracker(1*time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, collector.Start(ctx))
	defer func() { require.NoError(t, collector.Shutdown(ctx)) }()

	waitForInitialEmissions(t, sink, 3)
	assertEventNamesByObjectName(t, sink, map[string]string{
		"testresources.allowed.com": emit.EventNameCRD,
		"testresources.blocked.com": emit.EventNameCRD,
		"allowed-1":                 emit.EventNameCR,
	})
	assertCRDWatchedByName(t, sink, map[string]bool{
		"testresources.allowed.com": true,
		"testresources.blocked.com": false,
	})
}

func TestResourceCollector_AppliesPayloadBudgets(t *testing.T) {
	crGVR := schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachines"}
	staticGVR := schema.GroupVersionResource{Group: "storage.sbomscanner.kubewarden.io", Version: "v1alpha1", Resource: "sboms"}
	crSmall := makeTestCR("a-small", "default", "kubevirt.io", "v1", "VirtualMachine")
	crSmall.Object["spec"] = map[string]interface{}{"payload": "small"}
	crLarge := makeTestCR("b-large", "default", "kubevirt.io", "v1", "VirtualMachine")
	crLarge.Object["spec"] = map[string]interface{}{"payload": "0123456789abcdefghijklmnopqrstuvwxyz"}
	staticLarge := makeTestCR("sbom-1", "default", "storage.sbomscanner.kubewarden.io", "v1alpha1", "SBOM")
	staticLarge.Object["spec"] = map[string]interface{}{"payload": "0123456789abcdefghijklmnopqrstuvwxyz"}
	recorder := &payloadRecorder{}
	c := &resourceCollector{
		logger:   zaptest.NewLogger(t),
		config:   &Config{ClusterName: "test-cluster", MaxCRTotalDataSizeBytes: 140, MaxObjectTotalDataSizeBytes: 20},
		consumer: &consumertest.LogsSink{},
		metrics:  recorder,
		enricher: noopResourceAttributeEnricher{},
	}

	filtered := c.applyPayloadBudgets(context.Background(), map[schema.GroupVersionResource]ObjectGroup{
		crGVR:     {Source: ObjectSourceCR, Objects: []*unstructured.Unstructured{crLarge, crSmall}},
		staticGVR: {Source: ObjectSourceStatic, Objects: []*unstructured.Unstructured{staticLarge}},
	})

	require.Contains(t, filtered, crGVR)
	require.Len(t, filtered[crGVR].Objects, 1)
	assert.Equal(t, "a-small", filtered[crGVR].Objects[0].GetName())
	assert.NotContains(t, filtered, staticGVR)
	assert.ElementsMatch(t, []payloadDropRecord{
		{source: metrics.PayloadSourceCR, apiGroup: "kubevirt.io", kind: "VirtualMachine"},
		{source: metrics.PayloadSourceObject, apiGroup: "storage.sbomscanner.kubewarden.io", kind: "SBOM"},
	}, recorder.Dropped())
	budgets := recorder.Budgets()
	require.Len(t, budgets, 2)
	assert.Contains(t, budgets, payloadBudgetRecord{source: metrics.PayloadSourceObject, budget: 20, used: 0})
	crBudget := findPayloadBudget(t, budgets, metrics.PayloadSourceCR)
	assert.Equal(t, int64(140), crBudget.budget)
	assert.Greater(t, crBudget.used, int64(0))
}

func findPayloadBudget(
	t *testing.T, budgets []payloadBudgetRecord, source metrics.PayloadSource,
) payloadBudgetRecord {
	t.Helper()
	for _, budget := range budgets {
		if budget.source == source {
			return budget
		}
	}
	require.Failf(t, "missing payload budget", "source %q not recorded", source)
	return payloadBudgetRecord{}
}

func TestResourceCollector_CRDWatchedMarkerHonorsWildcard(t *testing.T) {
	collector := &resourceCollector{config: testConfig([]string{"*"}, nil)}
	crd := makeTestCRDUnstructured("widgets.example.com", "example.com", "Widget", "widgets")

	assert.True(t, collector.crdCustomResourcesWatched(crd))
}

// assertEventNamesByObjectName walks every emitted log record and matches
// k8s.object.name → expected event name. Fails if any expected entry is missing
// or any record carries the wrong event name.
func assertEventNamesByObjectName(t *testing.T, sink *consumertest.LogsSink, want map[string]string) {
	t.Helper()
	got := map[string]string{}
	for _, ld := range sink.AllLogs() {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				sl := rl.ScopeLogs().At(j)
				for k := 0; k < sl.LogRecords().Len(); k++ {
					lr := sl.LogRecords().At(k)
					name, ok := lr.Attributes().Get(emit.AttrK8sObjectName)
					require.True(t, ok, "log record missing k8s.object.name")
					got[name.Str()] = lr.EventName()
				}
			}
		}
	}
	assert.Equal(t, want, got)
}

func assertCRDWatchedByName(t *testing.T, sink *consumertest.LogsSink, want map[string]bool) {
	t.Helper()
	got := map[string]bool{}
	for _, ld := range sink.AllLogs() {
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				sl := rl.ScopeLogs().At(j)
				for k := 0; k < sl.LogRecords().Len(); k++ {
					lr := sl.LogRecords().At(k)
					if lr.EventName() != emit.EventNameCRD {
						continue
					}
					name, ok := lr.Attributes().Get(emit.AttrK8sObjectName)
					require.True(t, ok, "CRD log record missing k8s.object.name")
					watched, ok := lr.Attributes().Get(emit.AttrK8sCRDCRsWatched)
					require.True(t, ok, "CRD log record missing watched marker")
					got[name.Str()] = watched.Bool()
				}
			}
		}
	}
	assert.Equal(t, want, got)
}

func TestConfig_Simplified(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := &Config{
			IncrementInterval: 10 * time.Second,
			SnapshotInterval:  5 * time.Minute,
			DiscoveryMode:     DiscoveryModeAPIGroups,
			CustomResourceAPIGroups: &APIGroups{
				Include: []string{testExampleGroup},
				Exclude: []string{},
			},
		}
		require.NoError(t, cfg.Validate())
		assert.Equal(t, 10*time.Second, cfg.IncrementInterval)
		assert.Equal(t, 5*time.Minute, cfg.SnapshotInterval)
	})

	t.Run("default intervals", func(t *testing.T) {
		cfg := &Config{
			DiscoveryMode: DiscoveryModeAPIGroups,
			CustomResourceAPIGroups: &APIGroups{
				Include: []string{"*"},
			},
		}
		require.NoError(t, cfg.Validate())
		assert.Equal(t, 10*time.Second, cfg.IncrementInterval)
		assert.Equal(t, 5*time.Minute, cfg.SnapshotInterval)
		assert.Equal(t, defaultMaxCRTotalDataSizeBytes, cfg.MaxCRTotalDataSizeBytes)
		assert.Equal(t, defaultMaxObjectTotalDataSizeBytes, cfg.MaxObjectTotalDataSizeBytes)
	})

	t.Run("negative max CR total data size", func(t *testing.T) {
		cfg := &Config{
			DiscoveryMode:           DiscoveryModeAll,
			MaxCRTotalDataSizeBytes: -1,
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max_cr_total_data_size_bytes must be non-negative")
	})

	t.Run("negative max object total data size", func(t *testing.T) {
		cfg := &Config{
			DiscoveryMode:               DiscoveryModeAll,
			MaxObjectTotalDataSizeBytes: -1,
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max_object_total_data_size_bytes must be non-negative")
	})

	t.Run("increment interval too short", func(t *testing.T) {
		cfg := &Config{
			IncrementInterval: 500 * time.Millisecond,
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least 1 second")
	})

	t.Run("snapshot interval too short", func(t *testing.T) {
		cfg := &Config{
			IncrementInterval: 5 * time.Second,
			SnapshotInterval:  30 * time.Second,
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least 1 minute")
	})

	t.Run("snapshot interval less than increment interval", func(t *testing.T) {
		cfg := &Config{
			IncrementInterval: 10 * time.Minute,
			SnapshotInterval:  5 * time.Minute,
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "greater than or equal to increment_interval")
	})

	t.Run("discovery mode all", func(t *testing.T) {
		cfg := &Config{
			IncrementInterval: 10 * time.Second,
			SnapshotInterval:  5 * time.Minute,
			DiscoveryMode:     DiscoveryModeAll,
		}
		require.NoError(t, cfg.Validate())
		assert.True(t, cfg.shouldWatchAPIGroup("anything.io"))
	})

	t.Run("default api group filters", func(t *testing.T) {
		cfg := &Config{
			DiscoveryMode: DiscoveryModeAPIGroups,
		}
		require.NoError(t, cfg.Validate())
		require.NotNil(t, cfg.CustomResourceAPIGroups)
		assert.Equal(t, []string{"*"}, cfg.CustomResourceAPIGroups.Include)
	})

	t.Run("empty include", func(t *testing.T) {
		cfg := &Config{
			DiscoveryMode: DiscoveryModeAPIGroups,
			CustomResourceAPIGroups: &APIGroups{
				Include: []string{},
			},
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be empty")
	})

	t.Run("invalid discovery mode", func(t *testing.T) {
		cfg := &Config{
			IncrementInterval: 10 * time.Second,
			SnapshotInterval:  5 * time.Minute,
			DiscoveryMode:     testInvalid,
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid discovery_mode")
	})

	t.Run("special chars in patterns are literal", func(t *testing.T) {
		cfg := &Config{
			DiscoveryMode: DiscoveryModeAPIGroups,
			CustomResourceAPIGroups: &APIGroups{
				Include: []string{"example.com"},
			},
		}
		require.NoError(t, cfg.Validate())

		// The dot in "example.com" is literal, not a regex wildcard
		assert.True(t, cfg.shouldWatchAPIGroup("example.com"))
		assert.False(t, cfg.shouldWatchAPIGroup("exampleXcom"))
	})

	t.Run("shouldWatchAPIGroup with wildcard and exclude", func(t *testing.T) {
		cfg := &Config{
			DiscoveryMode: DiscoveryModeAPIGroups,
			CustomResourceAPIGroups: &APIGroups{
				Include: []string{testSuseWildcard, "longhorn.io"},
				Exclude: []string{"internal.suse.com"},
			},
		}
		require.NoError(t, cfg.Validate())

		// Included by wildcard
		assert.True(t, cfg.shouldWatchAPIGroup("policies.suse.com"))
		assert.True(t, cfg.shouldWatchAPIGroup("fleet.suse.com"))

		// Included by exact match
		assert.True(t, cfg.shouldWatchAPIGroup("longhorn.io"))

		// Excluded explicitly
		assert.False(t, cfg.shouldWatchAPIGroup("internal.suse.com"))

		// Not matched by any include pattern
		assert.False(t, cfg.shouldWatchAPIGroup("other.example.com"))
	})

	t.Run("default discovery mode", func(t *testing.T) {
		cfg := &Config{
			IncrementInterval: 10 * time.Second,
			SnapshotInterval:  5 * time.Minute,
		}
		require.NoError(t, cfg.Validate())
		assert.Equal(t, DiscoveryModeAPIGroups, cfg.DiscoveryMode)
	})
}
