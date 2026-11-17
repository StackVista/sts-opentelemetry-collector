//nolint:testpackage
package k8sresourcereceiver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/resourcecollection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	clienttesting "k8s.io/client-go/testing"
)

type collectionHandoverRegistry struct {
	observations map[collectionWatchKey]collectionWatchSnapshot
	err          error
}

func newCollectionHandoverTestRegistry() *collectionHandoverRegistry {
	return &collectionHandoverRegistry{observations: map[collectionWatchKey]collectionWatchSnapshot{}}
}

func (r *collectionHandoverRegistry) reconcile(specs []collectionWatchSpec) error {
	if r.err != nil {
		return r.err
	}
	next := map[collectionWatchKey]collectionWatchSnapshot{}
	for _, spec := range specs {
		observation := r.observations[spec.Key]
		observation.Spec = copyCollectionWatchSpec(spec)
		next[spec.Key] = observation
	}
	r.observations = next
	return nil
}

func (r *collectionHandoverRegistry) snapshot() []collectionWatchSnapshot {
	result := make([]collectionWatchSnapshot, 0, len(r.observations))
	for _, observation := range r.observations {
		result = append(result, observation)
	}
	return result
}

func (r *collectionHandoverRegistry) observe(key collectionWatchKey, objects ...resourceObject) {
	observation := r.observations[key]
	observation.Synced, observation.State = true, resourcecollection.Active
	observation.Objects = objects
	r.observations[key] = observation
}

func (r *collectionHandoverRegistry) fail(key collectionWatchKey, reason string) {
	observation := r.observations[key]
	observation.State, observation.Reason = resourcecollection.Pending, reason
	// A failed relist may leave the informer cache stale or empty.
	observation.Objects = nil
	r.observations[key] = observation
}

func collectionHandoverPlan(t *testing.T, declarations ...*resourcecollection.ResourceCollection) collectionPlan {
	t.Helper()
	return buildCollectionPlan(collectionTestConfig(t, nil, nil, nil, nil), collectionTestDiscovery(), declarations)
}

func collectionHandoverRead(t *testing.T, handover *collectionHandover) collectionEffectiveView {
	t.Helper()
	view, err := handover.read()
	require.NoError(t, err)
	return view
}

func collectionHandoverObject(name, rv string) *unstructured.Unstructured {
	object := collectionTestWidget(name, "team-a")
	object.SetResourceVersion(rv)
	object.SetUID(types.UID("uid-" + name))
	return object
}

func TestCollectionHandoverSelectorReplacement(t *testing.T) {
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	declaration := collectionTestDeclaration("apps", collectionTestSelection("widgets", nil, map[string]string{"app": "old"}))
	old := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(old))
	registry.observe(old.Watches[0].Key, collectionHandoverObject("old", "opaque-old"))
	view := collectionHandoverRead(t, handover)
	require.True(t, view.Complete)
	cache := newResourceCache()
	cache.applyAdditions(cache.computeChanges(nil, view.Objects))

	declaration.Generation++
	declaration.Spec.Resources[0].LabelSelector.MatchLabels["app"] = "new"
	next := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(next))
	require.Len(t, registry.observations, 1)
	view = collectionHandoverRead(t, handover)
	assert.False(t, view.Complete)
	assert.Empty(t, view.Active)
	withdrawn := cache.computeChanges(nil, view.Objects)
	require.Len(t, withdrawn, 1)
	assert.Equal(t, watch.Deleted, withdrawn[0].EventType)
	assert.NotContains(t, registry.observations, old.Watches[0].Key)

	registry.observe(next.Watches[0].Key, collectionHandoverObject("new", "opaque-new"))
	view = collectionHandoverRead(t, handover)
	assert.True(t, view.Complete)
	assert.Equal(t, int64(2), view.Active[0].Generation)
	require.Len(t, registry.observations, 1)
	assert.Contains(t, registry.observations, next.Watches[0].Key)
	changes := cache.computeChanges(nil, view.Objects)
	require.Len(t, changes, 2)
	events := map[string]watch.EventType{}
	for _, change := range changes {
		events[change.Obj.GetName()] = change.EventType
	}
	assert.Equal(t, map[string]watch.EventType{"new": watch.Added, "old": watch.Deleted}, events)
}

func TestCollectionHandoverSupersedesPendingReplacement(t *testing.T) {
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	declaration := collectionTestDeclaration("apps", collectionTestSelection("widgets", nil, map[string]string{"app": "one"}))
	old := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(old))
	registry.observe(old.Watches[0].Key, collectionHandoverObject("one", "one"))
	collectionHandoverRead(t, handover)
	declaration.Generation = 2
	declaration.Spec.Resources[0].LabelSelector.MatchLabels["app"] = "two"
	pending := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(pending))
	declaration.Generation = 3
	declaration.Spec.Resources[0].LabelSelector.MatchLabels["app"] = "three"
	next := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(next))
	assert.NotContains(t, registry.observations, pending.Watches[0].Key)
	assert.NotContains(t, registry.observations, old.Watches[0].Key)
	assert.Empty(t, collectionHandoverRead(t, handover).Active)
	registry.observe(next.Watches[0].Key)
	view := collectionHandoverRead(t, handover)
	assert.True(t, view.Complete, "a successful empty observation is authoritative")
	assert.Empty(t, view.Objects[next.Watches[0].Key.GVR].Objects)
	assert.Equal(t, int64(3), view.Active[0].Generation)
	assert.Len(t, registry.observations, 1)
}

func TestCollectionHandoverNamespaceReplacementWaitsForEveryScope(t *testing.T) {
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	declaration := collectionTestDeclaration("apps", collectionTestSelection("widgets", nil, nil))
	old := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(old))
	registry.observe(old.Watches[0].Key, collectionHandoverObject("old", "old"))
	collectionHandoverRead(t, handover)
	declaration.Generation++
	declaration.Spec.Resources[0].Namespaces = []string{"team-a", "team-b"}
	next := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(next))
	registry.observe(next.Watches[0].Key)
	view := collectionHandoverRead(t, handover)
	assert.False(t, view.Complete)
	assert.Empty(t, view.Objects[old.Watches[0].Key.GVR].Objects)
	assert.Len(t, registry.observations, 2)
	registry.observe(next.Watches[1].Key)
	view = collectionHandoverRead(t, handover)
	assert.True(t, view.Complete)
	assert.Empty(t, view.Objects[old.Watches[0].Key.GVR].Objects)
	assert.Len(t, registry.observations, 2)
}

func TestCollectionHandoverIndependentProgressAndImmediateRemoval(t *testing.T) {
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	a := collectionTestDeclaration("a", collectionTestSelection("widgets", nil, nil))
	b := collectionTestDeclaration("b", collectionTestSelection("gadgets", nil, nil))
	old := collectionHandoverPlan(t, a, b)
	require.NoError(t, handover.update(old))
	for _, spec := range old.Watches {
		registry.observe(spec.Key, collectionHandoverObject(spec.Key.GVR.Resource, "old"))
	}
	cache := newResourceCache()
	cache.applyAdditions(cache.computeChanges(nil, collectionHandoverRead(t, handover).Objects))
	a.Generation++
	a.Spec.Resources[0].LabelSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "new"}}
	next := collectionHandoverPlan(t, a)
	require.NoError(t, handover.update(next))
	view := collectionHandoverRead(t, handover)
	assert.False(t, view.Complete)
	changes := cache.computeChanges(nil, view.Objects)
	require.Len(t, changes, 2)
	for _, change := range changes {
		assert.Equal(t, watch.Deleted, change.EventType)
	}
	assert.Len(t, registry.observations, 1, "changed and deleted selections withdraw their old contributions")
	require.NoError(t, handover.update(collectionHandoverPlan(t)))
	view = collectionHandoverRead(t, handover)
	assert.True(t, view.Complete)
	assert.Empty(t, view.Objects)
	assert.Empty(t, registry.observations)
}

func TestCollectionHandoverObservationFailureRetainsLastSuccessfulView(t *testing.T) {
	for _, reason := range []string{"AccessDenied", "APINotAvailable", "ObservationFailed"} {
		t.Run(reason, func(t *testing.T) {
			registry := newCollectionHandoverTestRegistry()
			handover := newCollectionHandover(registry)
			a := collectionTestDeclaration("a", collectionTestSelection("widgets", nil, nil))
			b := collectionTestDeclaration("b", collectionTestSelection("gadgets", nil, nil))
			plan := collectionHandoverPlan(t, a, b)
			require.NoError(t, handover.update(plan))
			for _, spec := range plan.Watches {
				registry.observe(spec.Key, collectionHandoverObject(spec.Key.GVR.Resource, "old"))
			}
			cache := newResourceCache()
			cache.applyAdditions(cache.computeChanges(nil, collectionHandoverRead(t, handover).Objects))
			registry.fail(plan.Watches[1].Key, reason)
			registry.observe(plan.Watches[0].Key, collectionHandoverObject("gadgets", "new"))
			view := collectionHandoverRead(t, handover)
			assert.False(t, view.Complete)
			changes := cache.computeChanges(nil, view.Objects)
			require.Len(t, changes, 1)
			assert.Equal(t, watch.Modified, changes[0].EventType, "healthy siblings continue; a failure is not deletion")
			registry.observe(plan.Watches[1].Key)
			view = collectionHandoverRead(t, handover)
			assert.True(t, view.Complete)
			assert.Empty(t, view.Objects[plan.Watches[1].Key.GVR].Objects)
		})
	}
}

func TestCollectionHandoverWildcardProgressAndPolicyDenial(t *testing.T) {
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	declaration := collectionTestDeclaration("types", collectionTestSelection("*", nil, nil))
	old := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(old))
	for _, spec := range old.Watches {
		registry.observe(spec.Key, collectionHandoverObject(spec.Key.GVR.Resource, "old"))
	}
	require.Len(t, collectionHandoverRead(t, handover).Active, 1, "wildcard children share one contributor")
	declaration.Generation++
	cfg := collectionTestConfig(t, nil, nil, nil, []ObjectMatcher{{Group: collectionTestGroup, Name: "widgets"}})
	next := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{declaration})
	require.NoError(t, handover.update(next))
	for key := range registry.observations {
		assert.NotEqual(t, "widgets", key.GVR.Resource, "denial immediately overrides old and new contributions")
	}
	registry.observe(next.Watches[0].Key, collectionHandoverObject("new-fleets", "new"))
	view := collectionHandoverRead(t, handover)
	assert.False(t, view.Complete)
	assert.Equal(t, "new-fleets", view.Objects[next.Watches[0].Key.GVR].Objects[0].GetName())
	gadgets := schema.GroupVersionResource{Group: collectionTestGroup, Version: "v1", Resource: "gadgets"}
	assert.Equal(t, "gadgets", view.Objects[gadgets].Objects[0].GetName(), "an unready child retains its old view")
	assert.NotContains(t, view.Objects, schema.GroupVersionResource{Group: collectionTestGroup, Version: "v1", Resource: "widgets"})
}

func TestCollectionHandoverPendingAPIAndInvalidDeclarationPreserveView(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing-api", true: "invalid-declaration"}[invalid], func(t *testing.T) {
			registry := newCollectionHandoverTestRegistry()
			handover := newCollectionHandover(registry)
			declaration := collectionTestDeclaration("types", collectionTestSelection("*", nil, nil))
			old := collectionHandoverPlan(t, declaration)
			require.NoError(t, handover.update(old))
			for _, spec := range old.Watches {
				registry.observe(spec.Key, collectionHandoverObject(spec.Key.GVR.Resource, "old"))
			}
			cache := newResourceCache()
			cache.applyAdditions(cache.computeChanges(nil, collectionHandoverRead(t, handover).Objects))
			declaration.Generation++
			discovery := collectionDiscoverySnapshot{}
			if invalid {
				declaration.Spec.Resources = nil
				discovery = collectionTestDiscovery()
			}
			plan := buildCollectionPlan(collectionTestConfig(t, nil, nil, nil, nil), discovery, []*resourcecollection.ResourceCollection{declaration})
			require.NoError(t, handover.update(plan))
			view := collectionHandoverRead(t, handover)
			assert.False(t, view.Complete)
			assert.Empty(t, cache.computeChanges(nil, view.Objects))
			assert.Equal(t, int64(1), view.Active[0].Generation)
			require.NoError(t, handover.update(collectionHandoverPlan(t)))
			assert.Empty(t, collectionHandoverRead(t, handover).Objects, "a deletion still withdraws an invalid declaration")
		})
	}
}

func TestCollectionHandoverPolicyAppliesToRetainedResourceTypes(t *testing.T) {
	for _, wildcard := range []bool{false, true} {
		t.Run(map[bool]string{false: "named-replacement", true: "wildcard-replacement"}[wildcard], func(t *testing.T) {
			registry := newCollectionHandoverTestRegistry()
			handover := newCollectionHandover(registry)
			declaration := collectionTestDeclaration("types", collectionTestSelection("widgets", nil, nil))
			old := collectionHandoverPlan(t, declaration)
			require.NoError(t, handover.update(old))
			registry.observe(old.Watches[0].Key, collectionHandoverObject("widgets", "old"))
			collectionHandoverRead(t, handover)
			declaration.Generation++
			declaration.Spec.Resources[0].Resource = "gadgets"
			if wildcard {
				declaration.Spec.Resources[0].Resource = "*"
			}
			cfg := collectionTestConfig(t, nil, nil, nil, []ObjectMatcher{{Group: collectionTestGroup, Name: "widgets"}})
			next := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{declaration})
			require.NoError(t, handover.update(next))
			view := collectionHandoverRead(t, handover)
			assert.False(t, view.Complete)
			assert.Empty(t, view.Objects, "an old resource cannot survive an operator veto during a type change")
			assert.NotContains(t, registry.observations, old.Watches[0].Key)
		})
	}
}

func TestCollectionHandoverResourceTypeReplacement(t *testing.T) {
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	declaration := collectionTestDeclaration("types", collectionTestSelection("widgets", nil, nil))
	old := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(old))
	registry.observe(old.Watches[0].Key, collectionHandoverObject("widgets", "old"))
	collectionHandoverRead(t, handover)
	declaration.Generation++
	declaration.Spec.Resources[0].Resource = "*"
	next := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(next))
	registry.observe(next.Watches[0].Key)
	view := collectionHandoverRead(t, handover)
	assert.False(t, view.Complete)
	widgets := old.Watches[0].Key.GVR
	assert.Len(t, view.Objects[widgets].Objects, 1)
	for _, contributor := range view.Active {
		assert.Equal(t, int64(2), contributor.Generation)
	}
	for _, spec := range next.Watches {
		registry.observe(spec.Key)
	}
	view = collectionHandoverRead(t, handover)
	assert.True(t, view.Complete)
	assert.Empty(t, view.Objects[widgets].Objects)
	assert.Len(t, view.Active, 1)
	assert.Equal(t, int64(2), view.Active[0].Generation)
}

func TestCollectionHandoverWithdrawalProvenanceLifecycle(t *testing.T) {
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	declaration := collectionTestDeclaration("apps", collectionTestSelection("widgets", nil, map[string]string{"app": "old"}))
	original := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(original))
	registry.observe(original.Watches[0].Key, collectionHandoverObject("old", "old"))
	assert.Empty(t, collectionHandoverRead(t, handover).Withdrawn)

	for generation, selector := range []string{"new", "other", "old", "last"} {
		declaration.Generation = int64(generation + 2)
		declaration.Spec.Resources[0].LabelSelector.MatchLabels["app"] = selector
		plan := collectionHandoverPlan(t, declaration)
		require.NoError(t, handover.update(plan))
		view := collectionHandoverRead(t, handover)
		require.Len(t, view.Withdrawn, 1)
		assert.Equal(t, int64(1), view.Withdrawn[0].Contributor.Generation)
		assert.Equal(t, "app=old", view.Withdrawn[0].Scope.LabelSelector)
		assert.Empty(t, view.Objects)
	}

	latest := collectionHandoverPlan(t, declaration)
	registry.observe(latest.Watches[0].Key)
	assert.Empty(t, collectionHandoverRead(t, handover).Withdrawn, "observing the replacement retires prior scope metadata")
	registry.fail(latest.Watches[0].Key, "AccessDenied")
	require.NoError(t, handover.update(latest))
	assert.Empty(t, collectionHandoverRead(t, handover).Withdrawn, "unchanged failures must retain the new contribution")

	declaration.Generation++
	declaration.Spec.Resources[0].LabelSelector.MatchLabels["app"] = "pending"
	require.NoError(t, handover.update(collectionHandoverPlan(t, declaration)))
	require.Len(t, collectionHandoverRead(t, handover).Withdrawn, 1)
	require.NoError(t, handover.update(collectionHandoverPlan(t)))
	assert.Empty(t, collectionHandoverRead(t, handover).Withdrawn, "removing a declaration retires prior scope metadata")
}

func TestCollectionHandoverStorageVersionFailureRetainsObjects(t *testing.T) {
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	declaration := collectionTestDeclaration("apps", collectionTestSelection("widgets", nil, map[string]string{"app": "old"}))
	original := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(original))
	object := collectionHandoverObject("old", "old")
	object.SetLabels(map[string]string{"app": "old"})
	registry.observe(original.Watches[0].Key, object)
	collectionHandoverRead(t, handover)

	catalog := collectionTestDiscovery()
	catalog.CRDs[0] = collectionTestCRD("widgets", catalog.CRDs[0].Spec.Scope, "v2", "v1")
	catalog.ServedResources = append(catalog.ServedResources, collectionTestAPI(collectionTestGroup, "v2", true, "widgets"))
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	next := buildCollectionPlan(cfg, catalog, []*resourcecollection.ResourceCollection{declaration})
	require.NoError(t, handover.update(next))
	for _, spec := range registry.snapshot() {
		registry.fail(spec.Spec.Key, "AccessDenied")
	}
	view := collectionHandoverRead(t, handover)
	require.Len(t, view.Objects[collectionTestWidgetsGVR()].Objects, 1)
	assert.Empty(t, view.Withdrawn)
	assert.Len(t, registry.snapshot(), 2)
	allowDelete := collectionDeletionPolicy(cfg, next, catalog, registry.snapshot(), view.Withdrawn, true, true, false)
	assert.False(t, allowDelete(ResourceChange{GVR: collectionTestWidgetsGVR(), Obj: object, Source: ObjectSourceCR}))
}

func TestCollectionHandoverOverlapNormalizesRepresentations(t *testing.T) {
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Group: collectionTestGroup, Name: "widgets"}}, nil)
	declaration := collectionTestDeclaration("scoped", collectionTestSelection("widgets", []string{"team-a"}, nil))
	second := collectionTestDeclaration("second", declaration.Spec.Resources[0])
	discovery := collectionTestDiscovery()
	discovery.ServedResources = append(discovery.ServedResources, collectionTestAPI(collectionTestGroup, "v2", true, "widgets"))
	discovery.CRDs[0] = collectionTestCRD("widgets", "Namespaced", "v1", "v2")
	cfg.Objects = append(cfg.Objects, ObjectWatch{Group: collectionTestGroup, Name: "widgets", Version: "v2"})
	plan := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{declaration, second})
	require.NoError(t, handover.update(plan))
	require.Len(t, plan.Watches, 3)
	for _, spec := range plan.Watches {
		registry.observe(spec.Key, collectionHandoverObject("same", "opaque-"+spec.Key.Namespace))
	}
	view := collectionHandoverRead(t, handover)
	require.Len(t, newResourceCache().computeChanges(nil, view.Objects), 2, "one object per API representation")
	v1 := schema.GroupVersionResource{Group: collectionTestGroup, Version: "v1", Resource: "widgets"}
	assert.Equal(t, "opaque-", view.Objects[v1].Objects[0].GetResourceVersion(), "a stable key order resolves overlapping healthy snapshots")
	failed := plan.Watches[0].Key
	registry.fail(failed, "AccessDenied")
	scoped := collectionHandoverObject("same", "opaque-recreated")
	scoped.SetUID("recreated")
	registry.observe(plan.Watches[1].Key, scoped)
	view = collectionHandoverRead(t, handover)
	require.Len(t, view.Objects[v1].Objects, 1)
	assert.Same(t, scoped, view.Objects[v1].Objects[0], "current observations take precedence over stale failure data")
	assert.Len(t, newResourceCache().computeChanges(nil, view.Objects), 2)
	require.NoError(t, handover.update(buildCollectionPlan(cfg, discovery, nil)))
	assert.Len(t, registry.observations, 2, "baseline survives removal of overlapping declarations")
}

func TestCollectionHandoverRegistryFailureDoesNotPublish(t *testing.T) {
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	declaration := collectionTestDeclaration("apps", collectionTestSelection("widgets", nil, nil))
	plan := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(plan))
	registry.observe(plan.Watches[0].Key, collectionHandoverObject("old", "old"))
	collectionHandoverRead(t, handover)
	registry.observe(plan.Watches[0].Key)
	registry.err = context.Canceled
	view, err := handover.read()
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, view.Objects)
	require.ErrorIs(t, handover.update(collectionHandoverPlan(t)), context.Canceled)
	registry.err = nil
	registry.fail(plan.Watches[0].Key, "ObservationFailed")
	view = collectionHandoverRead(t, handover)
	assert.Equal(t, "old", view.Objects[plan.Watches[0].Key.GVR].Objects[0].GetName())
}

func TestCollectionHandoverWithClientGoInformers(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, compact bool) {
		old := collectionTestWidget("old", "team-a")
		next := collectionTestWidget("new", "team-a")
		old.SetLabels(map[string]string{"app": "old"})
		next.SetLabels(map[string]string{"app": "new"})
		client := collectionTestWidgetClient()
		allowNew := make(chan struct{})
		var release sync.Once
		client.PrependReactor("list", "widgets", func(action clienttesting.Action) (bool, runtime.Object, error) {
			listAction, ok := action.(clienttesting.ListAction)
			require.True(t, ok)
			selector := listAction.GetListRestrictions().Labels.String()
			object := old
			if selector == "app=new" {
				object = next
			}
			return true, &unstructured.UnstructuredList{Items: []unstructured.Unstructured{*object}}, nil
		})
		intercepted := collectionListInterceptClient{Interface: client, beforeList: func(ctx context.Context, _ string, options metav1.ListOptions) error {
			if options.LabelSelector == "app=new" {
				select {
				case <-allowNew:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}}
		registry := collectionTestRegistry(t, intercepted, compact)
		t.Cleanup(func() { release.Do(func() { close(allowNew) }) })
		handover := newCollectionHandover(registry)
		declaration := collectionTestDeclaration("apps", resourcecollection.Selection{
			Group: collectionTestGroup, Resource: "widgets", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "old"}},
		})
		plan := collectionHandoverPlan(t, declaration)
		require.NoError(t, handover.update(plan))
		require.Eventually(t, func() bool {
			return collectionRegistryObservation(registry, plan.Watches[0].Key).State == resourcecollection.Active
		}, 5*time.Second, 10*time.Millisecond)
		view := collectionHandoverRead(t, handover)
		require.Len(t, view.Objects[collectionTestWidgetsGVR()].Objects, 1)
		assertRepresentation(t, compact, view.Objects[collectionTestWidgetsGVR()].Objects[0])
		declaration.Generation++
		declaration.Spec.Resources[0].LabelSelector.MatchLabels["app"] = "new"
		replacement := collectionHandoverPlan(t, declaration)
		require.NoError(t, handover.update(replacement))
		view = collectionHandoverRead(t, handover)
		assert.False(t, view.Complete)
		assert.Empty(t, view.Objects[collectionTestWidgetsGVR()].Objects)
		require.Len(t, registry.snapshot(), 1)
		release.Do(func() { close(allowNew) })
		require.Eventually(t, func() bool {
			return collectionRegistryObservation(registry, replacement.Watches[0].Key).State == resourcecollection.Active
		}, 5*time.Second, 10*time.Millisecond)
		view = collectionHandoverRead(t, handover)
		assert.True(t, view.Complete)
		assert.Equal(t, "new", view.Objects[collectionTestWidgetsGVR()].Objects[0].GetName())
		assert.Len(t, registry.snapshot(), 1)
	})
}

func TestCollectionHandoverFailedChangedScopeWithClientGoInformers(t *testing.T) {
	for _, rejectedField := range []bool{false, true} {
		name := "broader-label"
		if rejectedField {
			name = "rejected-neq-field"
		}
		t.Run(name, func(t *testing.T) {
			forEachCacheMode(t, func(t *testing.T, compact bool) {
				object := collectionTestWidget("old", "team-a")
				object.SetLabels(map[string]string{"app": "old"})
				client := collectionTestWidgetClient()
				allowReplacement := make(chan struct{})
				var release sync.Once
				client.PrependReactor("list", "widgets", func(clienttesting.Action) (bool, runtime.Object, error) {
					return true, &unstructured.UnstructuredList{Items: []unstructured.Unstructured{*object}}, nil
				})
				intercepted := collectionListInterceptClient{Interface: client, beforeList: func(ctx context.Context, _ string, options metav1.ListOptions) error {
					if options.FieldSelector != "" {
						return apierrors.NewBadRequest("field label not supported: spec.unsupported")
					}
					if options.LabelSelector != "app=old" {
						select {
						case <-allowReplacement:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				}}
				registry := collectionTestRegistry(t, intercepted, compact)
				t.Cleanup(func() { release.Do(func() { close(allowReplacement) }) })
				handover := newCollectionHandover(registry)
				declaration := collectionTestDeclaration("apps", collectionTestSelection("widgets", nil, map[string]string{"app": "old"}))
				original := collectionHandoverPlan(t, declaration)
				require.NoError(t, handover.update(original))
				require.Eventually(t, func() bool {
					return collectionRegistryObservation(registry, original.Watches[0].Key).State == resourcecollection.Active
				}, 5*time.Second, 10*time.Millisecond)
				peers := newResourceCache()
				peers.applyAdditions(peers.computeChanges(nil, collectionHandoverRead(t, handover).Objects))

				declaration.Generation++
				if rejectedField {
					declaration.Spec.Resources[0].FieldSelector = "spec.unsupported!=x"
				} else {
					declaration.Spec.Resources[0].LabelSelector = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
						Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{"old", "new"},
					}}}
				}
				next := collectionHandoverPlan(t, declaration)
				require.NoError(t, handover.update(next))
				if rejectedField {
					require.Eventually(t, func() bool {
						return collectionRegistryObservation(registry, next.Watches[0].Key).Reason == "InvalidSelection"
					}, 5*time.Second, 10*time.Millisecond)
				}
				observations := registry.snapshot()
				require.Len(t, observations, 1)
				assert.Equal(t, next.Watches[0].Key, observations[0].Spec.Key)
				assert.Empty(t, observations[0].Objects, "the withdrawn watch's cache must not survive replacement")
				view := collectionHandoverRead(t, handover)
				assert.Empty(t, view.Objects)
				require.Len(t, view.Withdrawn, 1)
				cfg := collectionTestConfig(t, nil, nil, nil, nil)
				allowDelete := collectionDeletionPolicy(cfg, next, collectionTestDiscovery(), observations, view.Withdrawn, true, true, false)
				_, reported, _ := preserveCollectionPeerView(nil, view.Objects, peers.computeChanges(nil, view.Objects), allowDelete)
				assert.Empty(t, reported)
				if !rejectedField {
					release.Do(func() { close(allowReplacement) })
					require.Eventually(t, func() bool {
						return collectionRegistryObservation(registry, next.Watches[0].Key).State == resourcecollection.Active
					}, 5*time.Second, 10*time.Millisecond)
					view = collectionHandoverRead(t, handover)
					require.Len(t, view.Objects[collectionTestWidgetsGVR()].Objects, 1)
					assert.Empty(t, view.Withdrawn)
					assert.Equal(t, int64(2), view.Active[0].Generation)
				}
			})
		})
	}
}
