//nolint:testpackage
package k8sresourcereceiver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/k8sleaderelector"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/emit"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/tracker"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/resourcecollection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
)

type collectionRuntimeDiscovery struct {
	discovery.DiscoveryInterface
	mu        sync.Mutex
	resources []*metav1.APIResourceList
	err       error
	calls     atomic.Int64
}

func (d *collectionRuntimeDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	d.calls.Add(1)
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make([]*metav1.APIResourceList, 0, len(d.resources))
	for _, resources := range d.resources {
		result = append(result, resources.DeepCopy())
	}
	return result, d.err
}

func (d *collectionRuntimeDiscovery) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	resources, err := d.ServerPreferredResources()
	byGroup := map[string]*metav1.APIGroup{}
	var groups []*metav1.APIGroup
	for _, list := range resources {
		gv, parseErr := schema.ParseGroupVersion(list.GroupVersion)
		if parseErr != nil {
			continue
		}
		group := byGroup[gv.Group]
		version := metav1.GroupVersionForDiscovery{GroupVersion: list.GroupVersion, Version: gv.Version}
		if group == nil {
			group = &metav1.APIGroup{Name: gv.Group, PreferredVersion: version}
			byGroup[gv.Group] = group
			groups = append(groups, group)
		}
		group.Versions = append(group.Versions, version)
	}
	return groups, resources, err
}

func (d *collectionRuntimeDiscovery) set(resources []*metav1.APIResourceList, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resources, d.err = resources, err
}

func collectionRuntimeClient(t *testing.T, objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	t.Helper()
	scheme := testScheme()
	registerCRGVR(scheme, resourcecollection.Group, resourcecollection.Version, resourcecollection.Kind)
	registerCRGVR(scheme, collectionTestGroup, "v1", "Widget")
	registerCRGVR(scheme, collectionTestGroup, "v2", "Widget")
	registerCRGVR(scheme, collectionTestGroup, "v1", "Gadget")
	registerCRGVR(scheme, "", "v1", "Pod")
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		crdGVR:                     "CustomResourceDefinitionList",
		resourcecollection.GVR():   resourcecollection.Kind + "List",
		collectionTestWidgetsGVR(): "WidgetList",
		{Group: collectionTestGroup, Version: "v2", Resource: "widgets"}: "WidgetList",
		{Group: collectionTestGroup, Version: "v1", Resource: "gadgets"}: "GadgetList",
		podsGVR: "PodList",
	}, objects...)
}

func collectionRuntimeCRD() *unstructured.Unstructured {
	return makeTestCRDUnstructured("widgets."+collectionTestGroup, collectionTestGroup, "Widget", "widgets")
}

func startCollectionTestRuntime(
	t *testing.T, cfg *Config, client dynamic.Interface, disc discovery.DiscoveryInterface, watchClocks ...clock.Clock,
) (*collectionRuntime, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := newCollectionRuntime(testSettings(t), cfg, client, disc, nil)
	if len(watchClocks) > 0 {
		r.watchClock = watchClocks[0]
	}
	require.NoError(t, r.Start(ctx))
	t.Cleanup(func() {
		cancel()
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		require.NoError(t, r.Shutdown(shutdown))
	})
	return r, ctx
}

func collectionRuntimeStatus(t *testing.T, client dynamic.Interface, name string, state string) *resourcecollection.ResourceCollection {
	t.Helper()
	var decoded *resourcecollection.ResourceCollection
	require.Eventually(t, func() bool {
		object, err := client.Resource(resourcecollection.GVR()).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		decoded, err = resourcecollection.Decode(object)
		return err == nil && len(decoded.Status.Resources) == 1 && decoded.Status.Resources[0].State == state &&
			decoded.Status.ObservedGeneration == decoded.Generation
	}, 15*time.Second, 20*time.Millisecond)
	return decoded
}

func TestCollectionRuntimeDeclarationChangesPreserveBaselineAndShareWatches(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, compact bool) {
		cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{
			{Name: "pods"}, {Name: "widgets", Group: collectionTestGroup, Namespaces: []string{"team-a"}},
		}, nil)
		cfg.CompactCache = compact
		a := collectionTestDeclaration("a", collectionTestSelection("widgets", []string{"team-a"}, nil))
		b := collectionTestDeclaration("b", collectionTestSelection("widgets", []string{"team-a"}, nil))
		client := collectionRuntimeClient(t, collectionRuntimeCRD(),
			collectionTestDeclarationObject(t, a), collectionTestDeclarationObject(t, b),
			collectionTestWidget("a-widget", "team-a"), collectionTestWidget("b-widget", "team-b"),
			makeTestPod("baseline-pod", "base", nil))
		disc := &collectionRuntimeDiscovery{resources: []*metav1.APIResourceList{
			collectionTestAPI("", "v1", true, "pods"),
			collectionTestAPI(collectionTestGroup, "v1", true, "widgets"),
		}}
		r, ctx := startCollectionTestRuntime(t, cfg, client, disc)
		collectionRuntimeStatus(t, client, "a", resourcecollection.Active)
		collectionRuntimeStatus(t, client, "b", resourcecollection.Active)
		require.Eventually(t, func() bool {
			view := r.collectionSnapshot()
			return view.Complete && len(view.Objects[podsGVR].Objects) == 1 &&
				len(view.Objects[collectionTestWidgetsGVR()].Objects) == 1
		}, 5*time.Second, 20*time.Millisecond)
		view := r.collectionSnapshot()
		assert.Equal(t, ObjectSourceCR, view.Objects[collectionTestWidgetsGVR()].Source)
		assertRepresentation(t, compact, view.Objects[collectionTestWidgetsGVR()].Objects[0])
		watches := 0
		for _, action := range client.Actions() {
			if action.GetVerb() == "watch" && action.GetResource() == collectionTestWidgetsGVR() {
				watches++
				assert.Equal(t, "team-a", action.GetNamespace())
			}
		}
		assert.Equal(t, 1, watches, "identical YAML/declaration scopes share one informer")

		a.Generation = 2
		a.Spec.Resources[0].Namespaces = []string{"team-b"}
		_, err := client.Resource(resourcecollection.GVR()).Update(ctx, collectionTestDeclarationObject(t, a), metav1.UpdateOptions{})
		require.NoError(t, err)
		collectionRuntimeStatus(t, client, "a", resourcecollection.Active)
		require.Eventually(t, func() bool {
			return len(r.collectionSnapshot().Objects[collectionTestWidgetsGVR()].Objects) == 2
		}, 5*time.Second, 20*time.Millisecond)

		require.NoError(t, client.Resource(resourcecollection.GVR()).Delete(ctx, "a", metav1.DeleteOptions{}))
		require.NoError(t, client.Resource(resourcecollection.GVR()).Delete(ctx, "b", metav1.DeleteOptions{}))
		require.Eventually(t, func() bool {
			view := r.collectionSnapshot()
			return len(view.Objects[collectionTestWidgetsGVR()].Objects) == 1 &&
				len(view.Objects[resourcecollection.GVR()].Objects) == 0
		}, 5*time.Second, 20*time.Millisecond)
		assert.Equal(t, "a-widget", r.collectionSnapshot().Objects[collectionTestWidgetsGVR()].Objects[0].GetName())
		assert.Len(t, r.collectionSnapshot().Objects[podsGVR].Objects, 1)
	})
}

func TestCollectionRuntimeMissingAPIRecoversAndReportsDeclarationStatus(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "pods"}}, nil)
	declaration := collectionTestDeclaration("integration", collectionTestSelection("widgets", nil, nil))
	client := collectionRuntimeClient(t, collectionTestDeclarationObject(t, declaration),
		makeTestPod("baseline-pod", "base", nil))
	disc := &collectionRuntimeDiscovery{resources: []*metav1.APIResourceList{collectionTestAPI("", "v1", true, "pods")}}
	r, ctx := startCollectionTestRuntime(t, cfg, client, disc)
	collectionRuntimeStatus(t, client, declaration.Name, resourcecollection.Pending)
	require.Eventually(t, func() bool {
		return r.collectionSnapshot().Complete && len(r.collectionSnapshot().Objects[podsGVR].Objects) == 1
	}, 5*time.Second, 20*time.Millisecond)
	disc.set([]*metav1.APIResourceList{
		collectionTestAPI("", "v1", true, "pods"),
		collectionTestAPI(collectionTestGroup, "v1", true, "widgets"),
	}, nil)
	_, err := client.Resource(collectionTestWidgetsGVR()).Namespace("payments").Create(ctx,
		collectionTestWidget("installed-later", "payments"), metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = client.Resource(crdGVR).Create(ctx, collectionRuntimeCRD(), metav1.CreateOptions{})
	require.NoError(t, err)
	collectionRuntimeStatus(t, client, declaration.Name, resourcecollection.Active)
	require.Eventually(t, func() bool {
		view := r.collectionSnapshot()
		if len(view.Objects[resourcecollection.GVR()].Objects) != 1 || len(view.Objects[collectionTestWidgetsGVR()].Objects) != 1 {
			return false
		}
		object, decodeErr := decodeObject(view.Objects[resourcecollection.GVR()].Objects[0])
		if decodeErr != nil {
			return false
		}
		reported, decodeErr := resourcecollection.Decode(object)
		return decodeErr == nil && reported.Status.ObservedGeneration == 1 &&
			reported.Status.Resources[0].State == resourcecollection.Active
	}, 5*time.Second, 20*time.Millisecond, "automatic declaration collection must preserve feedback")
	assert.True(t, r.collectionSnapshot().Watched[collectionTestWidgetsGVR().GroupResource()])
}

func TestCollectionRuntimeRetainsCatalogOnDiscoveryFailuresAndRemovesDeletedCRDs(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, compact bool) {
		cfg := collectionTestConfig(t, nil, nil, nil, nil)
		cfg.CompactCache = compact
		declaration := collectionTestDeclaration("integration", collectionTestSelection("widgets", nil, nil))
		client := collectionRuntimeClient(t, collectionRuntimeCRD(),
			collectionTestDeclarationObject(t, declaration), collectionTestWidget("retained", "payments"))
		resources := []*metav1.APIResourceList{collectionTestAPI(collectionTestGroup, "v1", true, "widgets")}
		disc := &collectionRuntimeDiscovery{resources: resources}
		r, ctx := startCollectionTestRuntime(t, cfg, client, disc)
		collectionRuntimeStatus(t, client, declaration.Name, resourcecollection.Active)
		disc.set(nil, errors.New("discovery temporarily unavailable"))
		r.dirty.Store(true)
		r.enqueue()
		require.Eventually(t, func() bool {
			return !r.collectionSnapshot().AllowDelete(ResourceChange{
				GVR:    schema.GroupVersionResource{Group: "unknown.example.com", Version: "v1", Resource: "others"},
				Source: ObjectSourceStatic,
			})
		}, 5*time.Second, 20*time.Millisecond)
		assert.Len(t, r.collectionSnapshot().Objects[collectionTestWidgetsGVR()].Objects, 1)

		disc.set(nil, &discovery.ErrGroupDiscoveryFailed{Groups: map[schema.GroupVersion]error{
			{Group: collectionTestGroup, Version: "v1"}: errors.New("API group temporarily unavailable"),
		}})
		r.dirty.Store(true)
		r.enqueue()
		require.NoError(t, client.Resource(crdGVR).Delete(ctx, "widgets."+collectionTestGroup, metav1.DeleteOptions{}))
		require.Eventually(t, func() bool {
			view := r.collectionSnapshot()
			return len(view.Objects[collectionTestWidgetsGVR()].Objects) == 0 &&
				view.AllowDelete(ResourceChange{GVR: collectionTestWidgetsGVR(), Source: ObjectSourceCR})
		}, 5*time.Second, 20*time.Millisecond, "authoritative CRD removal differs from transient discovery loss")
	})
}

func TestCollectionRuntimeRemovedCRDCannotReuseStaticCachedObjects(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{
		{Name: "pods"}, {Name: "widgets", Group: collectionTestGroup},
	}, nil)
	client := collectionRuntimeClient(t, collectionRuntimeCRD(),
		collectionTestWidget("old-widget", "payments"), makeTestPod("baseline-pod", "base", nil))
	var removed atomic.Bool
	client.PrependReactor("list", "widgets", func(clienttesting.Action) (bool, runtime.Object, error) {
		if removed.Load() {
			return true, nil, apierrors.NewNotFound(collectionTestWidgetsGVR().GroupResource(), "")
		}
		return false, nil, nil
	})
	disc := &collectionRuntimeDiscovery{resources: []*metav1.APIResourceList{
		collectionTestAPI("", "v1", true, "pods"), collectionTestAPI(collectionTestGroup, "v1", true, "widgets"),
	}}
	r, ctx := startCollectionTestRuntime(t, cfg, client, disc)
	require.Eventually(t, func() bool {
		return len(r.collectionSnapshot().Objects[collectionTestWidgetsGVR()].Objects) == 1
	}, 5*time.Second, 20*time.Millisecond)
	removed.Store(true)
	disc.set(nil, errors.New("discovery temporarily unavailable"))
	require.NoError(t, client.Resource(crdGVR).Delete(ctx, "widgets."+collectionTestGroup, metav1.DeleteOptions{}))
	require.Eventually(t, func() bool {
		view := r.collectionSnapshot()
		return len(view.CRDs) == 0 && len(view.Objects[collectionTestWidgetsGVR()].Objects) == 0 &&
			len(view.Objects[podsGVR].Objects) == 1
	}, 5*time.Second, 20*time.Millisecond)
	lists := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "list" && action.GetResource() == collectionTestWidgetsGVR() {
			lists++
		}
	}
	assert.GreaterOrEqual(t, lists, 2, "a static request must obtain a fresh observation after CRD removal")
}

func TestCollectionRuntimeWildcardDeniedAndForbiddenChildrenKeepHealthyTypes(t *testing.T) {
	for _, policy := range []string{"deny", "rbac"} {
		t.Run(policy, func(t *testing.T) {
			cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "pods"}}, nil)
			if policy == "deny" {
				cfg.DeniedObjects = []ObjectMatcher{{Name: "widgets", Group: collectionTestGroup}}
			}
			declaration := collectionTestDeclaration("integration", collectionTestSelection("*", nil, nil))
			gadgetsGVR := schema.GroupVersionResource{Group: collectionTestGroup, Version: "v1", Resource: "gadgets"}
			client := collectionRuntimeClient(t, collectionRuntimeCRD(),
				makeTestCRDUnstructured("gadgets."+collectionTestGroup, collectionTestGroup, "Gadget", "gadgets"),
				collectionTestDeclarationObject(t, declaration), collectionTestWidget("widget", "payments"),
				makeTestCR("gadget", "payments", collectionTestGroup, "v1", "Gadget"), makeTestPod("baseline-pod", "base", nil))
			var granted atomic.Bool
			if policy == "rbac" {
				client.PrependReactor("list", "widgets", func(clienttesting.Action) (bool, runtime.Object, error) {
					if !granted.Load() {
						return true, nil, apierrors.NewForbidden(collectionTestWidgetsGVR().GroupResource(), "", errors.New("reads not granted"))
					}
					return false, nil, nil
				})
			}
			disc := &collectionRuntimeDiscovery{resources: []*metav1.APIResourceList{
				collectionTestAPI("", "v1", true, "pods"),
				collectionTestAPI(collectionTestGroup, "v1", true, "widgets", "gadgets"),
			}}
			watchClock := clocktesting.NewFakeClock(time.Now())
			r, _ := startCollectionTestRuntime(t, cfg, client, disc, watchClock)
			feedback := collectionRuntimeStatus(t, client, declaration.Name, resourcecollection.Blocked)
			require.Eventually(t, func() bool {
				object, err := client.Resource(resourcecollection.GVR()).Get(context.Background(), declaration.Name, metav1.GetOptions{})
				if err != nil {
					return false
				}
				feedback, err = resourcecollection.Decode(object)
				return err == nil && len(feedback.Status.Resources) == 1 &&
					strings.Contains(feedback.Status.Resources[0].Message, "1 types active, 1 blocked, 0 pending.")
			}, 5*time.Second, 20*time.Millisecond, "Blocked can precede the healthy wildcard child's synchronization")
			assert.Contains(t, feedback.Status.Resources[0].Message, "1 types active, 1 blocked, 0 pending.")
			require.Eventually(t, func() bool {
				return r.collectionSnapshot().Complete && len(r.collectionSnapshot().Objects[gadgetsGVR].Objects) == 1 &&
					len(r.collectionSnapshot().Objects[podsGVR].Objects) == 1
			}, 5*time.Second, 20*time.Millisecond)
			if policy == "deny" {
				for _, action := range client.Actions() {
					assert.NotEqual(t, collectionTestWidgetsGVR(), action.GetResource())
				}
			} else {
				granted.Store(true)
				require.Eventually(t, watchClock.HasWaiters, 5*time.Second, 10*time.Millisecond)
				watchClock.Step(tracker.DefaultRetryInterval)
				collectionRuntimeStatus(t, client, declaration.Name, resourcecollection.Active)
				assert.Len(t, r.collectionSnapshot().Objects[collectionTestWidgetsGVR()].Objects, 1)
			}
		})
	}
}

func TestCollectionRuntimeTakeoverPreservesPeerObjectsUntilObservation(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, compact bool) {
		cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "pods", Namespaces: []string{"base"}}}, nil)
		cfg.CompactCache, cfg.EmitSnapshotBoundaries = compact, true
		cfg.SnapshotInterval = time.Hour
		declaration := collectionTestDeclaration("integration", collectionTestSelection("widgets", []string{"held"}, nil))
		client := collectionRuntimeClient(t, collectionRuntimeCRD(),
			collectionTestDeclarationObject(t, declaration), makeTestPod("baseline-pod", "base", nil))
		release := make(chan struct{})
		defer close(release)
		var blocked atomic.Bool
		wrapped := collectionListInterceptClient{Interface: client, beforeList: func(ctx context.Context, namespace string, _ metav1.ListOptions) error {
			if namespace == "held" && !blocked.Swap(true) {
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}}
		disc := &collectionRuntimeDiscovery{resources: []*metav1.APIResourceList{
			collectionTestAPI("", "v1", true, "pods"), collectionTestAPI(collectionTestGroup, "v1", true, "widgets"),
		}}
		r, ctx := startCollectionTestRuntime(t, cfg, wrapped, disc)
		require.Eventually(t, func() bool {
			return blocked.Load() && r.collectionSnapshot().Complete && len(r.collectionSnapshot().Objects[podsGVR].Objects) == 1
		}, 5*time.Second, 20*time.Millisecond)
		peer := newRecordingPeerStore()
		require.NoError(t, peer.ApplyDelta(ctx, &PeerSyncDelta{Changes: []ResourceChange{{
			GVR: collectionTestWidgetsGVR(), Source: ObjectSourceCR, EventType: watch.Added,
			Obj: cachedAs(t, compact, collectionTestWidget("previously-observed", "held")),
		}}}))
		sink := &consumertest.LogsSink{}
		collector := newResourceCollector(testSettings(t).Logger, cfg, sink, r, peer, nil, nil)
		collector.runIncrement(ctx)
		found := false
		complete := false
		for _, record := range emittedRecords(sink, 0) {
			if record.Attrs[emit.AttrK8sObjectName] == "previously-observed" {
				found = true
				assert.Equal(t, string(watch.Added), record.Body["type"])
			}
			if record.Attrs[emit.AttrK8sSnapshotBoundary] == emit.SnapshotBoundaryEnd {
				complete, _ = record.Attrs[emit.AttrK8sSnapshotComplete].(bool)
			}
		}
		assert.True(t, found)
		assert.True(t, complete, "pending custom declarations must not block the healthy native baseline")
		assert.Len(t, peer.ComputeChanges(nil, nil), 4, "the prior object remains beside the pod, declaration and CRD")
		require.NoError(t, client.Resource(resourcecollection.GVR()).Delete(ctx, declaration.Name, metav1.DeleteOptions{}))
		require.Eventually(t, func() bool {
			return r.collectionSnapshot().AllowDelete(ResourceChange{GVR: collectionTestWidgetsGVR(), Source: ObjectSourceCR})
		}, 5*time.Second, 20*time.Millisecond)
		sink.Reset()
		collector.runIncrement(ctx)
		found = false
		for _, record := range emittedRecords(sink, 0) {
			if record.Attrs[emit.AttrK8sObjectName] == "previously-observed" {
				found = true
				assert.Equal(t, string(watch.Deleted), record.Body["type"])
			}
		}
		assert.True(t, found, "an intentional declaration removal must still withdraw the old contribution")
	})
}

func TestCollectionRuntimeIgnoresStatusOnlyControlEvents(t *testing.T) {
	r := &collectionRuntime{trigger: make(chan struct{}, 1)}
	before := collectionTestDeclarationObject(t, collectionTestDeclaration("integration", collectionTestSelection("widgets", nil, nil)))
	after := before.DeepCopy()
	after.Object["status"] = map[string]interface{}{"observedGeneration": int64(1)}
	after.SetResourceVersion("11")
	r.controlChanged(before, after)
	assert.False(t, r.dirty.Load())
	assert.False(t, r.controlDirty.Load(), "status-only updates must not rebuild the plan")
	assert.Empty(t, r.trigger)
	after.SetGeneration(2)
	r.controlChanged(before, after)
	assert.False(t, r.dirty.Load(), "declaration changes resolve against the cached API catalog")
	assert.True(t, r.controlDirty.Load())
	assert.Len(t, r.trigger, 1)

	crd := collectionRuntimeCRD()
	crd.SetGeneration(1)
	for _, change := range []struct {
		name      string
		update    func(*unstructured.Unstructured)
		discovery bool
	}{
		{"stored versions", func(object *unstructured.Unstructured) {
			require.NoError(t, unstructured.SetNestedSlice(object.Object, []interface{}{"v1"}, "status", "storedVersions"))
		}, false},
		{"condition message", func(object *unstructured.Unstructured) {
			object.Object["status"] = map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Established", "status": "True", "message": "updated"},
			}}
		}, false},
		{"established", func(object *unstructured.Unstructured) {
			object.Object["status"] = map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Established", "status": "False"},
			}}
		}, true},
		{"names accepted", func(object *unstructured.Unstructured) {
			object.Object["status"] = map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Established", "status": "True"},
				map[string]interface{}{"type": "NamesAccepted", "status": "True"},
			}}
		}, true},
		{"generation", func(object *unstructured.Unstructured) { object.SetGeneration(2) }, true},
		{"replacement", func(object *unstructured.Unstructured) { object.SetUID("replacement") }, true},
		{"deletion", func(object *unstructured.Unstructured) {
			now := metav1.Now()
			object.SetDeletionTimestamp(&now)
		}, true},
	} {
		t.Run(change.name, func(t *testing.T) {
			before := crd.DeepCopy()
			before.Object["status"] = map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Established", "status": "True"},
			}}
			after := before.DeepCopy()
			change.update(after)
			r := &collectionRuntime{trigger: make(chan struct{}, 1)}
			r.controlChanged(before, after)
			assert.Equal(t, change.discovery, r.dirty.Load())
			assert.Equal(t, change.discovery, r.controlDirty.Load())
			if change.discovery {
				assert.Len(t, r.trigger, 1)
			} else {
				assert.Empty(t, r.trigger)
			}
		})
	}
}

type collectionReportingFixture struct {
	fixedInformers
	view collectionReportingSnapshot
}

func (f *collectionReportingFixture) collectionSnapshot() collectionReportingSnapshot { return f.view }
func (*collectionReportingFixture) StaticWatchesSynced() bool {
	panic("combined reporting must not call legacy accessors")
}
func (*collectionReportingFixture) ReadCRDs() []*unstructured.Unstructured {
	panic("combined reporting must not call legacy accessors")
}
func (*collectionReportingFixture) ReadObjects() map[schema.GroupVersionResource]ObjectGroup {
	panic("combined reporting must not call legacy accessors")
}

func TestCollectionRuntimeInvalidSpecPreservesCachedStatus(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	declaration := collectionTestDeclaration("invalid", collectionTestSelection("widgets", nil, nil))
	result := collectionDeclarationResult{
		Name: declaration.Name, UID: declaration.UID, Generation: declaration.Generation, Reason: "InvalidDeclaration",
	}
	declaration.Status = collectionDeclarationStatus(result, nil)
	preserveCollectionTransition(&declaration.Status, resourcecollection.Status{}, time.Now())
	object := collectionTestDeclarationObject(t, declaration)
	object.Object["spec"] = map[string]interface{}{"resources": "malformed"}
	_, err := resourcecollection.Decode(object)
	require.Error(t, err)
	client := collectionRuntimeClient(t, object)
	r := newCollectionRuntime(testSettings(t), cfg, client, nil, nil)
	r.ctx = context.Background()
	r.controls = newCollectionWatchRegistry(r.ctx, client, nil)
	t.Cleanup(r.controls.cancel)
	store := cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc)
	require.NoError(t, store.Add(object))
	key := collectionWatchKey{GVR: resourcecollection.GVR()}
	r.controls.entries[key] = &collectionRegistryEntry{
		spec: collectionWatchSpec{Key: key}, informer: &collectionSyncedInformer{store: store},
	}
	for range 3 {
		r.readControls([]collectionWatchSnapshot{{
			Spec:  collectionWatchSpec{Key: collectionWatchKey{GVR: resourcecollection.GVR()}},
			State: resourcecollection.Active, Synced: true, Objects: []resourceObject{object},
		}})
		plan := buildCollectionPlan(cfg, collectionTestDiscovery(), r.declarations)
		r.publishStatuses(plan, nil)
	}
	assert.Empty(t, client.Actions(), "current invalid-declaration feedback needs neither GET nor PATCH")
}

func TestCollectionReportingRetainedPeerObjectsStillHonorPayloadBudgets(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, compact bool) {
		ctx := context.Background()
		cfg := collectionTestConfig(t, nil, nil, nil, nil)
		cfg.CompactCache, cfg.SnapshotInterval, cfg.MaxCRTotalDataSizeBytes = compact, time.Hour, 1
		peer := newRecordingPeerStore()
		require.NoError(t, peer.ApplyDelta(ctx, &PeerSyncDelta{Changes: []ResourceChange{{
			Obj: cachedAs(t, compact, collectionTestWidget("previously-observed", "payments")),
			GVR: collectionTestWidgetsGVR(), Source: ObjectSourceCR, EventType: watch.Added,
		}}}))
		source := &collectionReportingFixture{view: collectionReportingSnapshot{
			Objects: map[schema.GroupVersionResource]ObjectGroup{}, Complete: true,
			AllowDelete: func(ResourceChange) bool { return false },
		}}
		sink := &consumertest.LogsSink{}
		c := newResourceCollector(testSettings(t).Logger, cfg, sink, source, peer, nil, nil)
		c.runIncrement(ctx)
		require.True(t, peer.IsEmpty(), "the deliberate payload-budget drop must prune the retained entry")
		records := emittedRecords(sink, 0)
		require.Len(t, records, 1)
		assert.Equal(t, string(watch.Deleted), records[0].Body["type"])
	})
}

func TestCollectionRuntimeReusesControlWatchWhenDeclarationsAreSelected(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, compact bool) {
		cfg := collectionTestConfig(t, []string{resourcecollection.Group}, nil, nil, nil)
		cfg.CompactCache = compact
		declaration := collectionTestDeclaration("integration", resourcecollection.Selection{
			Group: resourcecollection.Group, Resource: resourcecollection.GVR().Resource,
		})
		crd := makeTestCRDWithVersions("resourcecollections."+resourcecollection.Group,
			resourcecollection.Group, resourcecollection.Kind, resourcecollection.GVR().Resource,
			[]map[string]interface{}{{"name": resourcecollection.Version, "storage": true, "served": true}})
		require.NoError(t, unstructured.SetNestedField(crd.Object, "Cluster", "spec", "scope"))
		client := collectionRuntimeClient(t, crd, collectionTestDeclarationObject(t, declaration))
		disc := &collectionRuntimeDiscovery{resources: []*metav1.APIResourceList{
			collectionTestAPI(resourcecollection.Group, resourcecollection.Version, false, resourcecollection.GVR().Resource),
		}}
		r, _ := startCollectionTestRuntime(t, cfg, client, disc)
		collectionRuntimeStatus(t, client, declaration.Name, resourcecollection.Active)
		watches := 0
		for _, action := range client.Actions() {
			if action.GetVerb() == "watch" && action.GetResource() == resourcecollection.GVR() {
				watches++
			}
		}
		assert.Equal(t, 1, watches, "control, baseline, declaration and automatic reporting reuse one source")
		assert.Len(t, r.collectionSnapshot().Objects[resourcecollection.GVR()].Objects, 1)
	})
}

func TestCollectionRuntimeHealthyVersionDeletesWhileAnotherVersionIsForbidden(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, compact bool) {
		v2 := schema.GroupVersionResource{Group: collectionTestGroup, Version: "v2", Resource: "widgets"}
		cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "widgets", Group: collectionTestGroup, Version: "v2"}}, nil)
		cfg.CompactCache, cfg.SnapshotInterval = compact, time.Hour
		declaration := collectionTestDeclaration("integration", collectionTestSelection("widgets", nil, nil))
		crd := makeTestCRDWithVersions("widgets."+collectionTestGroup, collectionTestGroup, "Widget", "widgets",
			[]map[string]interface{}{
				{"name": "v1", "storage": true, "served": true},
				{"name": "v2", "storage": false, "served": true},
			})
		client := collectionRuntimeClient(t, crd, collectionTestDeclarationObject(t, declaration))
		client.PrependReactor("list", "widgets", func(action clienttesting.Action) (bool, runtime.Object, error) {
			if action.GetResource().Version == "v2" {
				return true, nil, apierrors.NewForbidden(v2.GroupResource(), "", errors.New("v2 reads not granted"))
			}
			return false, nil, nil
		})
		disc := &collectionRuntimeDiscovery{resources: []*metav1.APIResourceList{
			collectionTestAPI(collectionTestGroup, "v1", true, "widgets"),
			collectionTestAPI(collectionTestGroup, "v2", true, "widgets"),
		}}
		r, ctx := startCollectionTestRuntime(t, cfg, client, disc)
		collectionRuntimeStatus(t, client, declaration.Name, resourcecollection.Active)
		v1Object := collectionTestWidget("old-v1", "payments")
		v2Object := makeTestCR("old-v2", "payments", collectionTestGroup, "v2", "Widget")
		peer := newRecordingPeerStore()
		require.NoError(t, peer.ApplyDelta(ctx, &PeerSyncDelta{Changes: []ResourceChange{
			{Obj: cachedAs(t, compact, v1Object), GVR: collectionTestWidgetsGVR(), Source: ObjectSourceCR, EventType: watch.Added},
			{Obj: cachedAs(t, compact, v2Object), GVR: v2, Source: ObjectSourceCR, EventType: watch.Added},
		}}))
		sink := &consumertest.LogsSink{}
		c := newResourceCollector(testSettings(t).Logger, cfg, sink, r, peer, nil, nil)
		c.runIncrement(ctx)
		events := map[string]interface{}{}
		for _, record := range emittedRecords(sink, 0) {
			if name, ok := record.Attrs[emit.AttrK8sObjectName].(string); ok {
				events[name] = record.Body["type"]
			}
		}
		assert.Equal(t, string(watch.Deleted), events["old-v1"])
		assert.Equal(t, string(watch.Added), events["old-v2"])
	})
}

func TestCollectionRuntimeUnavailableDeclarationAPIKeepsNativeBaseline(t *testing.T) {
	for _, reason := range []string{"absent", "forbidden"} {
		t.Run(reason, func(t *testing.T) {
			cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "pods"}}, nil)
			client := collectionRuntimeClient(t, collectionRuntimeCRD(), makeTestPod("baseline-pod", "base", nil))
			client.PrependReactor("list", "resourcecollections", func(clienttesting.Action) (bool, runtime.Object, error) {
				if reason == "absent" {
					return true, nil, apierrors.NewNotFound(resourcecollection.GVR().GroupResource(), "")
				}
				return true, nil, apierrors.NewForbidden(resourcecollection.GVR().GroupResource(), "", errors.New("no declaration reads"))
			})
			disc := &collectionRuntimeDiscovery{resources: []*metav1.APIResourceList{collectionTestAPI("", "v1", true, "pods")}}
			r, _ := startCollectionTestRuntime(t, cfg, client, disc)
			require.Eventually(t, func() bool {
				return r.collectionSnapshot().Complete && len(r.collectionSnapshot().Objects[podsGVR].Objects) == 1
			}, 5*time.Second, 20*time.Millisecond)
			assert.False(t, r.collectionSnapshot().AllowDelete(ResourceChange{GVR: collectionTestWidgetsGVR(), Source: ObjectSourceCR}))
		})
	}
}

func TestCollectionRuntimeShutdownCancelsDiscoveryRequests(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		once.Do(func() { close(started) })
		select {
		case <-request.Context().Done():
		case <-release:
			_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"APIGroupList","groups":[]}`))
		}
	}))
	defer server.Close()
	defer close(release)
	disc, err := discovery.NewDiscoveryClientForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	r, _ := startCollectionTestRuntime(t, cfg, collectionRuntimeClient(t), disc)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("discovery did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, r.Shutdown(ctx))
}

type collectionLifecycleElector struct {
	component.Component
	start k8sleaderelector.StartCallback
	stop  k8sleaderelector.StopCallback
}

func (e *collectionLifecycleElector) SetCallBackFuncs(start k8sleaderelector.StartCallback, stop k8sleaderelector.StopCallback) {
	e.start, e.stop = start, stop
}

func TestReceiverResourceCollectionsFollowLeadershipAndRestart(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "pods"}}, nil)
	cfg.IncrementInterval, cfg.SnapshotInterval = 50*time.Millisecond, time.Hour
	cfg.PeerSyncPort = 0
	id := component.MustNewID("k8s_leader_elector")
	cfg.K8sLeaderElector = &id
	declaration := collectionTestDeclaration("integration", collectionTestSelection("widgets", []string{"team-a"}, nil))
	client := collectionRuntimeClient(t, collectionRuntimeCRD(),
		collectionTestDeclarationObject(t, declaration),
		collectionTestWidget("a-widget", "team-a"), collectionTestWidget("b-widget", "team-b"),
		makeTestPod("baseline-pod", "base", nil))
	elector := &collectionLifecycleElector{}
	r := &k8sresourceReceiver{
		settings: testSettings(t), config: cfg, consumer: &consumertest.LogsSink{},
		metrics: metrics.NoopRecorder{}, dynamicClient: client,
		discoveryClient: &collectionRuntimeDiscovery{resources: []*metav1.APIResourceList{
			collectionTestAPI("", "v1", true, "pods"), collectionTestAPI(collectionTestGroup, "v1", true, "widgets"),
		}},
	}
	require.NoError(t, r.Start(context.Background(), receiverTestHost{extensions: map[component.ID]component.Component{id: elector}}))
	t.Cleanup(func() { require.NoError(t, r.Shutdown(context.Background())) })
	require.NotNil(t, elector.start)
	r.mu.Lock()
	assert.Nil(t, r.collector)
	r.mu.Unlock()
	leader, lose := context.WithCancel(context.Background())
	elector.start(leader)
	collectionRuntimeStatus(t, client, declaration.Name, resourcecollection.Active)
	r.mu.Lock()
	first := r.collector
	r.mu.Unlock()
	require.NotNil(t, first)
	require.IsType(t, &collectionRuntime{}, first.informers)
	lose()
	elector.stop()
	r.mu.Lock()
	assert.Nil(t, r.collector)
	r.mu.Unlock()

	client.ClearActions()
	declaration.Generation = 2
	declaration.Spec.Resources[0].Namespaces = []string{"team-b"}
	_, err := client.Resource(resourcecollection.GVR()).Update(context.Background(),
		collectionTestDeclarationObject(t, declaration), metav1.UpdateOptions{})
	require.NoError(t, err)
	for _, action := range client.Actions() {
		assert.False(t, action.GetSubresource() == "status" && action.GetVerb() == "update",
			"a stopped replica must not publish declaration feedback")
	}
	secondLeader, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	elector.start(secondLeader)
	collectionRuntimeStatus(t, client, declaration.Name, resourcecollection.Active)
	r.mu.Lock()
	second := r.collector
	r.mu.Unlock()
	require.NotNil(t, second)
	assert.NotSame(t, first, second)
	source, ok := second.informers.(collectionReportingSource)
	require.True(t, ok)
	require.Eventually(t, func() bool {
		objects := source.collectionSnapshot().Objects[collectionTestWidgetsGVR()].Objects
		return len(objects) == 1 && objects[0].GetNamespace() == "team-b"
	}, 5*time.Second, 20*time.Millisecond)
}
