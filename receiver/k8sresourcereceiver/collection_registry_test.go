//nolint:testpackage
package k8sresourcereceiver

import (
	"context"
	"errors"
	"sync/atomic"
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
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

// Intercept before entering Fake.Invokes: its mutex serializes reactor calls.
type collectionListInterceptClient struct {
	dynamic.Interface
	beforeList func(context.Context, string, metav1.ListOptions) error
}

func (collectionListInterceptClient) IsWatchListSemanticsUnSupported() bool { return true }

func (c collectionListInterceptClient) Resource(gvr schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	resource := c.Interface.Resource(gvr)
	return collectionListInterceptNamespaceable{
		collectionListInterceptResource: collectionListInterceptResource{
			ResourceInterface: resource, beforeList: c.beforeList,
		},
		underlying: resource,
	}
}

type collectionListInterceptNamespaceable struct {
	collectionListInterceptResource
	underlying dynamic.NamespaceableResourceInterface
}

func (r collectionListInterceptNamespaceable) Namespace(namespace string) dynamic.ResourceInterface {
	return collectionListInterceptResource{
		ResourceInterface: r.underlying.Namespace(namespace), namespace: namespace,
		beforeList: r.beforeList,
	}
}

type collectionListInterceptResource struct {
	dynamic.ResourceInterface
	namespace  string
	beforeList func(context.Context, string, metav1.ListOptions) error
}

func (r collectionListInterceptResource) List(ctx context.Context, options metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	if err := r.beforeList(ctx, r.namespace, options); err != nil {
		return nil, err
	}
	return r.ResourceInterface.List(ctx, options)
}

func collectionTestRegistry(t *testing.T, client dynamic.Interface, compact bool) *collectionWatchRegistry {
	t.Helper()
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	cfg.CompactCache = compact
	informers := newResourceInformers(testSettings(t), cfg, client, nil, nil, nil)
	registry := newCollectionWatchRegistry(context.Background(), client, informers.buildInformer)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, registry.shutdown(ctx))
	})
	return registry
}

func collectionRegistrySpec(namespace string) collectionWatchSpec {
	return collectionWatchSpec{
		Key:        collectionWatchKey{GVR: podsGVR, Namespace: namespace},
		Namespaced: true, Source: ObjectSourceStatic,
		Contributors: []collectionContributor{{Kind: collectionDeclaration, Declaration: "pods"}},
	}
}

func collectionRegistryObservation(r *collectionWatchRegistry, key collectionWatchKey) collectionWatchSnapshot {
	for _, observation := range r.snapshot() {
		if observation.Spec.Key == key {
			return observation
		}
	}
	return collectionWatchSnapshot{}
}

func TestCollectionRegistrySharesAndWithdrawsContributions(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, compact bool) {
		pod := collectionTestWidget("checkout", "payments")
		client := collectionTestWidgetClient(pod)
		stream := watch.NewRaceFreeFake()
		client.PrependWatchReactor("widgets", func(clienttesting.Action) (bool, watch.Interface, error) {
			return true, stream, nil
		})
		registry := collectionTestRegistry(t, client, compact)
		cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "widgets", Group: collectionTestGroup, Version: "v1", Namespaces: []string{"payments"}}}, nil)
		a := collectionTestDeclaration("a", resourcecollection.Selection{Group: collectionTestGroup, Resource: "widgets", Namespaces: []string{"payments"}})
		b := collectionTestDeclaration("b", a.Spec.Resources[0])
		plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{a, b})
		require.Len(t, plan.Watches, 1)
		require.NoError(t, registry.reconcile(plan.Watches))
		key := plan.Watches[0].Key
		require.Eventually(t, func() bool {
			return collectionRegistryObservation(registry, key).State == resourcecollection.Active
		}, 5*time.Second, 10*time.Millisecond)
		observation := collectionRegistryObservation(registry, key)
		require.Len(t, observation.Objects, 1)
		assertRepresentation(t, compact, observation.Objects[0])
		require.Len(t, observation.Spec.Contributors, 3)

		plan = buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{b})
		require.NoError(t, registry.reconcile(plan.Watches))
		assert.Len(t, collectionRegistryObservation(registry, key).Spec.Contributors, 2)
		require.False(t, stream.IsStopped())
		updated := pod.DeepCopy()
		updated.SetResourceVersion("2")
		stream.Modify(updated)
		require.Eventually(t, func() bool {
			objects := collectionRegistryObservation(registry, key).Objects
			return len(objects) == 1 && objects[0].GetResourceVersion() == "2"
		}, 5*time.Second, 10*time.Millisecond)

		plan = buildCollectionPlan(cfg, collectionTestDiscovery(), nil)
		require.NoError(t, registry.reconcile(plan.Watches))
		assert.Len(t, collectionRegistryObservation(registry, key).Spec.Contributors, 1)
		var watchCalls int
		for _, action := range client.Actions() {
			if action.GetVerb() == "watch" {
				watchCalls++
			}
		}
		assert.Equal(t, 1, watchCalls, "contributor changes must not restart an identical watch")

		cfg = collectionTestConfig(t, nil, nil, nil, nil)
		require.NoError(t, registry.reconcile(buildCollectionPlan(cfg, collectionTestDiscovery(), nil).Watches))
		assert.Empty(t, registry.snapshot())
		require.Eventually(t, stream.IsStopped, 5*time.Second, 10*time.Millisecond)
	})
}

func TestCollectionRegistryPreservesNamespaceAndSelectorRequests(t *testing.T) {
	client := staticPodClient()
	registry := collectionTestRegistry(t, client, false)
	a, b, c := collectionRegistrySpec("payments"), collectionRegistrySpec("staging"), collectionRegistrySpec("payments")
	a.Key.LabelSelector, a.Key.FieldSelector = "app=checkout", "status.phase=Running"
	b.Key.LabelSelector, b.Key.FieldSelector = a.Key.LabelSelector, a.Key.FieldSelector
	c.Key.LabelSelector, c.Key.FieldSelector = "monitoring=enabled", "metadata.name=checkout"
	specs := []collectionWatchSpec{a, b, c}
	require.NoError(t, registry.reconcile(specs))
	require.Eventually(t, func() bool {
		observations := registry.snapshot()
		if len(observations) != 3 {
			return false
		}
		for _, observation := range observations {
			if observation.State != resourcecollection.Active {
				return false
			}
		}
		var watchCalls int
		for _, action := range client.Actions() {
			if action.GetVerb() == "watch" {
				watchCalls++
			}
		}
		return watchCalls == 3
	}, 5*time.Second, 10*time.Millisecond)
	for _, spec := range specs {
		var listed, watched bool
		for _, action := range client.Actions() {
			if action.GetNamespace() != spec.Key.Namespace {
				continue
			}
			if list, ok := action.(clienttesting.ListAction); ok {
				filters := list.GetListRestrictions()
				listed = listed || (filters.Labels.String() == spec.Key.LabelSelector && filters.Fields.String() == spec.Key.FieldSelector)
			}
			if stream, ok := action.(clienttesting.WatchAction); ok {
				filters := stream.GetWatchRestrictions()
				watched = watched || (filters.Labels.String() == spec.Key.LabelSelector && filters.Fields.String() == spec.Key.FieldSelector)
			}
		}
		assert.True(t, listed, "list must use the exact scope and filters: %v", spec.Key)
		assert.True(t, watched, "watch must use the exact scope and filters: %v", spec.Key)
	}
	specs[0].Contributors[0].Declaration = "changed-input"
	snapshot := registry.snapshot()
	snapshot[0].Spec.Contributors[0].Declaration = "changed-snapshot"
	for _, observation := range registry.snapshot() {
		assert.Equal(t, "pods", observation.Spec.Contributors[0].Declaration)
	}
}

func TestCollectionRegistryKeepsVersionWatchesIndependent(t *testing.T) {
	scheme := testScheme()
	listKinds := map[schema.GroupVersionResource]string{}
	for _, version := range []string{"v1", "v2"} {
		registerCRGVR(scheme, collectionTestGroup, version, "Widget")
		listKinds[schema.GroupVersionResource{Group: collectionTestGroup, Version: version, Resource: "widgets"}] = "WidgetList"
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds)
	registry := collectionTestRegistry(t, client, false)
	a, b := collectionRegistrySpec(""), collectionRegistrySpec("")
	a.Key.GVR = schema.GroupVersionResource{Group: collectionTestGroup, Version: "v1", Resource: "widgets"}
	b.Key.GVR = a.Key.GVR
	b.Key.GVR.Version = "v2"
	a.Source, b.Source = ObjectSourceCR, ObjectSourceCR
	require.NoError(t, registry.reconcile([]collectionWatchSpec{a, b}))
	require.Eventually(t, func() bool {
		return collectionRegistryObservation(registry, a.Key).State == resourcecollection.Active &&
			collectionRegistryObservation(registry, b.Key).State == resourcecollection.Active
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, registry.reconcile([]collectionWatchSpec{b}))
	observations := registry.snapshot()
	require.Len(t, observations, 1)
	assert.Equal(t, b.Key, observations[0].Spec.Key)
	assert.Equal(t, resourcecollection.Active, observations[0].State)
	assert.Equal(t, ObjectSourceCR, observations[0].Spec.Source)
}

func TestCollectionRegistryPendingInitialObservationDoesNotBlockHealthyWatch(t *testing.T) {
	client := staticPodClient()
	release, requested := make(chan struct{}), make(chan struct{}, 1)
	intercepted := collectionListInterceptClient{Interface: client, beforeList: func(ctx context.Context, namespace string, _ metav1.ListOptions) error {
		if namespace == "waiting" {
			select {
			case requested <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}}
	registry := collectionTestRegistry(t, intercepted, false)
	t.Cleanup(func() { close(release) })
	waiting, healthy := collectionRegistrySpec("waiting"), collectionRegistrySpec("healthy")
	require.NoError(t, registry.reconcile([]collectionWatchSpec{waiting, healthy}))
	select {
	case <-requested:
	case <-time.After(5 * time.Second):
		t.Fatal("initial list did not start")
	}
	assert.Equal(t, resourcecollection.Pending, collectionRegistryObservation(registry, waiting.Key).State)
	assert.False(t, collectionRegistryObservation(registry, waiting.Key).Synced)
	require.Eventually(t, func() bool {
		observation := collectionRegistryObservation(registry, healthy.Key)
		return observation.State == resourcecollection.Active && len(observation.Objects) == 0
	}, 5*time.Second, 10*time.Millisecond, "a successful empty list is an active observation")
}

func TestCollectionRegistryReportsScopedObservationFailures(t *testing.T) {
	for _, tc := range []struct {
		name, state, reason string
		err                 error
	}{
		{"forbidden", resourcecollection.Blocked, "AccessDenied", apierrors.NewForbidden(podsGVR.GroupResource(), "", errors.New("read denied"))},
		{"unauthorized", resourcecollection.Blocked, "AccessDenied", apierrors.NewUnauthorized("credentials unavailable")},
		{"missing API", resourcecollection.Pending, "APINotAvailable", apierrors.NewNotFound(podsGVR.GroupResource(), "")},
		{"unsupported field", resourcecollection.Blocked, "InvalidSelection", apierrors.NewBadRequest("field is unsupported")},
		{"transient API", resourcecollection.Pending, "ObservationFailed", apierrors.NewServiceUnavailable("API unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := staticPodClient()
			registry := collectionTestRegistry(t, client, false)
			failed, healthy := collectionRegistrySpec("failed"), collectionRegistrySpec("healthy")
			client.PrependReactor("list", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if action.GetNamespace() == "failed" {
					return true, nil, tc.err
				}
				return false, nil, nil
			})
			require.NoError(t, registry.reconcile([]collectionWatchSpec{failed, healthy}))
			require.Eventually(t, func() bool {
				failure := collectionRegistryObservation(registry, failed.Key)
				return failure.State == tc.state && failure.Reason == tc.reason &&
					collectionRegistryObservation(registry, healthy.Key).State == resourcecollection.Active
			}, 5*time.Second, 10*time.Millisecond)
			failure := collectionRegistryObservation(registry, failed.Key)
			assert.Contains(t, failure.Message, tc.err.Error())
			assert.False(t, failure.Synced)
		})
	}
}

func TestCollectionRegistryRetainsKnownObjectsThroughWatchFailureAndRecovery(t *testing.T) {
	client := staticPodClient(makeTestPod("checkout", "payments", nil))
	registry := collectionTestRegistry(t, client, false)
	spec := collectionRegistrySpec("payments")
	var denied atomic.Bool
	streams := make(chan *watch.RaceFreeFakeWatcher, 10)
	forbidden := apierrors.NewForbidden(podsGVR.GroupResource(), "", errors.New("read denied"))
	client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		if denied.Load() {
			return true, nil, forbidden
		}
		return false, nil, nil
	})
	client.PrependWatchReactor("pods", func(clienttesting.Action) (bool, watch.Interface, error) {
		if denied.Load() {
			return true, nil, forbidden
		}
		stream := watch.NewRaceFreeFake()
		streams <- stream
		return true, stream, nil
	})
	require.NoError(t, registry.reconcile([]collectionWatchSpec{spec}))
	require.Eventually(t, func() bool {
		observation := collectionRegistryObservation(registry, spec.Key)
		return observation.State == resourcecollection.Active && len(observation.Objects) == 1
	}, 5*time.Second, 10*time.Millisecond)
	stream := <-streams
	denied.Store(true)
	stream.Error(&metav1.Status{Status: metav1.StatusFailure, Reason: metav1.StatusReasonForbidden, Code: 403, Message: "read denied"})
	require.Eventually(t, func() bool {
		observation := collectionRegistryObservation(registry, spec.Key)
		return observation.State == resourcecollection.Blocked && observation.Reason == "AccessDenied"
	}, 5*time.Second, 10*time.Millisecond)
	failed := collectionRegistryObservation(registry, spec.Key)
	assert.True(t, failed.Synced, "initial synchronization alone cannot describe current observation health")
	require.Len(t, failed.Objects, 1)
	assert.Equal(t, "checkout", failed.Objects[0].GetName())
	denied.Store(false)
	require.Eventually(t, func() bool {
		observation := collectionRegistryObservation(registry, spec.Key)
		return observation.State == resourcecollection.Active && len(observation.Objects) == 1
	}, 10*time.Second, 10*time.Millisecond)
}

func TestCollectionRegistryShutdownCancelsBlockedWatchForwarding(t *testing.T) {
	client := staticPodClient()
	registry := collectionTestRegistry(t, client, false)
	stream := watch.NewRaceFreeFake()
	observed := registry.observeWatch(registry.ctx, stream, func(error) {})
	stream.Add(makeTestPod("checkout", "payments", nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, registry.shutdown(ctx))
	assert.True(t, stream.IsStopped())
	select {
	case _, open := <-observed.ResultChan():
		assert.False(t, open)
	default:
		t.Fatal("watch forwarding goroutine did not stop")
	}
	assert.ErrorIs(t, registry.reconcile([]collectionWatchSpec{collectionRegistrySpec("")}), context.Canceled)
	assert.Empty(t, registry.snapshot())
	require.NoError(t, registry.shutdown(ctx))
}

func TestCollectionRegistryConcurrentReconciliationAndShutdown(t *testing.T) {
	registry := collectionTestRegistry(t, staticPodClient(), false)
	specs := []collectionWatchSpec{collectionRegistrySpec("payments")}
	require.NoError(t, registry.reconcile(specs))
	finished := make(chan error, 1)
	go func() {
		for {
			registry.snapshot()
			if err := registry.reconcile(specs); err != nil {
				finished <- err
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, registry.shutdown(ctx))
	select {
	case err := <-finished:
		assert.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("reconciliation continued after shutdown")
	}
}

var collectionTestWidgetsGVR = schema.GroupVersionResource{Group: collectionTestGroup, Version: "v1", Resource: "widgets"}

func collectionTestWidget(name, namespace string) *unstructured.Unstructured {
	object := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": collectionTestGroup + "/v1", "kind": "Widget",
		"metadata": map[string]interface{}{"name": name, "namespace": namespace},
		"spec":     map[string]interface{}{},
	}}
	return object
}

func collectionTestWidgetClient(objects ...*unstructured.Unstructured) *dynamicfake.FakeDynamicClient {
	fixtures := make([]runtime.Object, len(objects))
	for index, object := range objects {
		fixtures[index] = object
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{collectionTestWidgetsGVR: "WidgetList"}, fixtures...)
}
