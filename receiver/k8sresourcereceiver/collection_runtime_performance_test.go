//nolint:testpackage
package k8sresourcereceiver

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/collectiondeclaration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

type collectionCountingStore struct {
	cache.Store
	lists atomic.Int64
}

func (s *collectionCountingStore) List() []interface{} {
	s.lists.Add(1)
	return s.Store.List()
}

type collectionSyncedInformer struct {
	cache.SharedIndexInformer
	store cache.Store
}

func (*collectionSyncedInformer) HasSynced() bool         { return true }
func (i *collectionSyncedInformer) GetStore() cache.Store { return i.store }

func collectionCachedRuntime(tb testing.TB, count int, compact bool) (
	*collectionRuntime, *collectionCountingStore, *collectionRuntimeDiscovery,
) {
	tb.Helper()
	cfg := &Config{
		CompactCache: compact,
		Objects:      []ObjectWatch{{Name: "pods"}},
	}
	require.NoError(tb, cfg.Validate())
	disc := &collectionRuntimeDiscovery{resources: []*metav1.APIResourceList{collectionTestAPI("", "v1", true, "pods")}}
	r := newCollectionRuntime(receiver.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}},
		cfg, nil, disc, nil)
	r.ctx = context.Background()
	r.registry = newCollectionWatchRegistry(r.ctx, nil, nil)
	r.controls = newCollectionWatchRegistry(r.ctx, nil, nil)
	tb.Cleanup(r.registry.cancel)
	tb.Cleanup(r.controls.cancel)
	transform := sanitizeObject(cfg.ConfigMapMaxDataSize)
	if compact {
		transform = compactTransform(transform)
	}
	objects := &collectionCountingStore{Store: cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc)}
	for index := range count {
		object, err := transform(makeTestPod(strconv.Itoa(index), "baseline", nil))
		require.NoError(tb, err)
		require.NoError(tb, objects.Add(object))
	}
	spec := collectionWatchSpec{Key: collectionWatchKey{GVR: podsGVR}, Namespaced: true, Source: ObjectSourceStatic}
	r.registry.entries[spec.Key] = &collectionRegistryEntry{
		spec: spec, informer: &collectionSyncedInformer{store: objects}, cancel: func() {},
	}
	for _, spec := range []collectionWatchSpec{
		{Key: collectionWatchKey{GVR: crdGVR}},
		{Key: collectionWatchKey{GVR: collectiondeclaration.GVR()}},
	} {
		r.controls.entries[spec.Key] = &collectionRegistryEntry{
			spec: spec, cancel: func() {},
			informer: &collectionSyncedInformer{
				store: &collectionCountingStore{Store: cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc)},
			},
		}
	}
	r.controlRegistry = &collectionControlRegistry{target: r.registry, controls: r.controls, transform: transform}
	r.handover = newCollectionHandover(r.controlRegistry)
	r.dirty.Store(true)
	r.reconcile()
	r.collectionSnapshot()
	return r, objects, disc
}

func TestCollectionRuntimeLegacyReadsUseLastReport(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, compact bool) {
		r, objects, _ := collectionCachedRuntime(t, 1, compact)
		before := objects.lists.Load()
		cached, err := r.controlRegistry.transform(makeTestPod("added", "baseline", nil))
		require.NoError(t, err)
		require.NoError(t, objects.Add(cached))

		assert.True(t, r.StaticWatchesSynced())
		assert.Empty(t, r.ReadCRDs())
		assert.Len(t, r.ReadObjects()[podsGVR].Objects, 1)
		assert.Equal(t, before, objects.lists.Load(), "accessors must not read stores or advance handover")

		r.collectionSnapshot()
		assert.Len(t, r.ReadObjects()[podsGVR].Objects, 2)
		assert.Equal(t, before+1, objects.lists.Load(), "only reporting assembles a new view")
	})
}

func TestCollectionRuntimeStatusOnlyControlsStayFreshWithoutReplanning(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, compact bool) {
		r, _, disc := collectionCachedRuntime(t, 1, compact)
		declaration := collectionTestDeclaration("integration", collectionTestSelection("widgets", nil, nil))
		object := collectionTestDeclarationObject(t, declaration)
		client := collectionRuntimeClient(t, object)
		r.client = client
		key := collectionWatchKey{GVR: collectiondeclaration.GVR()}
		store := r.controls.entries[key].informer.GetStore()
		require.NoError(t, store.Add(object))
		crd := collectionRuntimeCRD()
		crdStore := r.controls.entries[collectionWatchKey{GVR: crdGVR}].informer.GetStore()
		require.NoError(t, crdStore.Add(crd))
		r.controlChanged(nil, object)
		r.controlChanged(nil, crd)
		r.reconcile()
		state := r.reporting
		discoveryCalls := disc.calls.Load()

		current, err := client.Resource(collectiondeclaration.GVR()).Get(context.Background(), object.GetName(), metav1.GetOptions{})
		require.NoError(t, err)
		require.NoError(t, store.Update(current))
		r.controlChanged(object, current)
		updatedCRD := crd.DeepCopy()
		updatedCRD.SetLabels(map[string]string{"updated": "yes"})
		require.NoError(t, crdStore.Update(updatedCRD))
		r.controlChanged(crd, updatedCRD)
		client.ClearActions()

		countedCRDs, ok := crdStore.(*collectionCountingStore)
		require.True(t, ok)
		countedDeclarations, ok := store.(*collectionCountingStore)
		require.True(t, ok)
		crdReads, declarationReads := countedCRDs.lists.Load(), countedDeclarations.lists.Load()
		for range 3 {
			r.reconcile()
		}
		assert.Same(t, state, r.reporting, "status and metadata changes must not rebuild the plan")
		assert.Equal(t, crdReads, countedCRDs.lists.Load())
		assert.Equal(t, declarationReads, countedDeclarations.lists.Load(), "status comparisons use individual cache lookups")
		assert.Equal(t, discoveryCalls, disc.calls.Load())
		assert.Empty(t, client.Actions(), "current feedback needs neither GET nor PATCH")

		view := r.collectionSnapshot()
		require.Len(t, view.CRDs, 1)
		assert.Equal(t, "yes", view.CRDs[0].GetLabels()["updated"])
		require.Len(t, view.Objects[collectiondeclaration.GVR()].Objects, 1)
		reported, err := decodeObject(view.Objects[collectiondeclaration.GVR()].Objects[0])
		require.NoError(t, err)
		assert.Equal(t, current.Object["status"], reported.Object["status"])

		r.controls.entries[key].lastErr = apierrors.NewForbidden(key.GVR.GroupResource(), "", assert.AnError)
		r.reconcile()
		view = r.collectionSnapshot()
		require.Len(t, view.Objects[collectiondeclaration.GVR()].Objects, 1)
		reported, err = decodeObject(view.Objects[collectiondeclaration.GVR()].Objects[0])
		require.NoError(t, err)
		assert.Equal(t, current.Object["status"], reported.Object["status"], "a control failure must retain current cached feedback")
	})
}

func TestCollectionRuntimeReadsObjectChangesOnlyWhenReporting(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, compact bool) {
		r, objects, disc := collectionCachedRuntime(t, 1, compact)
		before := objects.lists.Load()
		discoveryCalls := disc.calls.Load()
		for range 3 {
			r.reconcile()
		}
		assert.Equal(t, before, objects.lists.Load(), "idle health ticks must not enumerate cached objects")
		// Subsequent reports must work even when discovery is unavailable.
		disc.set(nil, assert.AnError)
		transform := r.controlRegistry.transform
		object := makeTestPod("added", "baseline", nil)
		cached, err := transform(object.DeepCopy())
		require.NoError(t, err)
		require.NoError(t, objects.Add(cached))
		view := r.collectionSnapshot()
		require.Len(t, view.Objects[podsGVR].Objects, 2)
		assert.Equal(t, before+1, objects.lists.Load(), "a report reads each target store once")

		object.SetLabels(map[string]string{"updated": "yes"})
		cached, err = transform(object.DeepCopy())
		require.NoError(t, err)
		require.NoError(t, objects.Update(cached))
		view = r.collectionSnapshot()
		for _, item := range view.Objects[podsGVR].Objects {
			if item.GetName() == object.GetName() {
				decoded, decodeErr := decodeObject(item)
				require.NoError(t, decodeErr)
				assert.Equal(t, "yes", decoded.GetLabels()["updated"])
			}
		}
		require.NoError(t, objects.Delete(cached))
		view = r.collectionSnapshot()
		require.Len(t, view.Objects[podsGVR].Objects, 1)
		assert.Equal(t, "0", view.Objects[podsGVR].Objects[0].GetName())
		assert.True(t, view.Complete)
		assert.True(t, view.AllowDelete(ResourceChange{GVR: podsGVR, Obj: object}))
		assert.Equal(t, discoveryCalls, disc.calls.Load(), "data changes and idle ticks do not refresh discovery")
	})
}

func TestCollectionRuntimeReportingRetainsFailedWatchObjects(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, compact bool) {
		r, objects, _ := collectionCachedRuntime(t, 1, compact)
		entry := r.registry.entries[collectionWatchKey{GVR: podsGVR}]
		entry.lastErr = apierrors.NewForbidden(podsGVR.GroupResource(), "", assert.AnError)
		previous := objects.List()[0]
		require.NoError(t, objects.Delete(previous))
		r.reconcile()
		view := r.collectionSnapshot()
		require.Len(t, view.Objects[podsGVR].Objects, 1, "unavailable reads must retain the last report")
		assert.False(t, view.Complete)
		assert.False(t, view.AllowDelete(ResourceChange{GVR: podsGVR}))
		entry.lastErr = nil
		view = r.collectionSnapshot()
		assert.Empty(t, view.Objects[podsGVR].Objects, "recovered observation can establish deletion")
		assert.True(t, view.Complete)
		assert.True(t, view.AllowDelete(ResourceChange{GVR: podsGVR}))
	})
}

func BenchmarkCollectionRuntimeCachedObjects(b *testing.B) {
	for _, count := range []int{10000, 30000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			r, _, _ := collectionCachedRuntime(b, count, false)
			b.Run("idle-health", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					r.reconcile()
				}
			})
			b.Run("report", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					r.collectionSnapshot()
				}
			})
		})
	}
}
