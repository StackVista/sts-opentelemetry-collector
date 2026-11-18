//nolint:testpackage
package k8sresourcereceiver

import (
	"context"
	"errors"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/collectiondeclaration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	clienttesting "k8s.io/client-go/testing"
)

func TestCollectionDiscoveryUsesOneCatalogWithPerResourceFallback(t *testing.T) {
	groups := []*metav1.APIGroup{{
		Name:             collectionTestGroup,
		PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: collectionTestGroup + "/v2", Version: "v2"},
		Versions: []metav1.GroupVersionForDiscovery{
			{GroupVersion: collectionTestGroup + "/v1", Version: "v1"},
			{GroupVersion: collectionTestGroup + "/v2", Version: "v2"},
		},
	}}
	served := []*metav1.APIResourceList{
		collectionTestAPI(collectionTestGroup, "v1", true, "widgets", "gadgets", "widgets/status"),
		collectionTestAPI(collectionTestGroup, "v2", true, "widgets"),
	}
	preferred, err := preferredCollectionResources(context.Background(), groups, served, nil)
	require.NoError(t, err)
	versions := map[string]string{}
	for _, list := range preferred {
		for _, resource := range list.APIResources {
			versions[resource.Name] = list.GroupVersion
		}
	}
	assert.Equal(t, map[string]string{"widgets": collectionTestGroup + "/v2", "gadgets": collectionTestGroup + "/v1"}, versions)

	failed := &discovery.ErrGroupDiscoveryFailed{Groups: map[schema.GroupVersion]error{
		{Group: collectionTestGroup, Version: "v2"}: errors.New("unavailable"),
	}}
	preferred, err = preferredCollectionResources(context.Background(), groups, served[:1], failed)
	require.True(t, discovery.IsGroupDiscoveryFailedError(err))
	require.Len(t, preferred[0].APIResources, 2)

	disc := &collectionRuntimeDiscovery{resources: served}
	r := newCollectionRuntime(testSettings(t), collectionTestConfig(t, nil, nil, nil, nil), nil, disc, nil)
	r.ctx = context.Background()
	r.refreshDiscovery()
	assert.Equal(t, int64(1), disc.calls.Load())
	assert.True(t, r.discoveryComplete)
	assert.Len(t, r.catalog.ServedResources, 2)
}

func TestCollectionStatusConflictRetriesCurrentResourceVersion(t *testing.T) {
	declaration := collectionTestDeclaration("integration", collectionTestSelection("widgets", nil, nil))
	object := collectionTestDeclarationObject(t, declaration)
	client := collectionTestStatusClient(t, object)
	attempts := 0
	client.PrependReactor("update", "collectiondeclarations", func(action clienttesting.Action) (bool, runtime.Object, error) {
		attempts++
		update, ok := action.(clienttesting.UpdateAction)
		require.True(t, ok)
		written, ok := update.GetObject().(*unstructured.Unstructured)
		require.True(t, ok)
		if attempts == 1 {
			current := object.DeepCopy()
			current.SetResourceVersion("11")
			current.SetLabels(map[string]string{"other-writer": "kept"})
			require.NoError(t, client.Tracker().Update(collectiondeclaration.GVR(), current, ""))
			return true, nil, apierrors.NewConflict(collectiondeclaration.GVR().GroupResource(), declaration.Name, nil)
		}
		assert.Equal(t, "11", written.GetResourceVersion())
		assert.Equal(t, "kept", written.GetLabels()["other-writer"])
		return false, nil, nil
	})
	result := collectionTestStatusResult(declaration.Name)
	require.NoError(t, writeCollectionStatus(context.Background(), client, result, collectionDeclarationStatus(result, nil)))
	assert.Equal(t, 2, attempts)
}

func TestCollectionExplicitChangesWithdrawPreviousAndPeerObjects(t *testing.T) {
	changes := map[string]struct {
		before, after func(*collectiondeclaration.Selection)
	}{
		"missing-resource": {after: func(selection *collectiondeclaration.Selection) { selection.Resource = "notinstalled" }},
		"namespace":        {after: func(selection *collectiondeclaration.Selection) { selection.Namespaces = []string{"team-b"} }},
		"label": {after: func(selection *collectiondeclaration.Selection) {
			selection.LabelSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "new"}}
		}},
		"broader-label": {
			before: func(selection *collectiondeclaration.Selection) {
				selection.LabelSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "old"}}
			},
			after: func(selection *collectiondeclaration.Selection) {
				selection.LabelSelector = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{"old", "new"},
				}}}
			},
		},
		"removed-label": {
			before: func(selection *collectiondeclaration.Selection) {
				selection.LabelSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "old"}}
			},
			after: func(selection *collectiondeclaration.Selection) { selection.LabelSelector = nil },
		},
		"broader-namespaces": {
			before: func(selection *collectiondeclaration.Selection) { selection.Namespaces = []string{"team-a"} },
			after:  func(selection *collectiondeclaration.Selection) { selection.Namespaces = []string{"team-a", "team-b"} },
		},
		"rejected-field":     {after: func(selection *collectiondeclaration.Selection) { selection.FieldSelector = "metadata.name=new" }},
		"rejected-neq-field": {after: func(selection *collectiondeclaration.Selection) { selection.FieldSelector = "spec.unsupported!=x" }},
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			forEachCacheMode(t, func(t *testing.T, compact bool) {
				registry := newCollectionHandoverTestRegistry()
				handover := newCollectionHandover(registry)
				declaration := collectionTestDeclaration("integration", collectionTestSelection("widgets", nil, nil))
				if change.before != nil {
					change.before(&declaration.Spec.Resources[0])
				}
				old := collectionHandoverPlan(t, declaration)
				require.NoError(t, handover.update(old))
				object := collectionHandoverObject("old", "old")
				object.SetLabels(map[string]string{"app": "old"})
				var cached resourceObject = object
				if compact {
					var err error
					cached, err = newCompactObject(object)
					require.NoError(t, err)
				}
				registry.observe(old.Watches[0].Key, cached)
				prior := collectionHandoverRead(t, handover)
				require.Len(t, prior.Objects[collectionTestWidgetsGVR()].Objects, 1)
				peers := newResourceCache()
				peers.applyAdditions(peers.computeChanges(nil, prior.Objects))
				declaration.Generation++
				change.after(&declaration.Spec.Resources[0])
				next := collectionHandoverPlan(t, declaration)
				require.NoError(t, handover.update(next))
				for _, spec := range next.Watches {
					registry.fail(spec.Key, "AccessDenied")
				}
				view := collectionHandoverRead(t, handover)
				assert.Empty(t, view.Objects)
				require.Len(t, view.Withdrawn, 1)
				for key := range registry.observations {
					assert.True(t, slicesContainWatch(next.Watches, key), "withdrawn watches must not remain in the registry")
				}
				cfg := collectionTestConfig(t, nil, nil, nil, nil)
				allowDelete := collectionDeletionPolicy(cfg, next, collectionTestDiscovery(), registry.snapshot(), view.Withdrawn, true, true, false)
				assert.True(t, allowDelete(ResourceChange{GVR: collectionTestWidgetsGVR(), Obj: cached, Source: ObjectSourceCR}),
					"failed replacement and discovery must not reintroduce withdrawn objects from peers")
				_, reported, _ := preserveCollectionPeerView(nil, view.Objects, peers.computeChanges(nil, view.Objects), allowDelete)
				assert.Empty(t, reported, "withdrawn objects must remain absent after peer preservation")
				delta := peers.computeChanges(nil, reported)
				require.Len(t, delta, 1)
				assert.Equal(t, watch.Deleted, delta[0].EventType)
				assert.Len(t, collectionHandoverRead(t, handover).Withdrawn, 1, "reports must not consume pending withdrawal provenance")
			})
		})
	}
}

func TestCollectionScopeReorderingKeepsUnchangedObservations(t *testing.T) {
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	declaration := collectionTestDeclaration("integration", collectionTestSelection("widgets", []string{"team-a", "team-b"}, nil))
	old := collectionHandoverPlan(t, declaration)
	require.NoError(t, handover.update(old))
	for _, spec := range old.Watches {
		registry.observe(spec.Key, collectionHandoverObject("kept", "old"))
	}
	collectionHandoverRead(t, handover)
	declaration.Generation++
	declaration.Spec.Resources[0].Namespaces = []string{"team-b", "team-a"}
	require.NoError(t, handover.update(collectionHandoverPlan(t, declaration)))
	for _, spec := range old.Watches {
		registry.fail(spec.Key, "AccessDenied")
	}
	assert.Len(t, collectionHandoverRead(t, handover).Objects[collectionTestWidgetsGVR()].Objects, 1)
	assert.Equal(t, int64(1), collectionHandoverRead(t, handover).Active[0].Generation)
	assert.Empty(t, collectionHandoverRead(t, handover).Withdrawn)
}

func slicesContainWatch(specs []collectionWatchSpec, key collectionWatchKey) bool {
	for _, spec := range specs {
		if spec.Key == key {
			return true
		}
	}
	return false
}

func TestCollectionWithdrawalsKeepIndependentPeerProtections(t *testing.T) {
	for _, baseline := range []bool{false, true} {
		name := "unchanged-declaration"
		if baseline {
			name = "unchanged-baseline"
		}
		t.Run(name, func(t *testing.T) {
			forEachCacheMode(t, func(t *testing.T, compact bool) {
				cfg := collectionTestConfig(t, nil, nil, nil, nil)
				changed := collectionTestDeclaration("changed", collectionTestSelection("widgets", nil, map[string]string{"app": "old"}))
				declarations := []*collectiondeclaration.CollectionDeclaration{changed}
				if baseline {
					cfg.Objects = []ObjectWatch{{Group: collectionTestGroup, Name: "widgets", LabelSelector: "app in (old,other)"}}
				} else {
					unchanged := collectionTestDeclaration("unchanged", collectionTestSelection("widgets", nil, nil))
					unchanged.Spec.Resources[0].LabelSelector = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
						Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{"old", "other"},
					}}}
					declarations = append(declarations, unchanged)
				}
				registry := newCollectionHandoverTestRegistry()
				handover := newCollectionHandover(registry)
				old := buildCollectionPlan(cfg, collectionTestDiscovery(), declarations)
				require.NoError(t, handover.update(old))
				object := collectionHandoverObject("old", "old")
				object.SetLabels(map[string]string{"app": "old"})
				var cached resourceObject = object
				if compact {
					var err error
					cached, err = newCompactObject(object)
					require.NoError(t, err)
				}
				for _, spec := range old.Watches {
					if spec.Key.LabelSelector == "app=old" {
						registry.observe(spec.Key, cached)
					} else {
						registry.fail(spec.Key, "AccessDenied")
					}
				}
				peers := newResourceCache()
				peers.applyAdditions(peers.computeChanges(nil, collectionHandoverRead(t, handover).Objects))
				changed.Generation++
				changed.Spec.Resources[0].LabelSelector = nil
				next := buildCollectionPlan(cfg, collectionTestDiscovery(), declarations)
				require.NoError(t, handover.update(next))
				for _, spec := range next.Watches {
					registry.fail(spec.Key, "AccessDenied")
				}
				view := collectionHandoverRead(t, handover)
				assert.Empty(t, view.Objects)
				require.Len(t, view.Withdrawn, 1)
				allowDelete := collectionDeletionPolicy(cfg, next, collectionTestDiscovery(), registry.snapshot(), view.Withdrawn, true, true, false)
				assert.False(t, allowDelete(ResourceChange{GVR: collectionTestWidgetsGVR(), Obj: cached, Source: ObjectSourceCR}))
				_, reported, _ := preserveCollectionPeerView(nil, view.Objects, peers.computeChanges(nil, view.Objects), allowDelete)
				require.Len(t, reported[collectionTestWidgetsGVR()].Objects, 1)
				assert.Empty(t, peers.computeChanges(nil, reported))
			})
		})
	}
}
