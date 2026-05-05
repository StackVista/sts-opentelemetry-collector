//nolint:testpackage
package k8scrdreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

//nolint:unparam
func makeCachedCRD(name, resourceVersion string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apiextensions.k8s.io/v1",
			"kind":       "CustomResourceDefinition",
			"metadata": map[string]interface{}{
				"name":            name,
				"resourceVersion": resourceVersion,
			},
			"spec": map[string]interface{}{
				"group": "example.com",
			},
		},
	}
}

//nolint:unparam
func makeCachedCR(name, namespace, group, version, kind, resourceVersion string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": group + "/" + version,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"name":            name,
				"resourceVersion": resourceVersion,
			},
		},
	}
	if namespace != "" {
		//nolint:forcetypeassert
		obj.Object["metadata"].(map[string]interface{})["namespace"] = namespace
	}
	return obj
}

func TestResourceCache_IsEmpty(t *testing.T) {
	rc := newResourceCache()
	assert.True(t, rc.isEmpty())

	rc.CRDs["test"] = &unstructured.Unstructured{}
	assert.False(t, rc.isEmpty())
}

func TestResourceCache_ComputeChanges_CRDAdded(t *testing.T) {
	rc := newResourceCache()

	currentCRDs := []*unstructured.Unstructured{
		makeCachedCRD("foos.example.com", "1"),
	}

	changes := rc.computeChanges(currentCRDs, nil)

	require.Len(t, changes, 1)
	assert.Equal(t, watch.Added, changes[0].EventType)
	assert.True(t, changes[0].IsCRD)
	assert.Equal(t, "foos.example.com", changes[0].Obj.GetName())
}

func TestResourceCache_ComputeChanges_CRDModified(t *testing.T) {
	rc := newResourceCache()
	rc.CRDs["foos.example.com"] = makeCachedCRD("foos.example.com", "1")

	currentCRDs := []*unstructured.Unstructured{
		makeCachedCRD("foos.example.com", "2"), // different resourceVersion
	}

	changes := rc.computeChanges(currentCRDs, nil)

	require.Len(t, changes, 1)
	assert.Equal(t, watch.Modified, changes[0].EventType)
	assert.True(t, changes[0].IsCRD)
}

func TestResourceCache_ComputeChanges_CRDDeleted(t *testing.T) {
	rc := newResourceCache()
	rc.CRDs["foos.example.com"] = makeCachedCRD("foos.example.com", "1")

	// Current state: no CRDs
	changes := rc.computeChanges(nil, nil)

	require.Len(t, changes, 1)
	assert.Equal(t, watch.Deleted, changes[0].EventType)
	assert.True(t, changes[0].IsCRD)
	assert.Equal(t, "foos.example.com", changes[0].Obj.GetName())
}

func TestResourceCache_ComputeChanges_CRDUnchanged(t *testing.T) {
	rc := newResourceCache()
	rc.CRDs["foos.example.com"] = makeCachedCRD("foos.example.com", "1")

	currentCRDs := []*unstructured.Unstructured{
		makeCachedCRD("foos.example.com", "1"), // same resourceVersion
	}

	changes := rc.computeChanges(currentCRDs, nil)
	assert.Empty(t, changes)
}

func TestResourceCache_ComputeChanges_CRAdded(t *testing.T) {
	rc := newResourceCache()

	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "foos"}
	currentCRs := map[schema.GroupVersionResource][]*unstructured.Unstructured{
		gvr: {
			makeCachedCR("my-foo", "default", "example.com", "v1", "Foo", "1"),
		},
	}

	changes := rc.computeChanges(nil, currentCRs)

	require.Len(t, changes, 1)
	assert.Equal(t, watch.Added, changes[0].EventType)
	assert.False(t, changes[0].IsCRD)
	assert.Equal(t, gvr, changes[0].GVR)
	assert.Equal(t, "my-foo", changes[0].Obj.GetName())
}

func TestResourceCache_ComputeChanges_CRModified(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "foos"}
	key := crResourceKey(gvr, "default", "my-foo")

	rc := newResourceCache()
	rc.CRs[key] = &cachedCR{
		Obj: makeCachedCR("my-foo", "default", "example.com", "v1", "Foo", "1"),
		GVR: gvr,
	}

	currentCRs := map[schema.GroupVersionResource][]*unstructured.Unstructured{
		gvr: {
			makeCachedCR("my-foo", "default", "example.com", "v1", "Foo", "2"),
		},
	}

	changes := rc.computeChanges(nil, currentCRs)

	require.Len(t, changes, 1)
	assert.Equal(t, watch.Modified, changes[0].EventType)
	assert.Equal(t, gvr, changes[0].GVR)
}

func TestResourceCache_ComputeChanges_CRDeleted(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "foos"}
	key := crResourceKey(gvr, "default", "my-foo")

	rc := newResourceCache()
	rc.CRs[key] = &cachedCR{
		Obj: makeCachedCR("my-foo", "default", "example.com", "v1", "Foo", "1"),
		GVR: gvr,
	}

	// Current state: no CRs
	changes := rc.computeChanges(nil, nil)

	require.Len(t, changes, 1)
	assert.Equal(t, watch.Deleted, changes[0].EventType)
	assert.Equal(t, gvr, changes[0].GVR)
	assert.Equal(t, "my-foo", changes[0].Obj.GetName())
}

func TestResourceCache_ComputeChanges_MixedChanges(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "foos"}

	rc := newResourceCache()
	rc.CRDs["foos.example.com"] = makeCachedCRD("foos.example.com", "1")
	rc.CRs[crResourceKey(gvr, "default", "existing-foo")] = &cachedCR{
		Obj: makeCachedCR("existing-foo", "default", "example.com", "v1", "Foo", "1"),
		GVR: gvr,
	}
	rc.CRs[crResourceKey(gvr, "default", "deleted-foo")] = &cachedCR{
		Obj: makeCachedCR("deleted-foo", "default", "example.com", "v1", "Foo", "1"),
		GVR: gvr,
	}

	currentCRDs := []*unstructured.Unstructured{
		makeCachedCRD("foos.example.com", "2"), // modified
	}
	currentCRs := map[schema.GroupVersionResource][]*unstructured.Unstructured{
		gvr: {
			makeCachedCR("existing-foo", "default", "example.com", "v1", "Foo", "1"), // unchanged
			makeCachedCR("new-foo", "default", "example.com", "v1", "Foo", "1"),      // added
			// deleted-foo is absent → deleted
		},
	}

	changes := rc.computeChanges(currentCRDs, currentCRs)

	// Expect: CRD modified + CR added (new-foo) + CR deleted (deleted-foo) = 3
	require.Len(t, changes, 3)

	changesByType := make(map[watch.EventType]int)
	for _, c := range changes {
		changesByType[c.EventType]++
	}
	assert.Equal(t, 1, changesByType[watch.Modified])
	assert.Equal(t, 1, changesByType[watch.Added])
	assert.Equal(t, 1, changesByType[watch.Deleted])
}

func TestResourceCache_ApplyAdditions(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "foos"}
	key := crResourceKey(gvr, "default", "my-foo")

	rc := newResourceCache()
	// Pre-existing entries that ApplyAdditions should update or leave alone.
	rc.CRDs["foos.example.com"] = makeCachedCRD("foos.example.com", "1")
	rc.CRs[key] = &cachedCR{Obj: makeCachedCR("my-foo", "default", "example.com", "v1", "Foo", "1"), GVR: gvr}

	newCRD := makeCachedCRD("foos.example.com", "2") // modify
	newCR := makeCachedCR("my-foo", "default", "example.com", "v1", "Foo", "2")
	addedCR := makeCachedCR("other-foo", "default", "example.com", "v1", "Foo", "1") // add
	deletedCRD := makeCachedCRD("ignored.example.com", "1")                          // should be skipped

	rc.applyAdditions([]ResourceChange{
		{Obj: newCRD, EventType: watch.Modified, IsCRD: true},
		{Obj: newCR, EventType: watch.Modified, GVR: gvr},
		{Obj: addedCR, EventType: watch.Added, GVR: gvr},
		{Obj: deletedCRD, EventType: watch.Deleted, IsCRD: true},
	})

	assert.Equal(t, "2", rc.CRDs["foos.example.com"].GetResourceVersion(), "CRD should be modified")
	assert.NotContains(t, rc.CRDs, "ignored.example.com", "deleted change must not be applied")
	assert.Equal(t, "2", rc.CRs[key].Obj.GetResourceVersion(), "CR should be modified")
	assert.Contains(t, rc.CRs, crResourceKey(gvr, "default", "other-foo"), "added CR should be present")

}

func TestResourceCache_ApplyDeletions(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "foos"}
	keepKey := crResourceKey(gvr, "default", "keep-foo")
	dropKey := crResourceKey(gvr, "default", "drop-foo")

	rc := newResourceCache()
	rc.CRDs["keep.example.com"] = makeCachedCRD("keep.example.com", "1")
	rc.CRDs["drop.example.com"] = makeCachedCRD("drop.example.com", "1")
	rc.CRs[keepKey] = &cachedCR{Obj: makeCachedCR("keep-foo", "default", "example.com", "v1", "Foo", "1"), GVR: gvr}
	rc.CRs[dropKey] = &cachedCR{Obj: makeCachedCR("drop-foo", "default", "example.com", "v1", "Foo", "1"), GVR: gvr}

	rc.applyDeletions([]ResourceChange{
		{Obj: makeCachedCRD("drop.example.com", "1"), EventType: watch.Deleted, IsCRD: true},
		{Obj: makeCachedCR("drop-foo", "default", "example.com", "v1", "Foo", "1"), EventType: watch.Deleted, GVR: gvr},
		// Non-delete changes must be ignored — they don't affect the cache here.
		{Obj: makeCachedCRD("keep.example.com", "2"), EventType: watch.Modified, IsCRD: true},
	})

	assert.NotContains(t, rc.CRDs, "drop.example.com", "deleted CRD should be removed")
	assert.NotContains(t, rc.CRs, dropKey, "deleted CR should be removed")
	assert.Contains(t, rc.CRDs, "keep.example.com", "non-delete change must not affect cache")
	assert.Equal(t, "1", rc.CRDs["keep.example.com"].GetResourceVersion(), "modify ignored, original retained")
	assert.Contains(t, rc.CRs, keepKey)
}

func TestCRResourceKey(t *testing.T) {
	gvr := schema.GroupVersionResource{
		Group:    "policies.kubewarden.io",
		Version:  "v1",
		Resource: "policyservers",
	}

	key := crResourceKey(gvr, "kubewarden", "default")
	assert.Equal(t, "policies.kubewarden.io/v1/policyservers/kubewarden/default", key)

	// Cluster-scoped (empty namespace)
	key = crResourceKey(gvr, "", "default")
	assert.Equal(t, "policies.kubewarden.io/v1/policyservers//default", key)
}
