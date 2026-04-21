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

	rc.crds["test"] = &unstructured.Unstructured{}
	assert.False(t, rc.isEmpty())
}

func TestResourceCache_ComputeChanges_CRDAdded(t *testing.T) {
	rc := newResourceCache()

	currentCRDs := []*unstructured.Unstructured{
		makeCachedCRD("foos.example.com", "1"),
	}

	changes := rc.computeChanges(currentCRDs, nil)

	require.Len(t, changes, 1)
	assert.Equal(t, watch.Added, changes[0].eventType)
	assert.True(t, changes[0].isCRD)
	assert.Equal(t, "foos.example.com", changes[0].obj.GetName())
}

func TestResourceCache_ComputeChanges_CRDModified(t *testing.T) {
	rc := newResourceCache()
	rc.crds["foos.example.com"] = makeCachedCRD("foos.example.com", "1")

	currentCRDs := []*unstructured.Unstructured{
		makeCachedCRD("foos.example.com", "2"), // different resourceVersion
	}

	changes := rc.computeChanges(currentCRDs, nil)

	require.Len(t, changes, 1)
	assert.Equal(t, watch.Modified, changes[0].eventType)
	assert.True(t, changes[0].isCRD)
}

func TestResourceCache_ComputeChanges_CRDDeleted(t *testing.T) {
	rc := newResourceCache()
	rc.crds["foos.example.com"] = makeCachedCRD("foos.example.com", "1")

	// Current state: no CRDs
	changes := rc.computeChanges(nil, nil)

	require.Len(t, changes, 1)
	assert.Equal(t, watch.Deleted, changes[0].eventType)
	assert.True(t, changes[0].isCRD)
	assert.Equal(t, "foos.example.com", changes[0].obj.GetName())
}

func TestResourceCache_ComputeChanges_CRDUnchanged(t *testing.T) {
	rc := newResourceCache()
	rc.crds["foos.example.com"] = makeCachedCRD("foos.example.com", "1")

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
	assert.Equal(t, watch.Added, changes[0].eventType)
	assert.False(t, changes[0].isCRD)
	assert.Equal(t, gvr, changes[0].gvr)
	assert.Equal(t, "my-foo", changes[0].obj.GetName())
}

func TestResourceCache_ComputeChanges_CRModified(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "foos"}
	key := crResourceKey(gvr, "default", "my-foo")

	rc := newResourceCache()
	rc.crs[key] = &cachedCR{
		obj: makeCachedCR("my-foo", "default", "example.com", "v1", "Foo", "1"),
		gvr: gvr,
	}

	currentCRs := map[schema.GroupVersionResource][]*unstructured.Unstructured{
		gvr: {
			makeCachedCR("my-foo", "default", "example.com", "v1", "Foo", "2"),
		},
	}

	changes := rc.computeChanges(nil, currentCRs)

	require.Len(t, changes, 1)
	assert.Equal(t, watch.Modified, changes[0].eventType)
	assert.Equal(t, gvr, changes[0].gvr)
}

func TestResourceCache_ComputeChanges_CRDeleted(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "foos"}
	key := crResourceKey(gvr, "default", "my-foo")

	rc := newResourceCache()
	rc.crs[key] = &cachedCR{
		obj: makeCachedCR("my-foo", "default", "example.com", "v1", "Foo", "1"),
		gvr: gvr,
	}

	// Current state: no CRs
	changes := rc.computeChanges(nil, nil)

	require.Len(t, changes, 1)
	assert.Equal(t, watch.Deleted, changes[0].eventType)
	assert.Equal(t, gvr, changes[0].gvr)
	assert.Equal(t, "my-foo", changes[0].obj.GetName())
}

func TestResourceCache_ComputeChanges_MixedChanges(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "foos"}

	rc := newResourceCache()
	rc.crds["foos.example.com"] = makeCachedCRD("foos.example.com", "1")
	rc.crs[crResourceKey(gvr, "default", "existing-foo")] = &cachedCR{
		obj: makeCachedCR("existing-foo", "default", "example.com", "v1", "Foo", "1"),
		gvr: gvr,
	}
	rc.crs[crResourceKey(gvr, "default", "deleted-foo")] = &cachedCR{
		obj: makeCachedCR("deleted-foo", "default", "example.com", "v1", "Foo", "1"),
		gvr: gvr,
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
		changesByType[c.eventType]++
	}
	assert.Equal(t, 1, changesByType[watch.Modified])
	assert.Equal(t, 1, changesByType[watch.Added])
	assert.Equal(t, 1, changesByType[watch.Deleted])
}

func TestResourceCache_Update_DeepCopies(t *testing.T) {
	rc := newResourceCache()

	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "foos"}
	crd := makeCachedCRD("foos.example.com", "1")
	cr := makeCachedCR("my-foo", "default", "example.com", "v1", "Foo", "1")

	rc.update(
		[]*unstructured.Unstructured{crd},
		map[schema.GroupVersionResource][]*unstructured.Unstructured{gvr: {cr}},
	)

	// Mutate the original objects
	crd.SetResourceVersion("999")
	cr.SetResourceVersion("999")

	// Cached copies should be unaffected
	assert.Equal(t, "1", rc.crds["foos.example.com"].GetResourceVersion())
	key := crResourceKey(gvr, "default", "my-foo")
	assert.Equal(t, "1", rc.crs[key].obj.GetResourceVersion())
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
