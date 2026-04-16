//nolint:testpackage // Tests require access to internal types
package k8scrdreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestNoopCacheStore_LoadReturnsEmptyCache(t *testing.T) {
	store := &noopCacheStore{}
	cache, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.True(t, cache.isEmpty())
}

func TestNoopCacheStore_SaveIsNoop(t *testing.T) {
	store := &noopCacheStore{}
	cache := newResourceCache()
	cache.crds["test"] = &unstructured.Unstructured{}
	err := store.Save(context.Background(), cache)
	require.NoError(t, err)
}

func TestMarshalUnmarshalResourceCache_Empty(t *testing.T) {
	original := newResourceCache()

	data, err := marshalResourceCache(original)
	require.NoError(t, err)

	restored, err := unmarshalResourceCache(data)
	require.NoError(t, err)
	assert.True(t, restored.isEmpty())
}

func TestMarshalUnmarshalResourceCache_WithCRDs(t *testing.T) {
	original := newResourceCache()
	crd := makeTestCRDUnstructured("widgets.example.com", "example.com", "Widget", "widgets")
	crd.SetResourceVersion("42")
	original.crds["widgets.example.com"] = crd

	data, err := marshalResourceCache(original)
	require.NoError(t, err)

	restored, err := unmarshalResourceCache(data)
	require.NoError(t, err)

	require.Len(t, restored.crds, 1)
	restoredCRD := restored.crds["widgets.example.com"]
	require.NotNil(t, restoredCRD)
	assert.Equal(t, "widgets.example.com", restoredCRD.GetName())
	assert.Equal(t, "42", restoredCRD.GetResourceVersion())
}

func TestMarshalUnmarshalResourceCache_WithCRs(t *testing.T) {
	original := newResourceCache()

	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	cr := makeTestCR("my-widget", "default", "example.com", "v1", "Widget")
	cr.SetResourceVersion("7")

	key := crResourceKey(gvr, "default", "my-widget")
	original.crs[key] = &cachedCR{obj: cr, gvr: gvr}

	data, err := marshalResourceCache(original)
	require.NoError(t, err)

	restored, err := unmarshalResourceCache(data)
	require.NoError(t, err)

	require.Len(t, restored.crs, 1)
	restoredCR := restored.crs[key]
	require.NotNil(t, restoredCR)
	assert.Equal(t, "my-widget", restoredCR.obj.GetName())
	assert.Equal(t, "default", restoredCR.obj.GetNamespace())
	assert.Equal(t, "7", restoredCR.obj.GetResourceVersion())
	assert.Equal(t, gvr, restoredCR.gvr)
}

func TestMarshalUnmarshalResourceCache_FullRoundtrip(t *testing.T) {
	original := newResourceCache()

	// Add a CRD
	crd := makeTestCRDUnstructured("widgets.example.com", "example.com", "Widget", "widgets")
	crd.SetResourceVersion("10")
	original.crds["widgets.example.com"] = crd

	// Add CRs from two different GVRs
	gvr1 := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	cr1 := makeTestCR("widget-1", "ns-a", "example.com", "v1", "Widget")
	cr1.SetResourceVersion("100")
	key1 := crResourceKey(gvr1, "ns-a", "widget-1")
	original.crs[key1] = &cachedCR{obj: cr1, gvr: gvr1}

	gvr2 := schema.GroupVersionResource{Group: "other.io", Version: "v1beta1", Resource: "gadgets"}
	cr2 := makeTestCR("gadget-1", "", "other.io", "v1beta1", "Gadget")
	cr2.SetResourceVersion("200")
	key2 := crResourceKey(gvr2, "", "gadget-1")
	original.crs[key2] = &cachedCR{obj: cr2, gvr: gvr2}

	data, err := marshalResourceCache(original)
	require.NoError(t, err)

	restored, err := unmarshalResourceCache(data)
	require.NoError(t, err)

	// Verify CRDs
	require.Len(t, restored.crds, 1)
	assert.Equal(t, "widgets.example.com", restored.crds["widgets.example.com"].GetName())

	// Verify CRs
	require.Len(t, restored.crs, 2)

	r1 := restored.crs[key1]
	require.NotNil(t, r1)
	assert.Equal(t, "widget-1", r1.obj.GetName())
	assert.Equal(t, "ns-a", r1.obj.GetNamespace())
	assert.Equal(t, gvr1, r1.gvr)

	r2 := restored.crs[key2]
	require.NotNil(t, r2)
	assert.Equal(t, "gadget-1", r2.obj.GetName())
	assert.Equal(t, gvr2, r2.gvr)
}

func TestUnmarshalResourceCache_InvalidJSON(t *testing.T) {
	_, err := unmarshalResourceCache([]byte("not json"))
	require.Error(t, err)
}

func TestMarshalUnmarshalResourceCache_PreservesSpecFields(t *testing.T) {
	original := newResourceCache()

	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "example.com/v1",
			"kind":       "Widget",
			"metadata": map[string]interface{}{
				"name":            "complex-widget",
				"namespace":       "prod",
				"resourceVersion": "999",
			},
			"spec": map[string]interface{}{
				"replicas": int64(3),
				"template": map[string]interface{}{
					"containers": []interface{}{
						map[string]interface{}{
							"name":  "main",
							"image": "nginx:latest",
						},
					},
				},
			},
			"status": map[string]interface{}{
				"ready": true,
			},
		},
	}

	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	key := crResourceKey(gvr, "prod", "complex-widget")
	original.crs[key] = &cachedCR{obj: cr, gvr: gvr}

	data, err := marshalResourceCache(original)
	require.NoError(t, err)

	restored, err := unmarshalResourceCache(data)
	require.NoError(t, err)

	restoredCR := restored.crs[key]
	require.NotNil(t, restoredCR)

	// Verify nested spec fields are preserved (important for CEL topology mappings)
	spec, found, err := unstructured.NestedMap(restoredCR.obj.Object, "spec")
	require.NoError(t, err)
	require.True(t, found)

	replicas, found, err := unstructured.NestedInt64(spec, "replicas")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, int64(3), replicas)

	// Verify status fields are preserved
	ready, found, err := unstructured.NestedBool(restoredCR.obj.Object, "status", "ready")
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, ready)
}
