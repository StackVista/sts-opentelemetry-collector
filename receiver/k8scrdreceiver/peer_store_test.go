//nolint:testpackage
package k8scrdreceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// roundtripCache marshals a cache through the peer sync envelope and returns the
// deserialized cache. Used by tests that focus on cache field preservation.
func roundtripCache(t *testing.T, c *resourceCache) *resourceCache {
	t.Helper()
	data, err := marshalPeerSyncMessage(&PeerSyncMessage{Cache: c})
	require.NoError(t, err)
	restored, err := unmarshalPeerSyncMessage(data)
	require.NoError(t, err)
	return restored.Cache
}

func TestMarshalUnmarshalResourceCache_WithCRs(t *testing.T) {
	original := newResourceCache()

	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	cr := makeTestCR("my-widget", "default", "example.com", "v1", "Widget")
	cr.SetResourceVersion("7")

	key := crResourceKey(gvr, "default", "my-widget")
	original.CRs[key] = &cachedCR{Obj: cr, GVR: gvr}

	restored := roundtripCache(t, original)

	require.Len(t, restored.CRs, 1)
	restoredCR := restored.CRs[key]
	require.NotNil(t, restoredCR)
	assert.Equal(t, "my-widget", restoredCR.Obj.GetName())
	assert.Equal(t, "default", restoredCR.Obj.GetNamespace())
	assert.Equal(t, "7", restoredCR.Obj.GetResourceVersion())
	assert.Equal(t, gvr, restoredCR.GVR)
}

func TestUnmarshalPeerSyncMessage_InvalidJSON(t *testing.T) {
	_, err := unmarshalPeerSyncMessage([]byte("not json"))
	require.Error(t, err)
}

func TestMarshalUnmarshalPeerSyncMessage_PreservesLastSnapshotTime(t *testing.T) {
	want := time.UnixMilli(1_700_000_000_000)
	data, err := marshalPeerSyncMessage(&PeerSyncMessage{
		Cache:            newResourceCache(),
		LastSnapshotTime: want,
	})
	require.NoError(t, err)

	got, err := unmarshalPeerSyncMessage(data)
	require.NoError(t, err)
	assert.True(t, want.Equal(got.LastSnapshotTime), "want %v, got %v", want, got.LastSnapshotTime)
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
	original.CRs[key] = &cachedCR{Obj: cr, GVR: gvr}

	restored := roundtripCache(t, original)

	restoredCR := restored.CRs[key]
	require.NotNil(t, restoredCR)

	// Verify nested spec fields are preserved (important for CEL topology mappings)
	spec, found, err := unstructured.NestedMap(restoredCR.Obj.Object, "spec")
	require.NoError(t, err)
	require.True(t, found)

	replicas, found, err := unstructured.NestedInt64(spec, "replicas")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, int64(3), replicas)

	// Verify status fields are preserved
	ready, found, err := unstructured.NestedBool(restoredCR.Obj.Object, "status", "ready")
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, ready)
}
