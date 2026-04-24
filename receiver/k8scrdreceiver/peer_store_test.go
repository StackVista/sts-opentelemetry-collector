//nolint:testpackage
package k8scrdreceiver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// roundtripCache pushes c through a real handleSnapshot → fetchPeerSnapshot HTTP
// transfer (NDJSON streamed, gzipped) and returns the assembled cache. Exercises
// the same code paths used in production.
func roundtripCache(t *testing.T, c *resourceCache) *resourceCache {
	t.Helper()
	server := newPeerSyncCacheStore(zaptest.NewLogger(t), 0, "", nil)
	server.SetLeader(true)
	seedCache(server, c)

	ts := httptest.NewServer(http.HandlerFunc(server.handleSnapshot))
	t.Cleanup(ts.Close)

	host, port := peerPort(t, ts.Listener.Addr().String())
	client := newPeerSyncCacheStore(zaptest.NewLogger(t), port, "", nil)

	httpClient := &http.Client{Timeout: 5 * time.Second}
	snap, status, err := client.fetchPeerSnapshot(context.Background(), httpClient, host)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	return snap.Cache
}

func TestSnapshotRoundtrip_PreservesCRWithGVR(t *testing.T) {
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

func TestSnapshotRoundtrip_PreservesNestedSpecAndStatus(t *testing.T) {
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

	// Verify nested spec fields are preserved (important for CEL topology mappings).
	spec, found, err := unstructured.NestedMap(restoredCR.Obj.Object, "spec")
	require.NoError(t, err)
	require.True(t, found)

	replicas, found, err := unstructured.NestedInt64(spec, "replicas")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, int64(3), replicas)

	// Verify status fields are preserved.
	ready, found, err := unstructured.NestedBool(restoredCR.Obj.Object, "status", "ready")
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, ready)
}

func TestSnapshotRoundtrip_PreservesLastSnapshotTime(t *testing.T) {
	want := time.UnixMilli(1_700_000_000_000)
	server := newPeerSyncCacheStore(zaptest.NewLogger(t), 0, "", nil)
	server.SetLeader(true)
	seedCache(server, testCacheWithData(t))
	server.cacheMu.Lock()
	server.lastSnapshotTime = want
	server.cacheMu.Unlock()

	ts := httptest.NewServer(http.HandlerFunc(server.handleSnapshot))
	t.Cleanup(ts.Close)

	host, port := peerPort(t, ts.Listener.Addr().String())
	client := newPeerSyncCacheStore(zaptest.NewLogger(t), port, "", nil)

	snap, _, err := client.fetchPeerSnapshot(context.Background(), &http.Client{Timeout: 5 * time.Second}, host)
	require.NoError(t, err)
	assert.True(t, want.Equal(snap.LastSnapshotTime),
		"want %v, got %v", want, snap.LastSnapshotTime)
}
