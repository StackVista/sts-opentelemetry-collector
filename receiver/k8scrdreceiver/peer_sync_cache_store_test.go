//nolint:testpackage // Tests require access to internal types
package k8scrdreceiver

import (
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func testCacheWithData(t *testing.T) *resourceCache {
	t.Helper()
	cache := newResourceCache()

	crd := makeTestCRDUnstructured("widgets.example.com", "example.com", "Widget", "widgets")
	crd.SetResourceVersion("42")
	cache.crds["widgets.example.com"] = crd

	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	cr := makeTestCR("my-widget", "default", "example.com", "v1", "Widget")
	cr.SetResourceVersion("7")
	key := crResourceKey(gvr, "default", "my-widget")
	cache.crs[key] = &cachedCR{obj: cr, gvr: gvr}

	return cache
}

func peerPort(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	return host, port
}

// --- Core Save/Load ---

func TestPeerSyncCacheStore_SaveLoadRoundtrip(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	store := newPeerSyncCacheStore(logger, 0, "")

	original := testCacheWithData(t)

	err := store.Save(context.Background(), original)
	require.NoError(t, err)

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)

	require.Len(t, loaded.crds, 1)
	assert.Equal(t, "widgets.example.com", loaded.crds["widgets.example.com"].GetName())
	assert.Equal(t, "42", loaded.crds["widgets.example.com"].GetResourceVersion())

	require.Len(t, loaded.crs, 1)
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	key := crResourceKey(gvr, "default", "my-widget")
	assert.Equal(t, "my-widget", loaded.crs[key].obj.GetName())
	assert.Equal(t, "7", loaded.crs[key].obj.GetResourceVersion())
}

func TestPeerSyncCacheStore_LoadReturnsEmptyWhenNoData(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	store := newPeerSyncCacheStore(logger, 0, "")

	cache, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.True(t, cache.isEmpty())
}

// --- HTTP handler ---

func TestPeerSyncCacheStore_HandleGet_Returns204WhenEmpty(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	store := newPeerSyncCacheStore(logger, 0, "")

	req := httptest.NewRequest(http.MethodGet, syncCachePath, nil)
	w := httptest.NewRecorder()
	store.handleSyncCache(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestPeerSyncCacheStore_HandlePost_StoresReceivedData(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	store := newPeerSyncCacheStore(logger, 0, "")

	cache := testCacheWithData(t)
	data, err := marshalResourceCache(cache)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, syncCachePath, strings.NewReader(string(data)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	store.handleSyncCache(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.Len(t, loaded.crds, 1)
	assert.Len(t, loaded.crs, 1)
}

func TestPeerSyncCacheStore_HandlePost_GzipCompressed(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	store := newPeerSyncCacheStore(logger, 0, "")

	data, err := marshalResourceCache(testCacheWithData(t))
	require.NoError(t, err)

	compressed, err := gzipCompress(data)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, syncCachePath, strings.NewReader(string(compressed)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	store.handleSyncCache(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.Len(t, loaded.crds, 1)
}

// --- Lifecycle ---

func TestPeerSyncCacheStore_StartStop(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	store := newPeerSyncCacheStore(logger, 0, "")
	err := store.Start(context.Background())
	require.NoError(t, err)

	store.Stop()
}

// --- Load failover: pull from peers ---

func TestPeerSyncCacheStore_Load_PullsFromPeerWhenLocalEmpty(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	// Set up a peer that has cached data (simulates previous leader).
	peer := newPeerSyncCacheStore(logger, 0, "")
	err := peer.Save(context.Background(), testCacheWithData(t))
	require.NoError(t, err)

	peerServer := httptest.NewServer(http.HandlerFunc(peer.handleSyncCache))
	defer peerServer.Close()

	_, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())

	// New store with no local data — Load should pull from the peer.
	store := newPeerSyncCacheStore(logger, peerPortInt, "localhost")

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.Len(t, loaded.crds, 1)
	assert.Len(t, loaded.crs, 1)
}

func TestPeerSyncCacheStore_Load_ReturnsEmptyWhenAllPeersEmpty(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	// Peer has no data — returns 204.
	peer := newPeerSyncCacheStore(logger, 0, "")
	peerServer := httptest.NewServer(http.HandlerFunc(peer.handleSyncCache))
	defer peerServer.Close()

	_, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())

	store := newPeerSyncCacheStore(logger, peerPortInt, "localhost")

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.True(t, loaded.isEmpty())
}

func TestPeerSyncCacheStore_Load_PrefersLocalOverPull(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	// Peer has different data than local.
	peerCache := newResourceCache()
	peerCRD := makeTestCRDUnstructured("gadgets.example.com", "example.com", "Gadget", "gadgets")
	peerCache.crds["gadgets.example.com"] = peerCRD

	peer := newPeerSyncCacheStore(logger, 0, "")
	err := peer.Save(context.Background(), peerCache)
	require.NoError(t, err)

	peerServer := httptest.NewServer(http.HandlerFunc(peer.handleSyncCache))
	defer peerServer.Close()

	_, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())

	// Store has local data — should use it, not pull from peer.
	store := newPeerSyncCacheStore(logger, peerPortInt, "localhost")
	err = store.Save(context.Background(), testCacheWithData(t))
	require.NoError(t, err)

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)

	assert.Contains(t, loaded.crds, "widgets.example.com")
	_, hasGadgets := loaded.crds["gadgets.example.com"]
	assert.False(t, hasGadgets, "should prefer local data over peer pull")
}

// --- Unmarshal recovery ---

func TestPeerSyncCacheStore_Load_RecoverFromCorruptData(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	store := newPeerSyncCacheStore(logger, 0, "")

	// Store corrupt data directly.
	store.mu.Lock()
	store.syncedCacheData = []byte("not valid json")
	store.mu.Unlock()

	// Load should recover gracefully — return empty cache, no error.
	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.True(t, loaded.isEmpty())
}

// --- Broadcast behavior ---

func TestPeerSyncCacheStore_BroadcastSkipsSelf(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	var receivedPush bool
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		receivedPush = true
		w.WriteHeader(http.StatusOK)
	}))
	defer peerServer.Close()

	peerHost, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())

	// Set POD_IP to the peer's address so broadcastToPeers skips it.
	t.Setenv("POD_IP", peerHost)

	store := newPeerSyncCacheStore(logger, peerPortInt, "localhost")

	data, err := marshalResourceCache(testCacheWithData(t))
	require.NoError(t, err)

	store.broadcastToPeers(context.Background(), data)

	assert.False(t, receivedPush, "should not push to self")
}

func TestPeerSyncCacheStore_BroadcastNoOpWithoutDNS(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	store := newPeerSyncCacheStore(logger, defaultPeerPort, "")

	data, err := marshalResourceCache(testCacheWithData(t))
	require.NoError(t, err)

	// Should return immediately without panic when peerDNS is empty.
	store.broadcastToPeers(context.Background(), data)
}

func TestPeerSyncCacheStore_BroadcastHandlesDNSFailure(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	store := newPeerSyncCacheStore(logger, defaultPeerPort, "this-dns-does-not-exist.invalid")

	data, err := marshalResourceCache(testCacheWithData(t))
	require.NoError(t, err)

	// Should not panic — logs and returns.
	store.broadcastToPeers(context.Background(), data)
}

// --- Push data integrity ---

func TestPeerSyncCacheStore_PushToPeer_DataIntegrity(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	// Use a real peer store as the receiver to verify end-to-end data flow.
	receiver := newPeerSyncCacheStore(logger, 0, "")
	peerServer := httptest.NewServer(http.HandlerFunc(receiver.handleSyncCache))
	defer peerServer.Close()

	peerHost, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())
	sender := newPeerSyncCacheStore(logger, peerPortInt, "")

	data, err := marshalResourceCache(testCacheWithData(t))
	require.NoError(t, err)

	compressed, err := gzipCompress(data)
	require.NoError(t, err)

	client := &http.Client{Timeout: pushPeerTimeout}
	sender.pushToPeer(context.Background(), client, peerHost, compressed)

	// Receiver should now be able to Load the pushed data.
	loaded, err := receiver.Load(context.Background())
	require.NoError(t, err)

	require.Len(t, loaded.crds, 1)
	assert.Equal(t, "widgets.example.com", loaded.crds["widgets.example.com"].GetName())
	require.Len(t, loaded.crs, 1)

	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	key := crResourceKey(gvr, "default", "my-widget")
	assert.Equal(t, "my-widget", loaded.crs[key].obj.GetName())
}

// --- Context cancellation ---

func TestPeerSyncCacheStore_PushRespectsContextCancellation(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	// Slow peer that blocks longer than we're willing to wait.
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(5 * time.Second):
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer peerServer.Close()

	peerHost, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())
	store := newPeerSyncCacheStore(logger, peerPortInt, "")

	data := []byte(`{"crds":{},"crs":{}}`)
	compressed, err := gzipCompress(data)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	client := &http.Client{Timeout: pushPeerTimeout}

	start := time.Now()
	store.pushToPeer(ctx, client, peerHost, compressed)
	assert.Less(t, time.Since(start), 2*time.Second, "should return promptly on cancelled context")
}

// --- Pull DNS failure ---

func TestPeerSyncCacheStore_PullHandlesDNSFailure(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	store := newPeerSyncCacheStore(logger, defaultPeerPort, "this-dns-does-not-exist.invalid")

	data := store.pullFromPeers(context.Background(), 1*time.Second)
	assert.Nil(t, data)
}

// --- GET handler serves data correctly ---

func TestPeerSyncCacheStore_HandleGet_ServesGzippedData(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	store := newPeerSyncCacheStore(logger, 0, "")

	err := store.Save(context.Background(), testCacheWithData(t))
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, syncCachePath, nil)
	w := httptest.NewRecorder()
	store.handleSyncCache(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))

	gr, err := gzip.NewReader(resp.Body)
	require.NoError(t, err)
	defer gr.Close()

	data, err := io.ReadAll(gr)
	require.NoError(t, err)

	restored, err := unmarshalResourceCache(data)
	require.NoError(t, err)
	assert.Len(t, restored.crds, 1)
	assert.Len(t, restored.crs, 1)
}
