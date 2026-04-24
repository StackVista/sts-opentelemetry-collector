//nolint:testpackage
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
	cache.CRDs["widgets.example.com"] = crd

	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	cr := makeTestCR("my-widget", "default", "example.com", "v1", "Widget")
	cr.SetResourceVersion("7")
	key := crResourceKey(gvr, "default", "my-widget")
	cache.CRs[key] = &cachedCR{Obj: cr, GVR: gvr}

	return cache
}

// msg wraps a cache in a PeerSyncMessage. Test convenience.
func msg(c *resourceCache) *PeerSyncMessage {
	return &PeerSyncMessage{Cache: c}
}

// cacheBytes marshals a cache through the peer sync envelope.
func cacheBytes(t *testing.T, c *resourceCache) []byte {
	t.Helper()
	data, err := marshalPeerSyncMessage(msg(c))
	require.NoError(t, err)
	return data
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
	store := newPeerSyncCacheStore(logger, 0, "", nil)

	original := testCacheWithData(t)

	err := store.Save(context.Background(), msg(original))
	require.NoError(t, err)

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)

	require.Len(t, loaded.Cache.CRDs, 1)
	assert.Equal(t, "widgets.example.com", loaded.Cache.CRDs["widgets.example.com"].GetName())
	assert.Equal(t, "42", loaded.Cache.CRDs["widgets.example.com"].GetResourceVersion())

	require.Len(t, loaded.Cache.CRs, 1)
	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	key := crResourceKey(gvr, "default", "my-widget")
	assert.Equal(t, "my-widget", loaded.Cache.CRs[key].Obj.GetName())
	assert.Equal(t, "7", loaded.Cache.CRs[key].Obj.GetResourceVersion())
}

func TestPeerSyncCacheStore_LoadReturnsEmptyWhenNoData(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	store := newPeerSyncCacheStore(logger, 0, "", nil)

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.True(t, loaded.Cache.isEmpty())
}

// --- HTTP handler ---

func TestPeerSyncCacheStore_HandleGet_Returns204WhenEmpty(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	store := newPeerSyncCacheStore(logger, 0, "", nil)

	req := httptest.NewRequest(http.MethodGet, syncCachePath, nil)
	w := httptest.NewRecorder()
	store.handleSyncCache(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestPeerSyncCacheStore_HandlePost_StoresReceivedData(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	store := newPeerSyncCacheStore(logger, 0, "", nil)

	data := cacheBytes(t, testCacheWithData(t))

	req := httptest.NewRequest(http.MethodPost, syncCachePath, strings.NewReader(string(data)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	store.handleSyncCache(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.Len(t, loaded.Cache.CRDs, 1)
	assert.Len(t, loaded.Cache.CRs, 1)
}

func TestPeerSyncCacheStore_HandlePost_GzipCompressed(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	store := newPeerSyncCacheStore(logger, 0, "", nil)

	data := cacheBytes(t, testCacheWithData(t))

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
	assert.Len(t, loaded.Cache.CRDs, 1)
}

// --- Lifecycle ---

func TestPeerSyncCacheStore_StartStop(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	store := newPeerSyncCacheStore(logger, 0, "", nil)
	err := store.Start(context.Background())
	require.NoError(t, err)

	store.Stop()
}

// --- Load failover: pull from peers ---

func TestPeerSyncCacheStore_Load_PullsFromPeerWhenLocalEmpty(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	// Set up a peer that has cached data (simulates previous leader).
	peer := newPeerSyncCacheStore(logger, 0, "", nil)
	err := peer.Save(context.Background(), msg(testCacheWithData(t)))
	require.NoError(t, err)

	peerServer := httptest.NewServer(http.HandlerFunc(peer.handleSyncCache))
	defer peerServer.Close()

	_, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())

	// New store with no local data — Load should pull from the peer.
	store := newPeerSyncCacheStore(logger, peerPortInt, "localhost", nil)

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.Len(t, loaded.Cache.CRDs, 1)
	assert.Len(t, loaded.Cache.CRs, 1)
}

func TestPeerSyncCacheStore_Load_ReturnsEmptyWhenAllPeersEmpty(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	// Peer has no data — returns 204.
	peer := newPeerSyncCacheStore(logger, 0, "", nil)
	peerServer := httptest.NewServer(http.HandlerFunc(peer.handleSyncCache))
	defer peerServer.Close()

	_, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())

	store := newPeerSyncCacheStore(logger, peerPortInt, "localhost", nil)

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.True(t, loaded.Cache.isEmpty())
}

func TestPeerSyncCacheStore_Load_PrefersLocalOverPull(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	// Peer has different data than local.
	peerCache := newResourceCache()
	peerCRD := makeTestCRDUnstructured("gadgets.example.com", "example.com", "Gadget", "gadgets")
	peerCache.CRDs["gadgets.example.com"] = peerCRD

	peer := newPeerSyncCacheStore(logger, 0, "", nil)
	err := peer.Save(context.Background(), msg(peerCache))
	require.NoError(t, err)

	peerServer := httptest.NewServer(http.HandlerFunc(peer.handleSyncCache))
	defer peerServer.Close()

	_, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())

	// Store has local data — should use it, not pull from peer.
	store := newPeerSyncCacheStore(logger, peerPortInt, "localhost", nil)
	err = store.Save(context.Background(), msg(testCacheWithData(t)))
	require.NoError(t, err)

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)

	assert.Contains(t, loaded.Cache.CRDs, "widgets.example.com")
	_, hasGadgets := loaded.Cache.CRDs["gadgets.example.com"]
	assert.False(t, hasGadgets, "should prefer local data over peer pull")
}

// --- Unmarshal recovery ---

func TestPeerSyncCacheStore_Load_RecoverFromCorruptData(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	store := newPeerSyncCacheStore(logger, 0, "", nil)

	// Store corrupt data directly.
	store.mu.Lock()
	store.syncedCacheData = []byte("not valid json")
	store.mu.Unlock()

	// Load should recover gracefully — return empty cache, no error.
	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.True(t, loaded.Cache.isEmpty())
}

// --- Broadcast behavior ---

func TestPeerSyncCacheStore_BroadcastSkipsSelf(t *testing.T) {
	// Cannot use t.Parallel() because t.Setenv is used below.
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

	store := newPeerSyncCacheStore(logger, peerPortInt, "localhost", nil)

	data := cacheBytes(t, testCacheWithData(t))

	store.broadcastToPeers(context.Background(), data)

	assert.False(t, receivedPush, "should not push to self")
}

func TestPeerSyncCacheStore_BroadcastNoOpWithoutDNS(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	store := newPeerSyncCacheStore(logger, defaultPeerPort, "", nil)

	data := cacheBytes(t, testCacheWithData(t))

	// Should return immediately without panic when peerDNS is empty.
	store.broadcastToPeers(context.Background(), data)
}

func TestPeerSyncCacheStore_BroadcastHandlesDNSFailure(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	store := newPeerSyncCacheStore(logger, defaultPeerPort, "this-dns-does-not-exist.invalid", nil)

	data := cacheBytes(t, testCacheWithData(t))

	// Should not panic — logs and returns.
	store.broadcastToPeers(context.Background(), data)
}

// --- Concurrent push ---

func TestPeerSyncCacheStore_PushConcurrently_Parallel(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	const (
		numPeers     = 4
		perPeerDelay = 200 * time.Millisecond
	)

	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(perPeerDelay)
		w.WriteHeader(http.StatusOK)
	}))
	defer peerServer.Close()

	peerHost, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())
	store := newPeerSyncCacheStore(logger, peerPortInt, "", nil)

	peers := make([]string, numPeers)
	for i := range peers {
		peers[i] = peerHost
	}

	compressed, err := gzipCompress(cacheBytes(t, testCacheWithData(t)))
	require.NoError(t, err)
	client := &http.Client{Timeout: pushPeerTimeout}

	start := time.Now()
	// ackThreshold=0 → wait for all; ackWaitTimeout=0 → no timeout.
	acked, timedOut := store.pushConcurrently(context.Background(), client, peers, compressed, 0, 0)
	elapsed := time.Since(start)

	assert.Equal(t, numPeers, acked, "all peers should succeed")
	assert.False(t, timedOut)
	// Sequential would take ≥ numPeers*perPeerDelay; allow 2× perPeerDelay for scheduler jitter.
	assert.Less(t, elapsed, 2*perPeerDelay,
		"pushes should run concurrently; sequential would take ≥%v, got %v", numPeers*perPeerDelay, elapsed)
}

func TestPeerSyncCacheStore_PushConcurrently_EarlyReturnOnAckThreshold(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	const (
		numPeers     = 4
		perPeerDelay = 1 * time.Second
		ackThreshold = 1
	)

	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(perPeerDelay)
		w.WriteHeader(http.StatusOK)
	}))
	defer peerServer.Close()

	peerHost, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())
	store := newPeerSyncCacheStore(logger, peerPortInt, "", nil)

	peers := make([]string, numPeers)
	for i := range peers {
		peers[i] = peerHost
	}

	compressed, err := gzipCompress(cacheBytes(t, testCacheWithData(t)))
	require.NoError(t, err)
	client := &http.Client{Timeout: pushPeerTimeout}

	start := time.Now()
	acked, timedOut := store.pushConcurrently(context.Background(), client, peers, compressed, ackThreshold, 0)
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, acked, ackThreshold, "should return as soon as threshold is met")
	assert.False(t, timedOut)
	// First peer responds at ~perPeerDelay; we shouldn't wait for the rest (would be ~4×).
	assert.Less(t, elapsed, perPeerDelay+500*time.Millisecond,
		"should return shortly after first ACK, got %v", elapsed)
}

func TestPeerSyncCacheStore_PushConcurrently_TimeoutWhenNoAck(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	const (
		numPeers       = 2
		perPeerDelay   = 5 * time.Second
		ackWaitTimeout = 100 * time.Millisecond
	)

	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(perPeerDelay)
		w.WriteHeader(http.StatusOK)
	}))
	defer peerServer.Close()

	peerHost, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())
	store := newPeerSyncCacheStore(logger, peerPortInt, "", nil)

	peers := make([]string, numPeers)
	for i := range peers {
		peers[i] = peerHost
	}

	compressed, err := gzipCompress(cacheBytes(t, testCacheWithData(t)))
	require.NoError(t, err)
	client := &http.Client{Timeout: pushPeerTimeout}

	start := time.Now()
	acked, timedOut := store.pushConcurrently(context.Background(), client, peers, compressed, 1, ackWaitTimeout)
	elapsed := time.Since(start)

	assert.Equal(t, 0, acked, "no peer should ACK within the wait timeout")
	assert.True(t, timedOut)
	assert.Less(t, elapsed, ackWaitTimeout+200*time.Millisecond,
		"should return shortly after wait timeout, got %v", elapsed)
}

// --- Push data integrity ---

func TestPeerSyncCacheStore_PushToPeer_DataIntegrity(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	// Use a real peer store as the receiver to verify end-to-end data flow.
	receiver := newPeerSyncCacheStore(logger, 0, "", nil)
	peerServer := httptest.NewServer(http.HandlerFunc(receiver.handleSyncCache))
	defer peerServer.Close()

	peerHost, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())
	sender := newPeerSyncCacheStore(logger, peerPortInt, "", nil)

	data := cacheBytes(t, testCacheWithData(t))

	compressed, err := gzipCompress(data)
	require.NoError(t, err)

	client := &http.Client{Timeout: pushPeerTimeout}
	sender.pushToPeer(context.Background(), client, peerHost, compressed)

	// Receiver should now be able to Load the pushed data.
	loaded, err := receiver.Load(context.Background())
	require.NoError(t, err)

	require.Len(t, loaded.Cache.CRDs, 1)
	assert.Equal(t, "widgets.example.com", loaded.Cache.CRDs["widgets.example.com"].GetName())
	require.Len(t, loaded.Cache.CRs, 1)

	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	key := crResourceKey(gvr, "default", "my-widget")
	assert.Equal(t, "my-widget", loaded.Cache.CRs[key].Obj.GetName())
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
	store := newPeerSyncCacheStore(logger, peerPortInt, "", nil)

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

	store := newPeerSyncCacheStore(logger, defaultPeerPort, "this-dns-does-not-exist.invalid", nil)

	data := store.pullFromPeers(context.Background(), 1*time.Second)
	assert.Nil(t, data)
}

// --- GET handler serves data correctly ---

func TestPeerSyncCacheStore_HandleGet_ServesGzippedData(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)
	store := newPeerSyncCacheStore(logger, 0, "", nil)

	err := store.Save(context.Background(), msg(testCacheWithData(t)))
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

	restored, err := unmarshalPeerSyncMessage(data)
	require.NoError(t, err)
	assert.Len(t, restored.Cache.CRDs, 1)
	assert.Len(t, restored.Cache.CRs, 1)
}
