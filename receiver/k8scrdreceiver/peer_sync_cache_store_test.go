//nolint:testpackage
package k8scrdreceiver

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"k8s.io/apimachinery/pkg/watch"
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

// deltaBytes marshals a cache as a delta payload containing all entries as ADDED.
// Used to feed handleIncrement / pushToPeer in tests.
func deltaBytes(t *testing.T, c *resourceCache) []byte {
	t.Helper()
	delta := &PeerSyncDelta{AppliedAt: time.Now()}
	for _, crd := range c.CRDs {
		delta.Changes = append(delta.Changes, ResourceChange{Obj: crd, EventType: watch.Added, IsCRD: true})
	}
	for _, cr := range c.CRs {
		delta.Changes = append(delta.Changes, ResourceChange{Obj: cr.Obj, EventType: watch.Added, GVR: cr.GVR})
	}
	data, err := marshalPeerSyncDelta(delta)
	require.NoError(t, err)
	return data
}

// seedCache directly installs c into store's cache, bypassing ApplyDelta.
// Used for setup in tests that want a pre-populated store.
func seedCache(store *peerSyncCacheStore, c *resourceCache) {
	store.cacheMu.Lock()
	store.cache = c
	store.cacheMu.Unlock()
}

func peerPort(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	return host, port
}

// --- Core ApplyDelta / cache state ---

func TestPeerSyncCacheStore_ApplyDelta_PopulatesCache(t *testing.T) {
	t.Parallel()
	store := newPeerSyncCacheStore(zaptest.NewLogger(t), 0, "", nil)

	delta := &PeerSyncDelta{AppliedAt: time.Now()}
	for _, crd := range testCacheWithData(t).CRDs {
		delta.Changes = append(delta.Changes, ResourceChange{Obj: crd, EventType: watch.Added, IsCRD: true})
	}
	for _, cr := range testCacheWithData(t).CRs {
		delta.Changes = append(delta.Changes, ResourceChange{Obj: cr.Obj, EventType: watch.Added, GVR: cr.GVR})
	}

	require.NoError(t, store.ApplyDelta(context.Background(), delta))

	crds, crs := store.cacheSize()
	assert.Equal(t, 1, crds)
	assert.Equal(t, 1, crs)
	assert.False(t, store.IsEmpty())
}

func TestPeerSyncCacheStore_IsEmpty_TrueByDefault(t *testing.T) {
	t.Parallel()
	store := newPeerSyncCacheStore(zaptest.NewLogger(t), 0, "", nil)
	assert.True(t, store.IsEmpty())
}

// --- HTTP handlers ---

func TestPeerSyncCacheStore_HandleSnapshot_Returns503WhenNotLeader(t *testing.T) {
	t.Parallel()
	store := newPeerSyncCacheStore(zaptest.NewLogger(t), 0, "", nil)
	seedCache(store, testCacheWithData(t))

	req := httptest.NewRequest(http.MethodGet, syncSnapshotPath, nil)
	w := httptest.NewRecorder()
	store.handleSnapshot(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestPeerSyncCacheStore_HandleSnapshot_Returns204WhenLeaderHasNoData(t *testing.T) {
	t.Parallel()
	store := newPeerSyncCacheStore(zaptest.NewLogger(t), 0, "", nil)
	store.SetLeader(true)

	req := httptest.NewRequest(http.MethodGet, syncSnapshotPath, nil)
	w := httptest.NewRecorder()
	store.handleSnapshot(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestPeerSyncCacheStore_HandleIncrement_AppliesReceivedDelta(t *testing.T) {
	t.Parallel()
	store := newPeerSyncCacheStore(zaptest.NewLogger(t), 0, "", nil)

	data := deltaBytes(t, testCacheWithData(t))
	req := httptest.NewRequest(http.MethodPost, syncIncrementsPath, strings.NewReader(string(data)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	store.handleIncrement(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	crds, crs := store.cacheSize()
	assert.Equal(t, 1, crds)
	assert.Equal(t, 1, crs)
}

func TestPeerSyncCacheStore_HandleIncrement_GzipCompressed(t *testing.T) {
	t.Parallel()
	store := newPeerSyncCacheStore(zaptest.NewLogger(t), 0, "", nil)

	compressed, err := gzipCompress(deltaBytes(t, testCacheWithData(t)))
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, syncIncrementsPath, strings.NewReader(string(compressed)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	store.handleIncrement(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	crds, _ := store.cacheSize()
	assert.Equal(t, 1, crds)
}

// --- Lifecycle ---

func TestPeerSyncCacheStore_StartStop(t *testing.T) {
	t.Parallel()
	store := newPeerSyncCacheStore(zaptest.NewLogger(t), 0, "", nil)
	require.NoError(t, store.Start(context.Background()))
	store.Stop()
}

// --- Bootstrap: pull from peers ---

func TestPeerSyncCacheStore_Bootstrap_PullsFromLeaderPeer(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	// Set up a peer that holds the leader role and has cached data.
	peer := newPeerSyncCacheStore(logger, 0, "", nil)
	peer.SetLeader(true)
	seedCache(peer, testCacheWithData(t))

	peerServer := httptest.NewServer(http.HandlerFunc(peer.handleSnapshot))
	defer peerServer.Close()

	_, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())

	// New store, empty cache — Bootstrap should pull from the peer.
	store := newPeerSyncCacheStore(logger, peerPortInt, "localhost", nil)
	require.NoError(t, store.Bootstrap(context.Background()))

	crds, crs := store.cacheSize()
	assert.Equal(t, 1, crds)
	assert.Equal(t, 1, crs)
}

func TestPeerSyncCacheStore_Bootstrap_StaysEmptyWhenAllPeersEmpty(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	peer := newPeerSyncCacheStore(logger, 0, "", nil)
	peer.SetLeader(true)
	peerServer := httptest.NewServer(http.HandlerFunc(peer.handleSnapshot))
	defer peerServer.Close()

	_, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())

	store := newPeerSyncCacheStore(logger, peerPortInt, "localhost", nil)
	require.NoError(t, store.Bootstrap(context.Background()))
	assert.True(t, store.IsEmpty())
}

func TestPeerSyncCacheStore_Bootstrap_NoOpWhenAlreadyPopulated(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	// Peer has different data than what's already in the store.
	peerCache := newResourceCache()
	peerCRD := makeTestCRDUnstructured("gadgets.example.com", "example.com", "Gadget", "gadgets")
	peerCache.CRDs["gadgets.example.com"] = peerCRD

	peer := newPeerSyncCacheStore(logger, 0, "", nil)
	peer.SetLeader(true)
	seedCache(peer, peerCache)

	peerServer := httptest.NewServer(http.HandlerFunc(peer.handleSnapshot))
	defer peerServer.Close()

	_, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())

	// Pre-populate store; Bootstrap should be a no-op.
	store := newPeerSyncCacheStore(logger, peerPortInt, "localhost", nil)
	seedCache(store, testCacheWithData(t))

	require.NoError(t, store.Bootstrap(context.Background()))
	store.cacheMu.RLock()
	assert.Contains(t, store.cache.CRDs, "widgets.example.com")
	assert.NotContains(t, store.cache.CRDs, "gadgets.example.com",
		"populated store should not be overwritten by bootstrap")
	store.cacheMu.RUnlock()
}

// --- Buffer + ready gating during Bootstrap ---

// TestPeerSyncCacheStore_BuffersDeltasDuringBootstrap puts the store into the
// "bootstrapping" state, sends two deltas (one before and one after the snapshot's
// AppliedAt), then completes bootstrap. The pre-snapshot delta is discarded; the
// post-snapshot delta is applied.
func TestPeerSyncCacheStore_BuffersDeltasDuringBootstrap(t *testing.T) {
	t.Parallel()
	store := newPeerSyncCacheStore(zaptest.NewLogger(t), 0, "", nil)

	// Put the store in the bootstrapping state directly.
	store.bufferMu.Lock()
	store.ready = false
	store.bufferMu.Unlock()

	// Send a delta that PREDATES the snapshot's reference time.
	oldDelta := makeAddDelta(t, "old.example.com", time.Now().Add(-1*time.Hour))
	postDeltaToStore(t, store, oldDelta)

	// Send a delta that POSTDATES the snapshot.
	newDelta := makeAddDelta(t, "new.example.com", time.Now().Add(1*time.Hour))
	postDeltaToStore(t, store, newDelta)

	// Both should be buffered, not applied.
	assert.True(t, store.IsEmpty())
	assert.Equal(t, 2, len(store.deltaBuffer))

	// Complete bootstrap with a snapshot whose reference time falls between the deltas.
	snap := &PeerSyncSnapshot{
		Cache:            newResourceCache(),
		LastSnapshotTime: time.Now(),
	}
	store.completeBootstrap(snap)

	// Only the post-snapshot delta should have been applied.
	store.cacheMu.RLock()
	defer store.cacheMu.RUnlock()
	assert.Contains(t, store.cache.CRDs, "new.example.com")
	assert.NotContains(t, store.cache.CRDs, "old.example.com",
		"delta predating the snapshot should be discarded on drain")
}

// TestPeerSyncCacheStore_Buffer_DropsOldestWhenFull verifies the bounded buffer
// behaviour: when the cap is reached, the oldest entry is dropped.
func TestPeerSyncCacheStore_Buffer_DropsOldestWhenFull(t *testing.T) {
	t.Parallel()
	store := newPeerSyncCacheStore(zaptest.NewLogger(t), 0, "", nil)
	store.bufferMu.Lock()
	store.ready = false
	store.bufferMu.Unlock()

	for i := 0; i <= deltaBufferMaxSize; i++ {
		postDeltaToStore(t, store, makeAddDelta(t, fmt.Sprintf("crd-%d.example.com", i), time.Now()))
	}

	store.bufferMu.Lock()
	assert.Equal(t, deltaBufferMaxSize, len(store.deltaBuffer))
	// The very first entry (crd-0) should have been dropped.
	for _, d := range store.deltaBuffer {
		require.Len(t, d.Changes, 1)
		assert.NotEqual(t, "crd-0.example.com", d.Changes[0].Obj.GetName())
	}
	store.bufferMu.Unlock()
}

// makeAddDelta builds a single-CRD ADDED delta with the given AppliedAt timestamp.
func makeAddDelta(t *testing.T, crdName string, appliedAt time.Time) *PeerSyncDelta {
	t.Helper()
	crd := makeTestCRDUnstructured(crdName, "example.com", "Foo", "foos")
	crd.SetResourceVersion("1")
	return &PeerSyncDelta{
		AppliedAt: appliedAt,
		Changes:   []ResourceChange{{Obj: crd, EventType: watch.Added, IsCRD: true}},
	}
}

// postDeltaToStore POSTs a delta to the store's increment endpoint.
func postDeltaToStore(t *testing.T, store *peerSyncCacheStore, delta *PeerSyncDelta) {
	t.Helper()
	body, err := marshalPeerSyncDelta(delta)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, syncIncrementsPath, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	store.handleIncrement(w, req)
	require.Equal(t, http.StatusOK, w.Code)
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

	data := deltaBytes(t, testCacheWithData(t))

	store.broadcastToPeers(context.Background(), data)

	assert.False(t, receivedPush, "should not push to self")
}

func TestPeerSyncCacheStore_BroadcastNoOpWithoutDNS(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	store := newPeerSyncCacheStore(logger, defaultPeerPort, "", nil)

	data := deltaBytes(t, testCacheWithData(t))

	// Should return immediately without panic when peerDNS is empty.
	store.broadcastToPeers(context.Background(), data)
}

func TestPeerSyncCacheStore_BroadcastHandlesDNSFailure(t *testing.T) {
	t.Parallel()
	logger := zaptest.NewLogger(t)

	store := newPeerSyncCacheStore(logger, defaultPeerPort, "this-dns-does-not-exist.invalid", nil)

	data := deltaBytes(t, testCacheWithData(t))

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

	compressed, err := gzipCompress(deltaBytes(t, testCacheWithData(t)))
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

	compressed, err := gzipCompress(deltaBytes(t, testCacheWithData(t)))
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

	compressed, err := gzipCompress(deltaBytes(t, testCacheWithData(t)))
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
	peerServer := httptest.NewServer(http.HandlerFunc(receiver.handleIncrement))
	defer peerServer.Close()

	peerHost, peerPortInt := peerPort(t, peerServer.Listener.Addr().String())
	sender := newPeerSyncCacheStore(logger, peerPortInt, "", nil)

	data := deltaBytes(t, testCacheWithData(t))

	compressed, err := gzipCompress(data)
	require.NoError(t, err)

	client := &http.Client{Timeout: pushPeerTimeout}
	sender.pushToPeer(context.Background(), client, peerHost, compressed)

	// Receiver should have applied the delta to its cache.
	receiver.cacheMu.RLock()
	defer receiver.cacheMu.RUnlock()
	require.Len(t, receiver.cache.CRDs, 1)
	assert.Equal(t, "widgets.example.com", receiver.cache.CRDs["widgets.example.com"].GetName())
	require.Len(t, receiver.cache.CRs, 1)

	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	key := crResourceKey(gvr, "default", "my-widget")
	assert.Equal(t, "my-widget", receiver.cache.CRs[key].Obj.GetName())
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

	snap, leaderEmpty := store.pullSnapshotFromPeers(context.Background(), 1*time.Second)
	assert.Nil(t, snap)
	assert.False(t, leaderEmpty)
}

// --- Snapshot endpoint serves data correctly ---

func TestPeerSyncCacheStore_HandleSnapshot_ServesNDJSONWithAppliedAtHeader(t *testing.T) {
	t.Parallel()
	store := newPeerSyncCacheStore(zaptest.NewLogger(t), 0, "", nil)
	store.SetLeader(true)
	seedCache(store, testCacheWithData(t))

	wantAppliedAt := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	require.NoError(t, store.ApplyDelta(context.Background(), &PeerSyncDelta{
		AppliedAt:        wantAppliedAt,
		LastSnapshotTime: wantAppliedAt,
	}))

	req := httptest.NewRequest(http.MethodGet, syncSnapshotPath, nil)
	w := httptest.NewRecorder()
	store.handleSnapshot(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/x-ndjson", resp.Header.Get("Content-Type"))
	assert.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))

	appliedAt, err := time.Parse(time.RFC3339Nano, resp.Header.Get(snapshotAppliedAtHeader))
	require.NoError(t, err, "X-Snapshot-AppliedAt header should be parseable")
	assert.True(t, wantAppliedAt.Equal(appliedAt),
		"appliedAt header should match the LastSnapshotTime set via ApplyDelta")

	gr, err := gzip.NewReader(resp.Body)
	require.NoError(t, err)
	defer gr.Close()

	dec := json.NewDecoder(gr)
	var metaCount, crdCount, crCount int
	for {
		var frame peerSyncStreamFrame
		if decodeErr := dec.Decode(&frame); decodeErr != nil {
			if errors.Is(decodeErr, io.EOF) {
				break
			}
			require.NoError(t, decodeErr)
		}
		switch frame.Type {
		case streamFrameMeta:
			metaCount++
			assert.True(t, wantAppliedAt.Equal(frame.LastSnapshotTime))
		case streamFrameCRD:
			crdCount++
		case streamFrameCR:
			crCount++
		}
	}
	assert.Equal(t, 1, metaCount, "exactly one meta frame")
	assert.Equal(t, 1, crdCount)
	assert.Equal(t, 1, crCount)
}
