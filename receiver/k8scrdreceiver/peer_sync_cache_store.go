package k8scrdreceiver

// Consistency model: the leader broadcasts per-cycle deltas to all replicas
// concurrently and waits up to broadcastAckTimeout for every peer to ACK before
// proceeding (best-effort fallback on timeout). Newly started replicas Bootstrap
// from the current leader to obtain a baseline cache; deltas that arrive during
// Bootstrap are buffered and drained on completion.

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/types"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// syncIncrementsPath is the leader -> peer push endpoint. POST only. Carries a
	// PeerSyncDelta with the per-cycle changes.
	syncIncrementsPath = "/sync/increments"

	// syncSnapshotPath is the bootstrap pull endpoint. GET only, served only by the
	// current leader.
	syncSnapshotPath = "/sync/snapshot"

	defaultPeerPort = 4319

	// snapshotAppliedAtHeader carries the leader's wall-clock time at the moment the
	// snapshot was taken. Secondaries use this to discard buffered deltas with a
	// timestamp older than the snapshot.
	snapshotAppliedAtHeader = "X-Snapshot-AppliedAt"

	pushMaxRetries    = 3
	pushRetryBaseWait = 100 * time.Millisecond
	pushPeerTimeout   = 2 * time.Second

	// broadcastAckTimeout caps how long ApplyDelta blocks waiting for every peer to
	// ACK. On timeout the broadcast is recorded as failed but ApplyDelta still returns
	// (best-effort emission so the platform keeps receiving updates).
	broadcastAckTimeout = 500 * time.Millisecond

	// broadcastFailureErrorThreshold is the number of consecutive failed broadcasts
	// before escalating the log level to Warn so operators notice peer sync is broken.
	broadcastFailureErrorThreshold = 5

	// Bootstrap pull retry parameters. The pull is retried with exponential backoff
	// until the leader becomes reachable or the overall deadline passes. On timeout
	// we fall back to cold-start behaviour (empty cache + drain whatever buffered).
	bootstrapPullTimeout = 3 * time.Second
	bootstrapBaseBackoff = 200 * time.Millisecond
	bootstrapMaxBackoff  = 5 * time.Second
	bootstrapMaxDuration = 30 * time.Second

	// deltaBufferMaxSize caps how many incoming deltas are queued while Bootstrap
	// is in progress. With a 10s increment interval and at most 2 deltas per cycle
	// (the two-phase split), a 30s bootstrap window only produces ~6 deltas; 50
	// leaves generous headroom for split-brain leader writes during failover.
	deltaBufferMaxSize = 50
)

// peerSyncCacheStore is the live owner of the synchronised resource cache plus the
// HTTP transport for peer sync. The leader applies changes locally and broadcasts a
// PeerSyncDelta to peers; secondaries apply received deltas to their copy.
type peerSyncCacheStore struct {
	logger   *zap.Logger
	syncPort int
	peerDNS  string
	metrics  metrics.Recorder

	// cacheMu guards cache and lastSnapshotTime.
	cacheMu          sync.RWMutex
	cache            *resourceCache
	lastSnapshotTime time.Time

	// isLeader gates the snapshot endpoint: only the current leader serves snapshots
	// (peer caches can be stale). Set via SetLeader from the leader-election callbacks.
	isLeader atomic.Bool

	// bufferMu serialises access to ready and deltaBuffer. Held by handleIncrement
	// across "check ready + apply OR buffer" and by Bootstrap when flipping ready,
	// so the transition can't race with an in-flight delta application. Initial
	// value of ready is true: until Bootstrap explicitly enters its window, deltas
	// apply directly.
	bufferMu    sync.Mutex
	ready       bool
	deltaBuffer []*PeerSyncDelta

	// consecutiveBroadcastFailures tracks how many broadcasts in a row failed to reach any peer.
	// Reset to zero on the first successful broadcast.
	consecutiveBroadcastFailures atomic.Int32

	server *http.Server
	wg     sync.WaitGroup
}

// SetLeader records whether this replica is the current leader.
func (p *peerSyncCacheStore) SetLeader(leader bool) {
	p.isLeader.Store(leader)
}

var _ PeerStore = (*peerSyncCacheStore)(nil)

func newPeerSyncCacheStore(logger *zap.Logger, syncPort int, peerDNS string, rec metrics.Recorder) *peerSyncCacheStore {
	if rec == nil {
		rec = metrics.NoopRecorder{}
	}
	return &peerSyncCacheStore{
		logger:   logger,
		syncPort: syncPort,
		peerDNS:  peerDNS,
		metrics:  rec,
		cache:    newResourceCache(),
		ready:    true,
	}
}

// Start launches the HTTP server. Must be called before any other method and
// regardless of leadership — secondaries also need the listener up so the leader
// can push deltas to them.
func (p *peerSyncCacheStore) Start(_ context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc(syncIncrementsPath, p.handleIncrement)
	mux.HandleFunc(syncSnapshotPath, p.handleSnapshot)

	p.server = &http.Server{
		Addr:              fmt.Sprintf("0.0.0.0:%d", p.syncPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ln, err := net.Listen("tcp", p.server.Addr)
	if err != nil {
		return fmt.Errorf("peer sync listen on %s: %w", p.server.Addr, err)
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if err := p.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			p.logger.Error("Peer sync HTTP server error", zap.Error(err))
		}
	}()

	p.logger.Info("Peer sync cache store started",
		zap.Int("port", p.syncPort),
		zap.String("peer_dns", p.peerDNS),
	)

	return nil
}

// Stop shuts down the HTTP server and waits for in-flight requests.
func (p *peerSyncCacheStore) Stop() {
	if p.server != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := p.server.Shutdown(shutdownCtx); err != nil {
			p.logger.Debug("Peer sync HTTP server shutdown error", zap.Error(err))
		}
	}
	p.wg.Wait()
	p.logger.Info("Peer sync cache store stopped")
}

// --- PeerStore interface ---

// Bootstrap pulls a snapshot from the leader and applies it to the local cache.
// Retries with exponential backoff until either a snapshot is obtained or the
// overall deadline (bootstrapMaxDuration) passes — in which case we fall back
// to cold start with whatever was buffered.
//
// While Bootstrap runs, ready is false: incoming deltas (via handleIncrement) go
// into deltaBuffer. On exit, the buffer is drained — applying only deltas with
// AppliedAt at or after the snapshot's reference time — and ready flips back to
// true. Subsequent deltas apply directly.
func (p *peerSyncCacheStore) Bootstrap(ctx context.Context) error {
	if !p.cache.isEmpty() {
		// Already populated (e.g., a previous Bootstrap or local leader writes).
		return nil
	}

	// Mark not-ready so concurrent handleIncrement calls buffer.
	p.bufferMu.Lock()
	p.ready = false
	p.bufferMu.Unlock()

	p.logger.Info("Bootstrapping peer cache from leader")
	snap, outcome := p.pullSnapshotWithRetry(ctx)
	p.metrics.RecordBootstrap(ctx, outcome)
	switch outcome {
	case types.BootstrapApplied:
		// Successful pull; snap is non-nil and will populate the cache below.
	case types.BootstrapLeaderEmpty:
		p.logger.Info("Leader has no data, starting fresh")
	case types.BootstrapTimedOut:
		// Cache stays empty; resources existing before this replica started won't
		// appear on the platform until the next snapshot cycle.
		p.logger.Warn("Bootstrap timed out, secondary cache may be incomplete until next snapshot",
			zap.Duration("max_duration", bootstrapMaxDuration),
		)
	}
	p.completeBootstrap(snap)
	return nil
}

// pullSnapshotWithRetry attempts to fetch a snapshot until success, the deadline
// passes, or a peer authoritatively says it has no data. Returns (snapshot, outcome):
// the outcome distinguishes "leader had data" from "leader empty" from "timed out"
// so the caller can pick the right log level and metric.
func (p *peerSyncCacheStore) pullSnapshotWithRetry(ctx context.Context) (*PeerSyncSnapshot, types.BootstrapOutcome) {
	deadline := time.Now().Add(bootstrapMaxDuration)
	backoff := bootstrapBaseBackoff

	for {
		snap, leaderEmpty := p.pullSnapshotFromPeers(ctx, bootstrapPullTimeout)
		if snap != nil {
			return snap, types.BootstrapApplied
		}
		if leaderEmpty {
			return nil, types.BootstrapLeaderEmpty
		}

		if time.Now().After(deadline) {
			return nil, types.BootstrapTimedOut
		}

		select {
		case <-ctx.Done():
			return nil, types.BootstrapTimedOut
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, bootstrapMaxBackoff)
	}
}

// completeBootstrap applies the (optional) snapshot, drains the buffered deltas
// (filtering out any that predate the snapshot), and flips ready=true. Holds both
// locks for the duration so the transition is atomic w.r.t. handleIncrement.
func (p *peerSyncCacheStore) completeBootstrap(snapshot *PeerSyncSnapshot) {
	p.bufferMu.Lock()
	defer p.bufferMu.Unlock()

	p.cacheMu.Lock()
	if snapshot != nil {
		p.cache = snapshot.Cache
		p.lastSnapshotTime = snapshot.LastSnapshotTime
		p.logger.Info("Applied bootstrap snapshot",
			zap.Int("crds", len(snapshot.Cache.CRDs)),
			zap.Int("crs", len(snapshot.Cache.CRs)),
			zap.Time("last_snapshot_time", snapshot.LastSnapshotTime),
		)
	}

	snapshotTime := p.lastSnapshotTime
	var applied, skipped int
	for _, delta := range p.deltaBuffer {
		if delta.AppliedAt.Before(snapshotTime) {
			skipped++
			continue
		}
		p.cache.applyDelta(delta.Changes)
		if !delta.LastSnapshotTime.IsZero() {
			p.lastSnapshotTime = delta.LastSnapshotTime
		}
		applied++
	}
	bufferedCount := len(p.deltaBuffer)
	p.deltaBuffer = nil
	p.cacheMu.Unlock()

	p.ready = true

	if bufferedCount > 0 {
		p.logger.Info("Drained delta buffer after Bootstrap",
			zap.Int("buffered", bufferedCount),
			zap.Int("applied", applied),
			zap.Int("skipped_predates_snapshot", skipped),
		)
	}
}

// ComputeChanges returns the delta between the cache and current informer state.
func (p *peerSyncCacheStore) ComputeChanges(
	currentCRDs []*unstructured.Unstructured,
	currentCRs map[schema.GroupVersionResource][]*unstructured.Unstructured,
) []ResourceChange {
	p.cacheMu.RLock()
	defer p.cacheMu.RUnlock()
	return p.cache.computeChanges(currentCRDs, currentCRs)
}

// ApplyDelta applies the delta to the cache and broadcasts to peers.
//
// LastSnapshotTime semantics: only updated when the caller passes a non-zero value
// (snapshot cycles set it; increment cycles leave it zero). Either way, the broadcast
// payload is filled with the store's current value so peers always receive a coherent
// snapshot timestamp.
func (p *peerSyncCacheStore) ApplyDelta(ctx context.Context, delta *PeerSyncDelta) error {
	if delta == nil {
		return nil
	}

	p.cacheMu.Lock()
	p.cache.applyDelta(delta.Changes)
	if !delta.LastSnapshotTime.IsZero() {
		p.lastSnapshotTime = delta.LastSnapshotTime
	}
	delta.LastSnapshotTime = p.lastSnapshotTime
	crds, crs := len(p.cache.CRDs), len(p.cache.CRs)
	p.cacheMu.Unlock()

	p.metrics.RecordCacheSize(ctx, types.KindCRD, int64(crds))
	p.metrics.RecordCacheSize(ctx, types.KindCR, int64(crs))

	p.logger.Debug("Applied delta",
		zap.Int("changes", len(delta.Changes)),
		zap.Int("crds", crds),
		zap.Int("crs", crs),
	)

	if len(delta.Changes) == 0 {
		// Nothing to broadcast; skip the network round-trip.
		return nil
	}

	data, err := marshalPeerSyncDelta(delta)
	if err != nil {
		return fmt.Errorf("marshal delta: %w", err)
	}
	p.broadcastToPeers(ctx, data)
	return nil
}

// IsEmpty returns true when the cache contains no CRDs or CRs.
func (p *peerSyncCacheStore) IsEmpty() bool {
	p.cacheMu.RLock()
	defer p.cacheMu.RUnlock()
	return p.cache.isEmpty()
}

// cacheSize returns the count of CRDs and CRs currently cached. Internal helper —
// callers go through ApplyDelta which records the size as a side effect.
func (p *peerSyncCacheStore) cacheSize() (int, int) {
	p.cacheMu.RLock()
	defer p.cacheMu.RUnlock()
	return len(p.cache.CRDs), len(p.cache.CRs)
}

// LastSnapshotTime returns the leader's clock at the most recently applied snapshot.
func (p *peerSyncCacheStore) LastSnapshotTime() time.Time {
	p.cacheMu.RLock()
	defer p.cacheMu.RUnlock()
	return p.lastSnapshotTime
}

// --- HTTP handler ---

// handleIncrement accepts a leader push of cache updates. POST only.
func (p *peerSyncCacheStore) handleIncrement(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	p.handlePost(w, r)
}

// handleSnapshot serves the leader's cache to a peer that's bootstrapping. Non-leader
// replicas return 503 — peer caches can be stale (their last delta may have been missed)
// so only the lease-holder is authoritative.
//
// The body is newline-delimited JSON: one meta frame followed by one frame per CRD
// and CR entry. Each frame is encoded and written through gzip immediately; nothing
// buffers the full marshaled snapshot. To keep cacheMu hold short, the cache maps
// are shallow-copied first, then encoded lock-free.
func (p *peerSyncCacheStore) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !p.isLeader.Load() {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}

	crdsCopy, crsCopy, lastSnapshotTime, isEmpty := p.snapshotCopyForServe()
	if isEmpty {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Encoding", "gzip")
	// Used by secondaries to filter buffered deltas that predate the snapshot.
	w.Header().Set(snapshotAppliedAtHeader, lastSnapshotTime.UTC().Format(time.RFC3339Nano))

	gz := gzip.NewWriter(w)
	defer func() {
		if err := gz.Close(); err != nil {
			p.logger.Debug("Failed to close gzip writer", zap.Error(err))
		}
	}()
	enc := json.NewEncoder(gz)

	if err := enc.Encode(peerSyncStreamFrame{
		Type:             streamFrameMeta,
		LastSnapshotTime: lastSnapshotTime,
	}); err != nil {
		// Body has started; client will see a truncated stream.
		p.metrics.RecordSnapshotStreamFailure(r.Context())
		p.logger.Error("Failed to encode snapshot meta frame; partial stream", zap.Error(err))
		return
	}
	for name, crd := range crdsCopy {
		if err := enc.Encode(peerSyncStreamFrame{
			Type: streamFrameCRD,
			Key:  name,
			Obj:  crd,
		}); err != nil {
			p.metrics.RecordSnapshotStreamFailure(r.Context())
			p.logger.Error("Failed to encode snapshot CRD frame; partial stream",
				zap.String("name", name), zap.Error(err))
			return
		}
	}
	for key, cr := range crsCopy {
		gvr := cr.GVR
		if err := enc.Encode(peerSyncStreamFrame{
			Type: streamFrameCR,
			Key:  key,
			Obj:  cr.Obj,
			GVR:  &gvr,
		}); err != nil {
			p.metrics.RecordSnapshotStreamFailure(r.Context())
			p.logger.Error("Failed to encode snapshot CR frame; partial stream",
				zap.String("key", key), zap.Error(err))
			return
		}
	}
}

// snapshotCopyForServe takes a shallow copy of the cache maps under read lock so
// the caller can iterate them lock-free. Resource values are shared (immutable
// post-insertion) — only the map structure is duplicated.
func (p *peerSyncCacheStore) snapshotCopyForServe() (
	map[string]*unstructured.Unstructured,
	map[string]*cachedCR,
	time.Time,
	bool,
) {
	p.cacheMu.RLock()
	defer p.cacheMu.RUnlock()
	if p.cache.isEmpty() {
		return nil, nil, time.Time{}, true
	}
	crds := make(map[string]*unstructured.Unstructured, len(p.cache.CRDs))
	for k, v := range p.cache.CRDs {
		crds[k] = v
	}
	crs := make(map[string]*cachedCR, len(p.cache.CRs))
	for k, v := range p.cache.CRs {
		crs[k] = v
	}
	return crds, crs, p.lastSnapshotTime, false
}

func (p *peerSyncCacheStore) handlePost(w http.ResponseWriter, r *http.Request) {
	var reader io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gr, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, "invalid gzip body", http.StatusBadRequest)
			return
		}
		defer func() {
			if err := gr.Close(); err != nil {
				p.logger.Debug("Failed to close gzip reader", zap.Error(err))
			}
		}()
		reader = gr
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	if len(data) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	delta, err := unmarshalPeerSyncDelta(data)
	if err != nil {
		http.Error(w, "invalid peer sync delta", http.StatusBadRequest)
		return
	}

	// Hold bufferMu across the ready check + apply so a concurrent Bootstrap
	// can't flip ready and overwrite the cache between them.
	p.bufferMu.Lock()
	if !p.ready {
		if len(p.deltaBuffer) >= deltaBufferMaxSize {
			p.deltaBuffer = p.deltaBuffer[1:]
			p.logger.Warn("Delta buffer full during bootstrap, dropped oldest",
				zap.Int("max_size", deltaBufferMaxSize),
			)
		}
		p.deltaBuffer = append(p.deltaBuffer, delta)
		p.bufferMu.Unlock()
		w.WriteHeader(http.StatusOK)
		return
	}

	p.cacheMu.Lock()
	p.cache.applyDelta(delta.Changes)
	if !delta.LastSnapshotTime.IsZero() {
		p.lastSnapshotTime = delta.LastSnapshotTime
	}
	crds, crs := len(p.cache.CRDs), len(p.cache.CRs)
	p.cacheMu.Unlock()
	p.bufferMu.Unlock()

	p.metrics.RecordCacheSize(r.Context(), types.KindCRD, int64(crds))
	p.metrics.RecordCacheSize(r.Context(), types.KindCR, int64(crs))

	p.logger.Debug("Applied delta from leader",
		zap.Int("changes", len(delta.Changes)),
		zap.Int("crds", crds),
		zap.Int("crs", crs),
	)
	w.WriteHeader(http.StatusOK)
}

// --- Push broadcast ---

// broadcastToPeers resolves peer IPs via DNS and pushes cache data to each (excluding self).
func (p *peerSyncCacheStore) broadcastToPeers(ctx context.Context, data []byte) {
	if p.peerDNS == "" {
		return
	}

	ips, err := net.LookupHost(p.peerDNS)
	if err != nil {
		p.logger.Debug("Failed to resolve peer DNS for broadcast",
			zap.String("dns", p.peerDNS),
			zap.Error(err),
		)
		p.recordBroadcastFailure(types.BroadcastFailureDNSLookup)
		return
	}

	podIP := os.Getenv("POD_IP")
	peers := make([]string, 0, len(ips))
	for _, ip := range ips {
		if ip == podIP {
			continue
		}
		peers = append(peers, ip)
	}

	// No peers to push to (single replica or only self resolved). Not a failure.
	if len(peers) == 0 {
		return
	}

	compressed, err := gzipCompress(data)
	if err != nil {
		p.logger.Warn("Failed to gzip cache data for broadcast", zap.Error(err))
		p.recordBroadcastFailure(types.BroadcastFailureGzip)
		return
	}

	client := &http.Client{Timeout: pushPeerTimeout}
	acked, timedOut := p.pushConcurrently(ctx, client, peers, compressed, len(peers), broadcastAckTimeout)

	switch {
	case acked == len(peers):
		p.recordBroadcastSuccess()
	case timedOut:
		p.recordBroadcastFailure(types.BroadcastFailureAckTimeout)
	default:
		p.recordBroadcastFailure(types.BroadcastFailureNoAcks)
	}
}

// pushConcurrently pushes to all peers in parallel and returns once `ackThreshold`
// peers have ACKed or `ackWaitTimeout` elapses. Push goroutines are tracked in the
// store's lifecycle WaitGroup so Stop() waits for them; the results channel is
// buffered to len(peers) so they never block on send after an early return.
//
// `ackThreshold <= 0` waits for all peers. `ackWaitTimeout <= 0` waits without timeout.
func (p *peerSyncCacheStore) pushConcurrently(
	ctx context.Context,
	client *http.Client,
	peers []string,
	compressed []byte,
	ackThreshold int,
	ackWaitTimeout time.Duration,
) (int, bool) {
	results := make(chan bool, len(peers))
	for _, ip := range peers {
		p.wg.Add(1)
		go func(peerIP string) {
			defer p.wg.Done()
			results <- p.pushToPeer(ctx, client, peerIP, compressed)
		}(ip)
	}

	var deadline <-chan time.Time
	if ackWaitTimeout > 0 {
		deadline = time.After(ackWaitTimeout)
	}

	var acked int
	for range len(peers) {
		select {
		case ok := <-results:
			if ok {
				acked++
				if ackThreshold > 0 && acked == ackThreshold {
					return acked, false
				}
			}
		case <-deadline:
			return acked, true
		}
	}
	return acked, false
}

func (p *peerSyncCacheStore) recordBroadcastSuccess() {
	prev := p.consecutiveBroadcastFailures.Swap(0)
	p.metrics.RecordPeerBroadcast(context.Background(), types.BroadcastSuccess, types.BroadcastFailureNone)
	if prev >= broadcastFailureErrorThreshold {
		p.logger.Info("Peer broadcast recovered after consecutive failures",
			zap.Int32("previous_failures", prev),
		)
	}
}

// recordBroadcastFailure increments the consecutive failure counter and emits a failure
// metric. Logs once at Error when the threshold is crossed; further occurrences are
// surfaced via the peer_broadcasts_total metric to avoid log spam from a chronically
// misconfigured peer DNS.
func (p *peerSyncCacheStore) recordBroadcastFailure(reason types.BroadcastFailureReason) {
	n := p.consecutiveBroadcastFailures.Add(1)
	p.metrics.RecordPeerBroadcast(context.Background(), types.BroadcastFailed, reason)
	if n == broadcastFailureErrorThreshold {
		p.logger.Error("Peer broadcast failing repeatedly — check peer DNS and network reachability",
			zap.String("reason", string(reason)),
			zap.String("peer_dns", p.peerDNS),
			zap.Int32("consecutive_failures", n),
		)
	}
}

// pushToPeer sends compressed cache data to a single peer with retries.
// Returns true if any attempt received a 200 OK. Records per-attempt outcomes,
// total push duration, and payload size as metrics for ops visibility.
func (p *peerSyncCacheStore) pushToPeer(ctx context.Context, client *http.Client, ip string, compressed []byte) bool {
	url := fmt.Sprintf("http://%s:%d%s", ip, p.syncPort, syncIncrementsPath)

	start := time.Now()
	defer func() {
		p.metrics.RecordPeerPushDuration(context.Background(), time.Since(start))
		p.metrics.RecordPeerPushBytes(context.Background(), int64(len(compressed)))
	}()

	backoff := pushRetryBaseWait
	for attempt := range pushMaxRetries {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
		}

		// Inner function so defer runs at the end of each iteration, closing the
		// response body before the next attempt.
		success, shouldRetry := func() (bool, bool) {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(compressed))
			if err != nil {
				p.logger.Debug("Failed to create push request", zap.String("peer", ip), zap.Error(err))
				p.metrics.RecordPeerPushAttempt(context.Background(), types.PushFailed, types.PushFailureRequestFailed)
				return false, false
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Content-Encoding", "gzip")

			resp, err := client.Do(req)
			if err != nil {
				reason := classifyPushError(err)
				p.logger.Debug("Failed to push cache to peer",
					zap.String("peer", ip),
					zap.Int("attempt", attempt+1),
					zap.String("reason", string(reason)),
					zap.Error(err),
				)
				p.metrics.RecordPeerPushAttempt(context.Background(), types.PushFailed, reason)
				return false, true
			}
			defer func() {
				if err := resp.Body.Close(); err != nil {
					p.logger.Debug("Failed to close response body", zap.String("peer", ip), zap.Error(err))
				}
			}()

			if resp.StatusCode == http.StatusOK {
				p.logger.Debug("Pushed cache to peer",
					zap.String("peer", ip),
					zap.Int("bytes", len(compressed)),
				)
				p.metrics.RecordPeerPushAttempt(context.Background(), types.PushSuccess, types.PushFailureNone)
				return true, false
			}

			p.logger.Debug("Peer rejected cache push",
				zap.String("peer", ip),
				zap.Int("status", resp.StatusCode),
				zap.Int("attempt", attempt+1),
			)
			p.metrics.RecordPeerPushAttempt(context.Background(), types.PushFailed, types.PushFailureHTTPStatus)
			return false, true
		}()

		if success {
			return true
		}
		if !shouldRetry {
			return false
		}
	}

	p.logger.Debug("Failed to push cache to peer after retries",
		zap.String("peer", ip),
		zap.Int("max_retries", pushMaxRetries),
	)
	return false
}

// classifyPushError maps a transport-layer error from client.Do into a metric reason.
// Timeouts (including context deadline) are distinguished from other transport errors so
// operators can tell whether the peer was slow vs. unreachable.
func classifyPushError(err error) types.PushFailureReason {
	if errors.Is(err, context.DeadlineExceeded) {
		return types.PushFailureTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return types.PushFailureTimeout
	}
	return types.PushFailureConnection
}

// --- Bootstrap pull ---

// pullSnapshotFromPeers walks the peers from DNS and asks each for a snapshot.
// Returns the first successful response. The two return values let the caller
// (Bootstrap) decide whether retrying is worthwhile:
//
//   - (snap, _)    — got a snapshot, use it.
//   - (nil, true)  — at least one peer said "I have no data" (HTTP 204): the leader
//     exists but is empty (cold cluster). No point retrying.
//   - (nil, false) — no peer was reachable, or all reachable peers replied "I'm not
//     leader" (HTTP 503): retry, since a leader may emerge.
func (p *peerSyncCacheStore) pullSnapshotFromPeers(
	ctx context.Context, timeout time.Duration,
) (*PeerSyncSnapshot, bool) {
	if p.peerDNS == "" {
		return nil, false
	}

	ips, err := net.LookupHost(p.peerDNS)
	if err != nil {
		p.logger.Debug("Failed to resolve peer DNS for pull",
			zap.String("dns", p.peerDNS),
			zap.Error(err),
		)
		return nil, false
	}

	podIP := os.Getenv("POD_IP")
	client := &http.Client{Timeout: timeout}
	leaderRespondedEmpty := false

	for _, ip := range ips {
		if ip == podIP {
			continue
		}

		fetched, status, err := p.fetchPeerSnapshot(ctx, client, ip)
		if err != nil {
			p.logger.Debug("Failed to pull snapshot from peer",
				zap.String("peer", ip),
				zap.Error(err),
			)
			continue
		}

		switch status {
		case http.StatusOK:
			crds, crs := 0, 0
			if fetched != nil && fetched.Cache != nil {
				crds, crs = len(fetched.Cache.CRDs), len(fetched.Cache.CRs)
			}
			p.logger.Debug("Pulled snapshot from peer",
				zap.String("peer", ip),
				zap.Int("crds", crds),
				zap.Int("crs", crs),
			)
			return fetched, false
		case http.StatusNoContent:
			leaderRespondedEmpty = true
		}
		// 503 / other: try next peer.
	}

	return nil, leaderRespondedEmpty
}

// fetchPeerSnapshot pulls a snapshot from a single peer and assembles it from a
// stream of NDJSON frames. Returns the assembled snapshot (only for 200), the
// HTTP status code, and any transport/decode error.
//
// Each frame is decoded one at a time and applied to a fresh cache directly —
// no intermediate buffer holds the full snapshot. Memory peak ≈ the assembled
// cache size (which becomes our cache state anyway), plus one frame's worth of
// transient decode buffer.
func (p *peerSyncCacheStore) fetchPeerSnapshot(
	ctx context.Context, client *http.Client, ip string,
) (*PeerSyncSnapshot, int, error) {
	url := fmt.Sprintf("http://%s:%d%s", ip, p.syncPort, syncSnapshotPath)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("create GET request for %s: %w", url, err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("GET %s: %w", url, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			p.logger.Debug("Failed to close response body", zap.String("url", url), zap.Error(err))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, nil
	}

	var reader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, resp.StatusCode, fmt.Errorf("gzip reader for %s: %w", url, err)
		}
		defer func() {
			if err := gr.Close(); err != nil {
				p.logger.Debug("Failed to close gzip reader", zap.String("url", url), zap.Error(err))
			}
		}()
		reader = gr
	}

	snap := &PeerSyncSnapshot{Cache: newResourceCache()}
	dec := json.NewDecoder(reader)
	for {
		var frame peerSyncStreamFrame
		if err := dec.Decode(&frame); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, resp.StatusCode, fmt.Errorf("decode frame from %s: %w", url, err)
		}
		switch frame.Type {
		case streamFrameMeta:
			snap.LastSnapshotTime = frame.LastSnapshotTime
		case streamFrameCRD:
			snap.Cache.CRDs[frame.Key] = frame.Obj
		case streamFrameCR:
			if frame.GVR == nil {
				return nil, resp.StatusCode, fmt.Errorf("CR frame for %q missing gvr", frame.Key)
			}
			snap.Cache.CRs[frame.Key] = &cachedCR{Obj: frame.Obj, GVR: *frame.GVR}
		default:
			p.logger.Debug("Unknown snapshot frame type, skipping",
				zap.String("type", string(frame.Type)),
				zap.String("peer", ip),
			)
		}
	}
	return snap, resp.StatusCode, nil
}

// --- Helpers ---

func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		return nil, errors.Join(err, gz.Close())
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
