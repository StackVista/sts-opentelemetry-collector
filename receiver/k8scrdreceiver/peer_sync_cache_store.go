package k8scrdreceiver

// Consistency model: eventually consistent. The leader pushes cache updates to all
// replicas with retries but does not block on ACKs. Replicas that miss pushes (e.g.,
// due to restarts) pull the latest cache from peers on leadership acquisition.

import (
	"bytes"
	"compress/gzip"
	"context"
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
	"go.uber.org/zap"
)

const (
	syncCachePath   = "/sync/cache"
	defaultPeerPort = 4319

	pushMaxRetries    = 3
	pushRetryBaseWait = 100 * time.Millisecond
	pushPeerTimeout   = 2 * time.Second

	// broadcastFailureErrorThreshold is the number of consecutive failed broadcasts
	// before escalating the log level to Warn so operators notice peer sync is broken.
	broadcastFailureErrorThreshold = 5
)

// peerSyncCacheStore implements PeerStore with push-primary, pull-failsafe peer sync.
//
// Save() marshals the cache, stores it locally, and broadcasts to all peer replicas
// via POST. Load() returns the locally stored cache, falling back to a one-shot GET
// pull from peers if no data has been received yet.
type peerSyncCacheStore struct {
	logger   *zap.Logger
	syncPort int
	peerDNS  string
	metrics  metrics.Recorder

	mu              sync.RWMutex
	syncedCacheData []byte

	// consecutiveBroadcastFailures tracks how many broadcasts in a row failed to reach any peer.
	// Reset to zero on the first successful broadcast.
	consecutiveBroadcastFailures atomic.Int32

	server *http.Server
	wg     sync.WaitGroup
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
	}
}

// Start launches the HTTP server. Must be called before Load/Save and regardless of leadership.
func (p *peerSyncCacheStore) Start(_ context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc(syncCachePath, p.handleSyncCache)

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

// Load returns the locally synced state. If empty, attempts a one-shot pull from peers.
func (p *peerSyncCacheStore) Load(ctx context.Context) (*PeerSyncMessage, error) {
	p.mu.RLock()
	data := p.syncedCacheData
	p.mu.RUnlock()

	if len(data) > 0 {
		return p.unmarshalAndLog(data, "peer sync")
	}

	p.logger.Info("No synced cache data, attempting pull from peers")
	if data := p.pullFromPeers(ctx, 3*time.Second); len(data) > 0 {
		p.mu.Lock()
		p.syncedCacheData = data
		p.mu.Unlock()
		return p.unmarshalAndLog(data, "peer pull")
	}

	p.logger.Info("No peers with cache data available, starting fresh")
	return &PeerSyncMessage{Cache: newResourceCache()}, nil
}

// Save marshals the state, stores it locally, and broadcasts to all peers via POST.
func (p *peerSyncCacheStore) Save(ctx context.Context, state *PeerSyncMessage) error {
	data, err := marshalPeerSyncMessage(state)
	if err != nil {
		return fmt.Errorf("marshal peer sync state: %w", err)
	}

	p.mu.Lock()
	p.syncedCacheData = data
	p.mu.Unlock()

	p.logger.Debug("Cache saved locally",
		zap.Int("bytes", len(data)),
		zap.Int("crds", len(state.Cache.CRDs)),
		zap.Int("crs", len(state.Cache.CRs)),
		zap.Time("last_snapshot_time", state.LastSnapshotTime),
	)

	p.broadcastToPeers(ctx, data)

	return nil
}

// --- HTTP handler ---

// handleSyncCache routes GET (pull) and POST (push) requests.
func (p *peerSyncCacheStore) handleSyncCache(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.handleGet(w)
	case http.MethodPost:
		p.handlePost(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (p *peerSyncCacheStore) handleGet(w http.ResponseWriter) {
	p.mu.RLock()
	data := p.syncedCacheData
	p.mu.RUnlock()

	if len(data) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Encoding", "gzip")

	gz := gzip.NewWriter(w)
	defer func() {
		if err := gz.Close(); err != nil {
			p.logger.Debug("Failed to close gzip writer", zap.Error(err))
		}
	}()

	if _, err := gz.Write(data); err != nil {
		p.logger.Debug("Failed to write gzip response", zap.Error(err))
	}
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

	if _, err := unmarshalPeerSyncMessage(data); err != nil {
		http.Error(w, "invalid peer sync envelope", http.StatusBadRequest)
		return
	}

	p.mu.Lock()
	p.syncedCacheData = data
	p.mu.Unlock()

	p.logger.Debug("Received cache push from leader", zap.Int("bytes", len(data)))
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
		p.recordBroadcastFailure("dns lookup failed")
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
		p.recordBroadcastFailure("gzip failed")
		return
	}

	client := &http.Client{Timeout: pushPeerTimeout}
	succeeded := p.pushConcurrently(ctx, client, peers, compressed)

	if succeeded > 0 {
		p.recordBroadcastSuccess()
	} else {
		p.recordBroadcastFailure("all peers unreachable")
	}
}

// pushConcurrently pushes to all peers in parallel; total wall-clock is max(per-peer)
// Returns the number of peers that ACKed.
func (p *peerSyncCacheStore) pushConcurrently(ctx context.Context, client *http.Client, peers []string, compressed []byte) int {
	results := make(chan bool, len(peers))
	var wg sync.WaitGroup
	for _, ip := range peers {
		wg.Add(1)
		go func(peerIP string) {
			defer wg.Done()
			results <- p.pushToPeer(ctx, client, peerIP, compressed)
		}(ip)
	}
	wg.Wait()
	close(results)

	var succeeded int
	for ok := range results {
		if ok {
			succeeded++
		}
	}
	return succeeded
}

// recordBroadcastSuccess resets the consecutive failure counter and emits a success metric.
// Logs an Info recovery message if the previous run had crossed the failure threshold.
func (p *peerSyncCacheStore) recordBroadcastSuccess() {
	prev := p.consecutiveBroadcastFailures.Swap(0)
	p.metrics.RecordPeerBroadcast(context.Background(), metrics.BroadcastSuccess)
	if prev >= broadcastFailureErrorThreshold {
		p.logger.Info("Peer broadcast recovered after consecutive failures",
			zap.Int32("previous_failures", prev),
		)
	}
}

// recordBroadcastFailure increments the consecutive failure counter and emits a failure metric.
// Logs once at Error level when the threshold is first crossed; further occurrences are
// surfaced via the peer_broadcasts_total metric to avoid log spam from a chronically
// misconfigured peer DNS.
func (p *peerSyncCacheStore) recordBroadcastFailure(reason string) {
	n := p.consecutiveBroadcastFailures.Add(1)
	p.metrics.RecordPeerBroadcast(context.Background(), metrics.BroadcastFailed)
	if n == broadcastFailureErrorThreshold {
		p.logger.Error("Peer broadcast failing repeatedly — check peer DNS and network reachability",
			zap.String("reason", reason),
			zap.String("peer_dns", p.peerDNS),
			zap.Int32("consecutive_failures", n),
		)
	}
}

// pushToPeer sends compressed cache data to a single peer with retries.
// Returns true if any attempt received a 200 OK. Records per-attempt outcomes,
// total push duration, and payload size as metrics for ops visibility.
func (p *peerSyncCacheStore) pushToPeer(ctx context.Context, client *http.Client, ip string, compressed []byte) bool {
	url := fmt.Sprintf("http://%s:%d%s", ip, p.syncPort, syncCachePath)

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

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(compressed))
		if err != nil {
			p.logger.Debug("Failed to create push request", zap.String("peer", ip), zap.Error(err))
			p.metrics.RecordPeerPushAttempt(context.Background(), metrics.PushFailed, metrics.PushFailureRequestFailed)
			return false
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
			p.metrics.RecordPeerPushAttempt(context.Background(), metrics.PushFailed, reason)
			continue
		}

		if err := resp.Body.Close(); err != nil {
			p.logger.Debug("Failed to close response body", zap.String("peer", ip), zap.Error(err))
		}

		if resp.StatusCode == http.StatusOK {
			p.logger.Debug("Pushed cache to peer",
				zap.String("peer", ip),
				zap.Int("bytes", len(compressed)),
			)
			p.metrics.RecordPeerPushAttempt(context.Background(), metrics.PushSuccess, metrics.PushFailureNone)
			return true
		}

		p.logger.Debug("Peer rejected cache push",
			zap.String("peer", ip),
			zap.Int("status", resp.StatusCode),
			zap.Int("attempt", attempt+1),
		)
		p.metrics.RecordPeerPushAttempt(context.Background(), metrics.PushFailed, metrics.PushFailureHTTPStatus)
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
func classifyPushError(err error) metrics.PushFailureReason {
	if errors.Is(err, context.DeadlineExceeded) {
		return metrics.PushFailureTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return metrics.PushFailureTimeout
	}
	return metrics.PushFailureConnection
}

// --- Pull failsafe ---

// pullFromPeers resolves peer IPs via DNS and pulls cache from the first peer that has it.
func (p *peerSyncCacheStore) pullFromPeers(ctx context.Context, timeout time.Duration) []byte {
	if p.peerDNS == "" {
		return nil
	}

	ips, err := net.LookupHost(p.peerDNS)
	if err != nil {
		p.logger.Debug("Failed to resolve peer DNS for pull",
			zap.String("dns", p.peerDNS),
			zap.Error(err),
		)
		return nil
	}

	podIP := os.Getenv("POD_IP")
	client := &http.Client{Timeout: timeout}

	for _, ip := range ips {
		if ip == podIP {
			continue
		}

		data, err := p.fetchPeerCache(ctx, client, ip)
		if err != nil {
			p.logger.Debug("Failed to pull cache from peer",
				zap.String("peer", ip),
				zap.Error(err),
			)
			continue
		}

		if len(data) > 0 {
			p.logger.Debug("Pulled cache from peer",
				zap.String("peer", ip),
				zap.Int("bytes", len(data)),
			)
			return data
		}
	}

	return nil
}

// fetchPeerCache fetches the cache from a single peer via GET. Returns nil for 204.
func (p *peerSyncCacheStore) fetchPeerCache(ctx context.Context, client *http.Client, ip string) ([]byte, error) {
	url := fmt.Sprintf("http://%s:%d%s", ip, p.syncPort, syncCachePath)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create GET request for %s: %w", url, err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			p.logger.Debug("Failed to close response body", zap.String("url", url), zap.Error(err))
		}
	}()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}

	var reader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("gzip reader for %s: %w", url, err)
		}
		defer func() {
			if err := gr.Close(); err != nil {
				p.logger.Debug("Failed to close gzip reader", zap.String("url", url), zap.Error(err))
			}
		}()
		reader = gr
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read body from %s: %w", url, err)
	}

	return data, nil
}

// --- Helpers ---

func (p *peerSyncCacheStore) unmarshalAndLog(data []byte, source string) (*PeerSyncMessage, error) {
	state, err := unmarshalPeerSyncMessage(data)
	if err != nil {
		p.logger.Warn("Failed to unmarshal peer sync envelope, starting fresh",
			zap.String("source", source),
			zap.Error(err),
		)
		return &PeerSyncMessage{Cache: newResourceCache()}, nil
	}
	p.logger.Info("Loaded cache from "+source,
		zap.Int("crds", len(state.Cache.CRDs)),
		zap.Int("crs", len(state.Cache.CRs)),
		zap.Time("last_snapshot_time", state.LastSnapshotTime),
	)
	return state, nil
}

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
