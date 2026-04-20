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
	"time"

	"go.uber.org/zap"
)

const (
	syncCachePath   = "/sync/cache"
	defaultPeerPort = 4319

	pushMaxRetries    = 3
	pushRetryBaseWait = 100 * time.Millisecond
	pushPeerTimeout   = 2 * time.Second
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

	mu              sync.RWMutex
	syncedCacheData []byte

	server *http.Server
	wg     sync.WaitGroup
}

var _ PeerStore = (*peerSyncCacheStore)(nil)

func newPeerSyncCacheStore(logger *zap.Logger, syncPort int, peerDNS string) *peerSyncCacheStore {
	return &peerSyncCacheStore{
		logger:   logger,
		syncPort: syncPort,
		peerDNS:  peerDNS,
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

// Load returns the locally synced cache. If empty, attempts a one-shot pull from peers.
func (p *peerSyncCacheStore) Load(ctx context.Context) (*resourceCache, error) {
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
	return newResourceCache(), nil
}

// Save marshals the cache, stores it locally, and broadcasts to all peers via POST.
func (p *peerSyncCacheStore) Save(ctx context.Context, cache *resourceCache) error {
	data, err := marshalResourceCache(cache)
	if err != nil {
		return fmt.Errorf("marshal cache for peer sync: %w", err)
	}

	p.mu.Lock()
	p.syncedCacheData = data
	p.mu.Unlock()

	p.logger.Debug("Cache saved locally",
		zap.Int("bytes", len(data)),
		zap.Int("crds", len(cache.crds)),
		zap.Int("crs", len(cache.crs)),
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

	if _, err := unmarshalResourceCache(data); err != nil {
		http.Error(w, "invalid cache data", http.StatusBadRequest)
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
		return
	}

	podIP := os.Getenv("POD_IP")

	compressed, err := gzipCompress(data)
	if err != nil {
		p.logger.Warn("Failed to gzip cache data for broadcast", zap.Error(err))
		return
	}

	client := &http.Client{Timeout: pushPeerTimeout}

	for _, ip := range ips {
		if ip == podIP {
			continue
		}
		p.pushToPeer(ctx, client, ip, compressed)
	}
}

// pushToPeer sends compressed cache data to a single peer with retries.
func (p *peerSyncCacheStore) pushToPeer(ctx context.Context, client *http.Client, ip string, compressed []byte) {
	url := fmt.Sprintf("http://%s:%d%s", ip, p.syncPort, syncCachePath)

	backoff := pushRetryBaseWait
	for attempt := range pushMaxRetries {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(compressed))
		if err != nil {
			p.logger.Debug("Failed to create push request", zap.String("peer", ip), zap.Error(err))
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")

		resp, err := client.Do(req)
		if err != nil {
			p.logger.Debug("Failed to push cache to peer",
				zap.String("peer", ip),
				zap.Int("attempt", attempt+1),
				zap.Error(err),
			)
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
			return
		}

		p.logger.Debug("Peer rejected cache push",
			zap.String("peer", ip),
			zap.Int("status", resp.StatusCode),
			zap.Int("attempt", attempt+1),
		)
	}

	p.logger.Warn("Failed to push cache to peer after retries",
		zap.String("peer", ip),
		zap.Int("max_retries", pushMaxRetries),
	)
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

func (p *peerSyncCacheStore) unmarshalAndLog(data []byte, source string) (*resourceCache, error) {
	cache, err := unmarshalResourceCache(data)
	if err != nil {
		p.logger.Warn("Failed to unmarshal cache data, starting fresh",
			zap.String("source", source),
			zap.Error(err),
		)
		return newResourceCache(), nil
	}
	p.logger.Info("Loaded cache from "+source,
		zap.Int("crds", len(cache.crds)),
		zap.Int("crs", len(cache.crs)),
	)
	return cache, nil
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
