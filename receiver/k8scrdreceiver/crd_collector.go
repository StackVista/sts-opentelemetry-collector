package k8scrdreceiver

import (
	"context"
	"sync"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/emit"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/metrics"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// crdCollector reads from Informers, compares against a resource cache,
// and emits deltas (ADDED/MODIFIED/DELETED) or periodic full snapshots as log records.
type crdCollector struct {
	logger   *zap.Logger
	config   *Config
	consumer consumer.Logs
	informer Informers
	metrics  metrics.Recorder

	// Resource cache for delta computation
	cache            *resourceCache
	lastSnapshotTime time.Time

	peerStore PeerStore

	// Lifecycle
	wg     sync.WaitGroup
	ctx    context.Context //nolint:containedctx
	cancel context.CancelFunc
}

func newCRDCollector(
	logger *zap.Logger,
	config *Config,
	cons consumer.Logs,
	informer Informers,
	peerStore PeerStore,
	rec metrics.Recorder,
) *crdCollector {
	if peerStore == nil {
		peerStore = &noopPeerStore{}
	}
	if rec == nil {
		rec = metrics.NoopRecorder{}
	}
	return &crdCollector{
		logger:    logger,
		config:    config,
		consumer:  cons,
		informer:  informer,
		peerStore: peerStore,
		metrics:   rec,
	}
}

func (c *crdCollector) Start(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)

	// Load cache from peer sync. On failure, start fresh — the first increment will be a full snapshot.
	loadedCache, err := c.peerStore.Load(ctx)
	if err != nil {
		c.logger.Debug("Failed to load resource cache from peers, starting fresh", zap.Error(err))
		c.cache = newResourceCache()
	} else {
		c.cache = loadedCache
		if !c.cache.isEmpty() {
			c.logger.Info("Loaded resource cache from peers",
				zap.Int("crds", len(c.cache.crds)),
				zap.Int("crs", len(c.cache.crs)),
			)
		}
	}

	if err := c.informer.Start(ctx); err != nil {
		return err
	}

	c.wg.Add(1)
	go c.runIncrementLoop()

	return nil
}

func (c *crdCollector) Shutdown(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}

	// Wait for the increment loop to exit before shutting down informers,
	// since the loop reads from informer caches.
	c.wg.Wait()

	// Save cache before stopping so peers have the latest state.
	if c.cache != nil {
		if err := c.peerStore.Save(ctx, c.cache); err != nil {
			c.logger.Debug("Failed to sync resource cache to peers on shutdown", zap.Error(err))
		}
	}

	if c.informer != nil {
		return c.informer.Shutdown(ctx)
	}

	return nil
}

// --- Consolidated increment loop ---

// runIncrementLoop is the single polling loop that reads informer caches, compares
// against the resource cache, and emits deltas or periodic snapshots.
func (c *crdCollector) runIncrementLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.IncrementInterval)
	defer ticker.Stop()

	// Run first increment immediately
	c.runIncrement()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.runIncrement()
		}
	}
}

// runIncrement reads the current state from informer caches, compares against the
// resource cache, and emits changes. Periodically emits full snapshots for TTL freshness.
func (c *crdCollector) runIncrement() {
	start := time.Now()
	currentCRDs := c.informer.ReadCRDs()
	currentCRs := c.informer.ReadCRs()

	if !c.config.IncludeInitialState && c.cache.isEmpty() {
		c.cache.update(currentCRDs, currentCRs)
		c.lastSnapshotTime = time.Now()
		c.logger.Info("Initial state loaded into resource cache (not emitting)",
			zap.Int("crds", len(currentCRDs)),
			zap.Int("cr_types", len(currentCRs)),
		)
		if err := c.peerStore.Save(c.ctx, c.cache); err != nil {
			c.logger.Debug("Failed to save cache after initial population", zap.Error(err))
		}
		c.recordCacheSize()
		return
	}

	isSnapshot := c.cache.isEmpty() || time.Since(c.lastSnapshotTime) >= c.config.SnapshotInterval

	mode := metrics.ModeIncrement
	hasChanges := true
	if isSnapshot {
		mode = metrics.ModeSnapshot
		c.emitSnapshot(currentCRDs, currentCRs)
		c.lastSnapshotTime = time.Now()
	} else {
		hasChanges = c.emitIncrement(currentCRDs, currentCRs)
	}

	c.cache.update(currentCRDs, currentCRs)

	if hasChanges {
		if err := c.peerStore.Save(c.ctx, c.cache); err != nil {
			c.logger.Debug("Failed to save cache after increment", zap.Error(err))
		}
	}

	c.recordCacheSize()
	c.metrics.RecordCycle(c.ctx, mode, time.Since(start))
}

// emitSnapshot emits all current resources as ADDED for downstream TTL freshness,
// and emits DELETED for resources that were in the resource cache but are no longer present.
func (c *crdCollector) emitSnapshot(
	currentCRDs []*unstructured.Unstructured,
	currentCRs map[schema.GroupVersionResource][]*unstructured.Unstructured,
) {
	var crdAdded, crAdded, crdDeleted, crDeleted int64

	currentCRDNames := make(map[string]struct{}, len(currentCRDs))
	for _, crd := range currentCRDs {
		currentCRDNames[crd.GetName()] = struct{}{}
		if err := emit.Log(
			c.ctx, c.consumer, crd, watch.Added, c.config.ClusterName, emit.BuildCRDLogRecord,
		); err != nil {
			c.logger.Debug("Failed to emit CRD snapshot log",
				zap.String("name", crd.GetName()),
				zap.Error(err),
			)
			continue
		}
		crdAdded++
	}

	currentCRKeys := make(map[string]struct{})
	for gvr, crs := range currentCRs {
		for _, cr := range crs {
			currentCRKeys[crResourceKey(gvr, cr.GetNamespace(), cr.GetName())] = struct{}{}
			if err := emit.Log(
				c.ctx, c.consumer, cr, watch.Added, c.config.ClusterName, emit.BuildCRLogRecord,
			); err != nil {
				c.logger.Debug("Failed to emit CR snapshot log",
					zap.String("gvr", emit.FormatGVRKey(gvr)),
					zap.String("name", cr.GetName()),
					zap.Error(err),
				)
				continue
			}
			crAdded++
		}
	}

	for name, cachedCRD := range c.cache.crds {
		if _, exists := currentCRDNames[name]; !exists {
			if err := emit.Log(
				c.ctx, c.consumer, cachedCRD, watch.Deleted, c.config.ClusterName, emit.BuildCRDLogRecord,
			); err != nil {
				c.logger.Debug("Failed to emit CRD deletion in snapshot",
					zap.String("name", cachedCRD.GetName()),
					zap.Error(err),
				)
				continue
			}
			crdDeleted++
		}
	}

	for key, cached := range c.cache.crs {
		if _, exists := currentCRKeys[key]; !exists {
			if err := emit.Log(
				c.ctx, c.consumer, cached.obj, watch.Deleted, c.config.ClusterName, emit.BuildCRLogRecord,
			); err != nil {
				c.logger.Debug("Failed to emit CR deletion in snapshot",
					zap.String("name", cached.obj.GetName()),
					zap.Error(err),
				)
				continue
			}
			crDeleted++
		}
	}

	c.metrics.RecordEmitted(c.ctx, metrics.ChangeAdded, crdAdded+crAdded)
	c.metrics.RecordEmitted(c.ctx, metrics.ChangeDeleted, crdDeleted+crDeleted)

	c.logger.Debug("Snapshot complete",
		zap.Int64("crds", crdAdded),
		zap.Int64("crs", crAdded),
		zap.Int64("deleted", crdDeleted+crDeleted),
	)
}

// emitIncrement computes the delta between resource cache and current state, and emits changes.
// Returns true if any changes were detected.
func (c *crdCollector) emitIncrement(
	currentCRDs []*unstructured.Unstructured,
	currentCRs map[schema.GroupVersionResource][]*unstructured.Unstructured,
) bool {
	changes := c.cache.computeChanges(currentCRDs, currentCRs)

	if len(changes) == 0 {
		return false
	}

	counts := map[metrics.ChangeType]int64{}
	for _, change := range changes {
		var err error
		if change.isCRD {
			err = emit.Log(c.ctx, c.consumer, change.obj, change.eventType, c.config.ClusterName, emit.BuildCRDLogRecord)
		} else {
			err = emit.Log(c.ctx, c.consumer, change.obj, change.eventType, c.config.ClusterName, emit.BuildCRLogRecord)
		}
		if err != nil {
			c.logger.Debug("Failed to emit change log",
				zap.String("event", string(change.eventType)),
				zap.String("name", change.obj.GetName()),
				zap.Error(err),
			)
			continue
		}
		counts[c.watchEventToChangeType(change.eventType)]++
	}

	for changeType, count := range counts {
		c.metrics.RecordEmitted(c.ctx, changeType, count)
	}

	c.logger.Debug("Increment complete",
		zap.Int("changes", len(changes)),
	)

	return true
}

func (c *crdCollector) recordCacheSize() {
	c.metrics.RecordCacheSize(c.ctx, metrics.KindCRD, int64(len(c.cache.crds)))
	c.metrics.RecordCacheSize(c.ctx, metrics.KindCR, int64(len(c.cache.crs)))
}

func (c *crdCollector) watchEventToChangeType(e watch.EventType) metrics.ChangeType {
	switch e {
	case watch.Added:
		return metrics.ChangeAdded
	case watch.Modified:
		return metrics.ChangeModified
	case watch.Deleted:
		return metrics.ChangeDeleted
	default:
		c.logger.Warn("Unexpected watch event type", zap.String("event", string(e)))
		return metrics.ChangeUnknown
	}
}
