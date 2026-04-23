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

// crdCollector reads from Informers, compares against a resource resourceCache,
// and emits deltas (ADDED/MODIFIED/DELETED) or periodic full snapshots as log records.
type crdCollector struct {
	logger    *zap.Logger
	config    *Config
	consumer  consumer.Logs
	informers Informers
	metrics   metrics.Recorder

	// Resource resourceCache for delta computation
	resourceCache    *resourceCache
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
	informers Informers,
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
		informers: informers,
		peerStore: peerStore,
		metrics:   rec,
	}
}

func (c *crdCollector) Start(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)

	// Load state from peer sync. On failure, start fresh — the first increment will be a full snapshot.
	state, err := c.peerStore.Load(ctx)
	if err != nil || state == nil {
		c.logger.Debug("Failed to load peer sync state, starting fresh", zap.Error(err))
		c.resourceCache = newResourceCache()
	} else {
		c.resourceCache = state.Cache
		// Inherit the previous leader's snapshot timestamp so the next snapshot is
		// scheduled relative to cluster history rather than this leader's startup
		// time. This bounds the worst-case snapshot gap to one configured interval
		// even across failovers.
		c.lastSnapshotTime = state.LastSnapshotTime
		if !c.resourceCache.isEmpty() {
			c.logger.Info("Loaded peer sync state",
				zap.Int("crds", len(c.resourceCache.CRDs)),
				zap.Int("crs", len(c.resourceCache.CRs)),
				zap.Time("last_snapshot_time", c.lastSnapshotTime),
			)
		}
	}

	if err := c.informers.Start(ctx); err != nil {
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

	// Save state before stopping so peers have the latest cache and snapshot timestamp.
	if c.resourceCache != nil {
		if err := c.peerStore.Save(ctx, c.currentState()); err != nil {
			c.logger.Debug("Failed to sync peer state on shutdown", zap.Error(err))
		}
	}

	if c.informers != nil {
		return c.informers.Shutdown(ctx)
	}

	return nil
}

// currentState wraps the live cache and snapshot timestamp into a PeerSyncMessage for
// transmission to peers. Used at every Save() site to keep the protocol contract in
// one place.
func (c *crdCollector) currentState() *PeerSyncMessage {
	return &PeerSyncMessage{
		Cache:            c.resourceCache,
		LastSnapshotTime: c.lastSnapshotTime,
	}
}

// --- Consolidated increment loop ---

// runIncrementLoop is the single polling loop that reads informers caches, compares
// against the resource resourceCache, and emits deltas or periodic snapshots.
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

// runIncrement reads the current state from informers caches, compares against the
// resource resourceCache, and emits changes. Periodically emits full snapshots for TTL freshness.
func (c *crdCollector) runIncrement() {
	start := time.Now()
	currentCRDs := c.informers.ReadCRDs()
	currentCRs := c.informers.ReadCRs()

	if !c.config.IncludeInitialState && c.resourceCache.isEmpty() {
		c.resourceCache.update(currentCRDs, currentCRs)
		c.lastSnapshotTime = time.Now()
		c.logger.Info("Initial state loaded into resource cache (not emitting)",
			zap.Int("crds", len(currentCRDs)),
			zap.Int("cr_types", len(currentCRs)),
		)
		if err := c.peerStore.Save(c.ctx, c.currentState()); err != nil {
			c.logger.Debug("Failed to save state after initial population", zap.Error(err))
		}
		c.recordCacheSize()
		return
	}

	isSnapshot := c.resourceCache.isEmpty() || time.Since(c.lastSnapshotTime) >= c.config.SnapshotInterval

	mode := metrics.ModeIncrement
	if isSnapshot {
		mode = metrics.ModeSnapshot
		c.emitSnapshot(currentCRDs, currentCRs)
		c.lastSnapshotTime = time.Now()
		c.resourceCache.update(currentCRDs, currentCRs)
		if err := c.peerStore.Save(c.ctx, c.currentState()); err != nil {
			c.logger.Debug("Failed to save state after snapshot", zap.Error(err))
		}
	} else {
		c.emitIncrement(currentCRDs, currentCRs)
	}

	c.recordCacheSize()
	c.metrics.RecordCycle(c.ctx, mode, time.Since(start))
}

// emitSnapshot emits all current resources as ADDED for downstream TTL freshness,
// and emits DELETED for resources that were in the resource resourceCache but are no longer present.
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

	for name, cachedCRD := range c.resourceCache.CRDs {
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

	for key, cached := range c.resourceCache.CRs {
		if _, exists := currentCRKeys[key]; !exists {
			if err := emit.Log(
				c.ctx, c.consumer, cached.Obj, watch.Deleted, c.config.ClusterName, emit.BuildCRLogRecord,
			); err != nil {
				c.logger.Debug("Failed to emit CR deletion in snapshot",
					zap.String("name", cached.Obj.GetName()),
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

// emitIncrement computes the delta between the resourceCache and current state (from the informers) and emits
// changes using a two-phase ordering chosen to minimise the impact of a leader crash:
//
//   - Adds/Mods: save peers BEFORE emitting. If we crash between save and emit, the new
//     leader's peerCache matches its informers state and emits nothing — the platform briefly
//     misses the change but is reconciled by the next snapshot (worst case: snapshot interval).
//   - Deletes: emit BEFORE saving peers. If we crash between emit and save, the new
//     leader's peerCache still contains the deleted resource while its informers does not, so it
//     re-emits the DELETE (idempotent duplicate). This avoids a missed DELETE that would
//     otherwise linger on the platform until TTL expiry.
func (c *crdCollector) emitIncrement(
	currentCRDs []*unstructured.Unstructured,
	currentCRs map[schema.GroupVersionResource][]*unstructured.Unstructured,
) {
	changes := c.resourceCache.computeChanges(currentCRDs, currentCRs)
	if len(changes) == 0 {
		return
	}

	var adds, deletes []resourceChange
	for _, ch := range changes {
		if ch.eventType == watch.Deleted {
			deletes = append(deletes, ch)
		} else {
			adds = append(adds, ch)
		}
	}

	// Phase 1 — adds/mods: save first, then emit.
	if len(adds) > 0 {
		c.resourceCache.applyAdditions(adds)
		if err := c.peerStore.Save(c.ctx, c.currentState()); err != nil {
			c.logger.Debug("Failed to save state before emitting adds/mods", zap.Error(err))
		}
		c.emitChanges(adds)
	}

	// Phase 2 — deletes: emit first, then save.
	if len(deletes) > 0 {
		c.emitChanges(deletes)
		c.resourceCache.applyDeletions(deletes)
		if err := c.peerStore.Save(c.ctx, c.currentState()); err != nil {
			c.logger.Debug("Failed to save state after emitting deletes", zap.Error(err))
		}
	}

	c.logger.Debug("Increment complete", zap.Int("changes", len(changes)))
}

// emitChanges sends a list of changes to the consumer and records per-type counters.
func (c *crdCollector) emitChanges(changes []resourceChange) {
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
}

func (c *crdCollector) recordCacheSize() {
	c.metrics.RecordCacheSize(c.ctx, metrics.KindCRD, int64(len(c.resourceCache.CRDs)))
	c.metrics.RecordCacheSize(c.ctx, metrics.KindCR, int64(len(c.resourceCache.CRs)))
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
