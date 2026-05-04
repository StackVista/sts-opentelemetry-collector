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

// crdCollector reads from Informers, compares against the peerStore's cache, and
// emits deltas (ADDED/MODIFIED/DELETED) or periodic full snapshots as log records.
// Cache state lives in peerStore - the collector is a stateless worker.
type crdCollector struct {
	logger    *zap.Logger
	config    *Config
	consumer  consumer.Logs
	informers Informers
	metrics   metrics.Recorder

	peerStore PeerStore

	// Lifecycle
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func newCRDCollector(
	logger *zap.Logger,
	config *Config,
	cons consumer.Logs,
	informers Informers,
	peerStore PeerStore,
	metricsRecorder metrics.Recorder,
) *crdCollector {
	if peerStore == nil {
		peerStore = &noopPeerStore{}
	}
	if metricsRecorder == nil {
		metricsRecorder = metrics.NoopRecorder{}
	}
	return &crdCollector{
		logger:    logger,
		config:    config,
		consumer:  cons,
		informers: informers,
		peerStore: peerStore,
		metrics:   metricsRecorder,
	}
}

func (c *crdCollector) Start(ctx context.Context) error {
	loopCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	// Bootstrap the peer cache from the leader (if any). Idempotent — if the cache
	// is already populated (e.g., this node was a secondary receiving deltas), it's
	// a no-op. If no peer can serve a snapshot, the cache stays empty and the first
	// increment becomes a full snapshot from informer state.
	if err := c.peerStore.Bootstrap(ctx); err != nil {
		c.logger.Debug("Bootstrap failed, starting fresh", zap.Error(err))
	}

	if err := c.informers.Start(ctx); err != nil {
		cancel()
		return err
	}

	c.wg.Add(1)
	go c.runIncrementLoop(loopCtx)

	return nil
}

func (c *crdCollector) Shutdown(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}

	// Wait for the increment loop to exit before shutting down informers,
	// since the loop reads from informer caches.
	c.wg.Wait()

	if c.informers != nil {
		return c.informers.Shutdown(ctx)
	}

	return nil
}

// --- Consolidated increment loop ---

// runIncrementLoop is the single polling loop that reads informers caches, compares
// against the resource resourceCache, and emits deltas or periodic snapshots.
func (c *crdCollector) runIncrementLoop(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.IncrementInterval)
	defer ticker.Stop()

	// Run first increment immediately
	c.runIncrement(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runIncrement(ctx)
		}
	}
}

// runIncrement reads the current informer state, compares it against the peerStore
// cache, and emits changes. Periodically emits full snapshots for TTL freshness.
func (c *crdCollector) runIncrement(ctx context.Context) {
	start := time.Now()
	currentCRDs := c.informers.ReadCRDs()
	currentCRs := c.informers.ReadCRs()
	changes := c.peerStore.ComputeChanges(currentCRDs, currentCRs)

	if c.peerStore.IsEmpty() || time.Since(c.peerStore.LastSnapshotTime()) >= c.config.SnapshotInterval {
		c.emitSnapshot(ctx, currentCRDs, currentCRs, changes)
		c.recordSnapshotApplied(ctx, changes)
		c.metrics.RecordCycle(ctx, metrics.ModeSnapshot, time.Since(start))
	} else {
		c.emitIncrement(ctx, changes)
		c.metrics.RecordCycle(ctx, metrics.ModeIncrement, time.Since(start))
	}
}

// recordSnapshotApplied syncs the peer cache with the given changes, broadcasts
// the diff to secondaries, and resets the snapshot timer to now. Called after a
// snapshot is emitted (or after the initial-state population) to keep all three
// pieces of state — cache, peers, snapshot clock — coherent.
//
// Increment cycles use ApplyDelta directly with LastSnapshotTime unset; only
// this helper sets it, which is how secondaries distinguish snapshot ticks.
func (c *crdCollector) recordSnapshotApplied(ctx context.Context, changes []ResourceChange) {
	now := time.Now()
	if err := c.peerStore.ApplyDelta(ctx, &PeerSyncDelta{
		AppliedAt:        now,
		LastSnapshotTime: now,
		Changes:          changes,
	}); err != nil {
		c.logger.Debug("Failed to record snapshot applied", zap.Error(err))
	}
}

// emitSnapshot emits all current resources as ADDED for downstream TTL freshness,
// and emits DELETED for entries in `changes` that mark resources removed from
// informer state. `changes` is the diff between peerStore cache and current
// informer state — computed once per cycle by the caller.
func (c *crdCollector) emitSnapshot(
	ctx context.Context,
	currentCRDs []*unstructured.Unstructured,
	currentCRs map[schema.GroupVersionResource][]*unstructured.Unstructured,
	changes []ResourceChange,
) {
	var crdAdded, crAdded, crdDeleted, crDeleted int64

	for _, crd := range currentCRDs {
		if err := emit.Log(
			ctx, c.consumer, crd, watch.Added, c.config.ClusterName, emit.BuildCRDLogRecord,
		); err != nil {
			c.logger.Debug("Failed to emit CRD snapshot log",
				zap.String("name", crd.GetName()),
				zap.Error(err),
			)
			continue
		}
		crdAdded++
	}

	for gvr, crs := range currentCRs {
		for _, cr := range crs {
			if err := emit.Log(
				ctx, c.consumer, cr, watch.Added, c.config.ClusterName, emit.BuildCRLogRecord,
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

	for _, ch := range changes {
		if ch.EventType != watch.Deleted {
			continue
		}
		var err error
		if ch.IsCRD {
			err = emit.Log(ctx, c.consumer, ch.Obj, watch.Deleted, c.config.ClusterName, emit.BuildCRDLogRecord)
		} else {
			err = emit.Log(ctx, c.consumer, ch.Obj, watch.Deleted, c.config.ClusterName, emit.BuildCRLogRecord)
		}
		if err != nil {
			c.logger.Debug("Failed to emit deletion in snapshot",
				zap.String("name", ch.Obj.GetName()),
				zap.Error(err),
			)
			continue
		}
		if ch.IsCRD {
			crdDeleted++
		} else {
			crDeleted++
		}
	}

	c.metrics.RecordEmitted(ctx, metrics.ChangeAdded, crdAdded+crAdded)
	c.metrics.RecordEmitted(ctx, metrics.ChangeDeleted, crdDeleted+crDeleted)

	c.logger.Debug("Snapshot complete",
		zap.Int64("crds", crdAdded),
		zap.Int64("crs", crAdded),
		zap.Int64("deleted", crdDeleted+crDeleted),
	)
}

// emitIncrement emits the given changes using a two-phase ordering chosen to
// minimise the impact of a leader crash:
//
//   - Adds/Mods: apply+broadcast BEFORE emitting. If we crash between apply and emit,
//     the new leader's cache matches informer state and emits nothing — the platform
//     briefly misses the change but is reconciled by the next snapshot.
//   - Deletes: emit BEFORE apply+broadcast. If we crash between emit and apply, the
//     new leader's cache still contains the deleted resource while informer does not,
//     so it re-emits the DELETE (idempotent duplicate). This avoids a missed DELETE
//     lingering on the platform until TTL expiry.
func (c *crdCollector) emitIncrement(ctx context.Context, changes []ResourceChange) {
	if len(changes) == 0 {
		return
	}

	var adds, deletes []ResourceChange
	for _, ch := range changes {
		if ch.EventType == watch.Deleted {
			deletes = append(deletes, ch)
		} else {
			adds = append(adds, ch)
		}
	}

	// Phase 1 — adds/mods: apply+broadcast first, then emit.
	if len(adds) > 0 {
		if err := c.peerStore.ApplyDelta(ctx, &PeerSyncDelta{
			AppliedAt: time.Now(),
			Changes:   adds,
		}); err != nil {
			c.logger.Debug("Failed to apply delta before emitting adds/mods", zap.Error(err))
		}
		c.emitChanges(ctx, adds)
	}

	// Phase 2 — deletes: emit first, then apply+broadcast.
	if len(deletes) > 0 {
		c.emitChanges(ctx, deletes)
		if err := c.peerStore.ApplyDelta(ctx, &PeerSyncDelta{
			AppliedAt: time.Now(),
			Changes:   deletes,
		}); err != nil {
			c.logger.Debug("Failed to apply delta after emitting deletes", zap.Error(err))
		}
	}

	c.logger.Debug("Increment complete", zap.Int("changes", len(changes)))
}

// emitChanges sends a list of changes to the consumer and records per-type counters.
func (c *crdCollector) emitChanges(ctx context.Context, changes []ResourceChange) {
	counts := map[metrics.ChangeType]int64{}
	for _, change := range changes {
		var err error
		if change.IsCRD {
			err = emit.Log(ctx, c.consumer, change.Obj, change.EventType, c.config.ClusterName, emit.BuildCRDLogRecord)
		} else {
			err = emit.Log(ctx, c.consumer, change.Obj, change.EventType, c.config.ClusterName, emit.BuildCRLogRecord)
		}
		if err != nil {
			c.logger.Debug("Failed to emit change log",
				zap.String("event", string(change.EventType)),
				zap.String("name", change.Obj.GetName()),
				zap.Error(err),
			)
			continue
		}
		counts[c.watchEventToChangeType(change.EventType)]++
	}
	for changeType, count := range counts {
		c.metrics.RecordEmitted(ctx, changeType, count)
	}
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
