package k8scrdreceiver

import (
	"context"
	"sync"
	"time"

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

	// Resource cache for delta computation
	cache            *resourceCache
	lastSnapshotTime time.Time

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
) *crdCollector {
	return &crdCollector{
		logger:   logger,
		config:   config,
		consumer: cons,
		informer: informer,
	}
}

func (c *crdCollector) Start(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.cache = newResourceCache()

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
	currentCRDs := c.informer.ReadCRDs()
	currentCRs := c.informer.ReadCRs()

	// If IncludeInitialState is false and cache is empty, populate without emitting.
	// Set lastSnapshotTime so the next increment doesn't trigger a snapshot.
	if !c.config.IncludeInitialState && c.cache.isEmpty() {
		c.cache.update(currentCRDs, currentCRs)
		c.lastSnapshotTime = time.Now()
		c.logger.Info("Initial state loaded into resource cache (not emitting)",
			zap.Int("crds", len(currentCRDs)),
			zap.Int("cr_types", len(currentCRs)),
		)
		return
	}

	// Determine if this is a snapshot or increment
	isSnapshot := c.cache.isEmpty() || time.Since(c.lastSnapshotTime) >= c.config.SnapshotInterval

	if isSnapshot {
		c.emitSnapshot(currentCRDs, currentCRs)
		c.lastSnapshotTime = time.Now()
	} else {
		c.emitIncrement(currentCRDs, currentCRs)
	}

	c.cache.update(currentCRDs, currentCRs)
}

// emitSnapshot emits all current resources as ADDED for downstream TTL freshness,
// and emits DELETED for resources that were in the resource cache but are no longer present.
func (c *crdCollector) emitSnapshot(
	currentCRDs []*unstructured.Unstructured,
	currentCRs map[schema.GroupVersionResource][]*unstructured.Unstructured,
) {
	var crdCount, crCount, deletedCount int

	// Emit ADDED for all current CRDs
	currentCRDNames := make(map[string]struct{}, len(currentCRDs))
	for _, crd := range currentCRDs {
		currentCRDNames[crd.GetName()] = struct{}{}
		if err := emitLog(c.ctx, c.consumer, crd, watch.Added, c.config.ClusterName, buildCRDLogRecord); err != nil {
			c.logger.Error("Failed to emit CRD snapshot log",
				zap.String("name", crd.GetName()),
				zap.Error(err),
			)
			continue
		}
		crdCount++
	}

	// Emit ADDED for all current CRs
	currentCRKeys := make(map[string]struct{})
	for gvr, crs := range currentCRs {
		for _, cr := range crs {
			currentCRKeys[crResourceKey(gvr, cr.GetNamespace(), cr.GetName())] = struct{}{}
			if err := emitLog(c.ctx, c.consumer, cr, watch.Added, c.config.ClusterName, buildCRLogRecord); err != nil {
				c.logger.Error("Failed to emit CR snapshot log",
					zap.String("gvr", formatGVRKey(gvr)),
					zap.String("name", cr.GetName()),
					zap.Error(err),
				)
				continue
			}
			crCount++
		}
	}

	// Emit DELETED for CRDs in the resource cache that are no longer present
	for name, cachedCRD := range c.cache.crds {
		if _, exists := currentCRDNames[name]; !exists {
			if err := emitLog(c.ctx, c.consumer, cachedCRD, watch.Deleted, c.config.ClusterName, buildCRDLogRecord); err != nil {
				c.logger.Error("Failed to emit CRD deletion in snapshot",
					zap.String("name", cachedCRD.GetName()),
					zap.Error(err),
				)
				continue
			}
			deletedCount++
		}
	}

	// Emit DELETED for CRs in the resource cache that are no longer present
	for key, cached := range c.cache.crs {
		if _, exists := currentCRKeys[key]; !exists {
			if err := emitLog(c.ctx, c.consumer, cached.obj, watch.Deleted, c.config.ClusterName, buildCRLogRecord); err != nil {
				c.logger.Error("Failed to emit CR deletion in snapshot",
					zap.String("name", cached.obj.GetName()),
					zap.Error(err),
				)
				continue
			}
			deletedCount++
		}
	}

	c.logger.Debug("Snapshot complete",
		zap.Int("crds", crdCount),
		zap.Int("crs", crCount),
		zap.Int("deleted", deletedCount),
	)
}

// emitIncrement computes the delta between resource cache and current state, and emits changes.
func (c *crdCollector) emitIncrement(
	currentCRDs []*unstructured.Unstructured,
	currentCRs map[schema.GroupVersionResource][]*unstructured.Unstructured,
) {
	changes := c.cache.computeChanges(currentCRDs, currentCRs)

	if len(changes) == 0 {
		return
	}

	for _, change := range changes {
		var err error
		if change.isCRD {
			err = emitLog(c.ctx, c.consumer, change.obj, change.eventType, c.config.ClusterName, buildCRDLogRecord)
		} else {
			err = emitLog(c.ctx, c.consumer, change.obj, change.eventType, c.config.ClusterName, buildCRLogRecord)
		}
		if err != nil {
			c.logger.Error("Failed to emit change log",
				zap.String("event", string(change.eventType)),
				zap.String("name", change.obj.GetName()),
				zap.Error(err),
			)
		}
	}

	c.logger.Debug("Increment complete",
		zap.Int("changes", len(changes)),
	)
}
