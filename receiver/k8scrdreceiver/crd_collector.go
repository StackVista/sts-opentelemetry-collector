package k8scrdreceiver

import (
	"context"
	"sync"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/emit"
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

	// External cache persistence
	cacheStore CacheStore

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
	cacheStore CacheStore,
) *crdCollector {
	if cacheStore == nil {
		cacheStore = &noopCacheStore{}
	}
	return &crdCollector{
		logger:     logger,
		config:     config,
		consumer:   cons,
		informer:   informer,
		cacheStore: cacheStore,
	}
}

func (c *crdCollector) Start(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)

	// Load persisted cache from external store (if configured).
	// On failure, start with an empty cache — the first increment will be a full snapshot.
	loadedCache, err := c.cacheStore.Load(ctx)
	if err != nil {
		c.logger.Debug("Failed to load cache from store, starting fresh", zap.Error(err))
		c.cache = newResourceCache()
	} else {
		c.cache = loadedCache
		if !c.cache.isEmpty() {
			c.logger.Info("Loaded resource cache from store",
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

	// Persist cache to external store before stopping.
	if c.cache != nil {
		if err := c.cacheStore.Save(ctx, c.cache); err != nil {
			c.logger.Debug("Failed to save cache to store on shutdown", zap.Error(err))
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
		if err := emit.Log(
			c.ctx, c.consumer, crd, watch.Added, c.config.ClusterName, emit.BuildCRDLogRecord,
		); err != nil {
			c.logger.Debug("Failed to emit CRD snapshot log",
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
			crCount++
		}
	}

	// Emit DELETED for CRDs in the resource cache that are no longer present
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
			deletedCount++
		}
	}

	// Emit DELETED for CRs in the resource cache that are no longer present
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
		}
	}

	c.logger.Debug("Increment complete",
		zap.Int("changes", len(changes)),
	)
}
