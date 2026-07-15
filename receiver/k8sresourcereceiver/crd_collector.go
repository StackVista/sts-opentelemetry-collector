package k8sresourcereceiver

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/emit"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// resourceCollector reads from Informers, compares against the peerStore's cache, and
// emits deltas (ADDED/MODIFIED/DELETED) or periodic full snapshots as log records.
// Cache state lives in peerStore - the collector is a stateless worker.
type resourceCollector struct {
	logger    *zap.Logger
	config    *Config
	consumer  consumer.Logs
	informers Informers
	metrics   metrics.Recorder
	enricher  resourceAttributeEnricher

	peerStore PeerStore

	// lastSnapshotTime tracks when the most recent snapshot was emitted locally.
	// Used instead of peerStore.IsEmpty() to guard the snapshot cadence so that
	// a repeated ApplyDelta failure does not cause a continuous snapshot storm.
	lastSnapshotTime time.Time

	// Lifecycle
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func newResourceCollector(
	logger *zap.Logger,
	config *Config,
	cons consumer.Logs,
	informers Informers,
	peerStore PeerStore,
	metricsRecorder metrics.Recorder,
	enricher resourceAttributeEnricher,
) *resourceCollector {
	if peerStore == nil {
		peerStore = &noopPeerStore{}
	}
	if metricsRecorder == nil {
		metricsRecorder = metrics.NoopRecorder{}
	}
	if enricher == nil {
		enricher = noopResourceAttributeEnricher{}
	}
	return &resourceCollector{
		logger:    logger,
		config:    config,
		consumer:  cons,
		informers: informers,
		peerStore: peerStore,
		metrics:   metricsRecorder,
		enricher:  enricher,
	}
}

func (c *resourceCollector) Start(ctx context.Context) error {
	loopCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	if err := c.informers.Start(ctx); err != nil {
		cancel()
		return err
	}

	c.wg.Add(1)
	go c.runIncrementLoop(loopCtx)

	return nil
}

func (c *resourceCollector) Shutdown(ctx context.Context) error {
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
func (c *resourceCollector) runIncrementLoop(ctx context.Context) {
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
func (c *resourceCollector) runIncrement(ctx context.Context) {
	start := time.Now()
	currentCRDs := c.informers.ReadCRDs()
	currentObjects := c.informers.ReadObjects()
	currentObjects = c.applyPayloadBudgets(ctx, currentObjects)
	changes := c.peerStore.ComputeChanges(currentCRDs, currentObjects)

	if c.lastSnapshotTime.IsZero() || time.Since(c.lastSnapshotTime) >= c.config.SnapshotInterval {
		c.emitSnapshot(ctx, currentCRDs, currentObjects, changes)
		c.recordSnapshotApplied(ctx, changes)
		c.lastSnapshotTime = time.Now()
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
func (c *resourceCollector) recordSnapshotApplied(ctx context.Context, changes []ResourceChange) {
	now := time.Now()
	if err := c.peerStore.ApplyDelta(ctx, &PeerSyncDelta{
		AppliedAt:        now,
		LastSnapshotTime: now,
		Changes:          changes,
	}); err != nil {
		c.logger.Warn("Failed to record snapshot applied", zap.Error(err))
	}
}

// emitSnapshot emits all current resources as ADDED for downstream TTL freshness,
// and emits DELETED for entries in `changes` that mark resources removed from
// informer state. `changes` is the diff between peerStore cache and current
// informer state — computed once per cycle by the caller.
func (c *resourceCollector) emitSnapshot(
	ctx context.Context,
	currentCRDs []*unstructured.Unstructured,
	currentObjects map[schema.GroupVersionResource]ObjectGroup,
	changes []ResourceChange,
) {
	var crdAdded, objAdded, crdDeleted, objDeleted int64

	for _, crd := range currentCRDs {
		if err := emit.LogCRD(
			ctx, c.consumer, crd, watch.Added, c.config.ClusterName, c.crdCustomResourcesWatched(crd),
		); err != nil {
			c.logger.Debug("Failed to emit CRD snapshot log",
				zap.String("name", crd.GetName()),
				zap.Error(err),
			)
			continue
		}
		crdAdded++
	}

	for gvr, group := range currentObjects {
		eventName := eventNameForSource(group.Source)
		resourceAttributes := c.resourceAttributesFor(gvr)
		for _, obj := range group.Objects {
			if err := emit.LogObject(
				ctx, c.consumer, obj, watch.Added, c.config.ClusterName, eventName, resourceAttributes,
			); err != nil {
				c.logger.Debug("Failed to emit object snapshot log",
					zap.String("gvr", emit.FormatGVRKey(gvr)),
					zap.String("name", obj.GetName()),
					zap.Error(err),
				)
				continue
			}
			objAdded++
		}
	}

	for _, ch := range changes {
		if ch.EventType != watch.Deleted {
			continue
		}
		var err error
		if ch.IsCRD {
			err = emit.LogCRD(ctx, c.consumer, ch.Obj, watch.Deleted, c.config.ClusterName, c.crdCustomResourcesWatched(ch.Obj))
		} else {
			err = emit.LogObject(
				ctx, c.consumer, ch.Obj, watch.Deleted, c.config.ClusterName,
				eventNameForSource(ch.Source), c.resourceAttributesFor(ch.GVR),
			)
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
			objDeleted++
		}
	}

	c.metrics.RecordEmitted(ctx, metrics.ChangeAdded, crdAdded+objAdded)
	c.metrics.RecordEmitted(ctx, metrics.ChangeDeleted, crdDeleted+objDeleted)

	c.logger.Debug("Snapshot complete",
		zap.Int64("crds", crdAdded),
		zap.Int64("objects", objAdded),
		zap.Int64("deleted", crdDeleted+objDeleted),
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
func (c *resourceCollector) emitIncrement(ctx context.Context, changes []ResourceChange) {
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
			c.logger.Warn("Failed to apply delta before emitting adds/mods", zap.Error(err))
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
			c.logger.Warn("Failed to apply delta after emitting deletes", zap.Error(err))
		}
	}

	c.logger.Debug("Increment complete", zap.Int("changes", len(changes)))
}

// emitChanges sends a list of changes to the consumer and records per-type counters.
func (c *resourceCollector) emitChanges(ctx context.Context, changes []ResourceChange) {
	counts := map[metrics.ChangeType]int64{}
	for _, change := range changes {
		var err error
		if change.IsCRD {
			err = emit.LogCRD(
				ctx, c.consumer, change.Obj, change.EventType,
				c.config.ClusterName, c.crdCustomResourcesWatched(change.Obj),
			)
		} else {
			err = emit.LogObject(
				ctx, c.consumer, change.Obj, change.EventType, c.config.ClusterName,
				eventNameForSource(change.Source), c.resourceAttributesFor(change.GVR),
			)
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

func (c *resourceCollector) watchEventToChangeType(e watch.EventType) metrics.ChangeType {
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

func (c *resourceCollector) resourceAttributesFor(gvr schema.GroupVersionResource) map[string]string {
	return c.enricher.AttributesFor(gvr)
}

func (c *resourceCollector) crdCustomResourcesWatched(crd *unstructured.Unstructured) bool {
	apiGroup, _, _ := unstructured.NestedString(crd.Object, "spec", "group")
	return c.config.shouldWatchAPIGroup(apiGroup)
}

type budgetCandidate struct {
	gvr    schema.GroupVersionResource
	group  ObjectGroup
	obj    *unstructured.Unstructured
	size   int
	source metrics.PayloadSource
}

func (c *resourceCollector) applyPayloadBudgets(
	ctx context.Context,
	objects map[schema.GroupVersionResource]ObjectGroup,
) map[schema.GroupVersionResource]ObjectGroup {
	result := make(map[schema.GroupVersionResource]ObjectGroup, len(objects))
	candidates := make(map[metrics.PayloadSource][]budgetCandidate)

	for gvr, group := range objects {
		payloadSource := payloadSourceForObjectSource(group.Source)
		if payloadBudget(c.config, payloadSource) <= 0 {
			// budgeting is disabled: the whole group is passed through
			result[gvr] = group
			continue
		}
		for _, obj := range group.Objects {
			size, ok := serializedObjectSize(obj.Object)
			if !ok {
				c.addObjectToBudgetResult(result, gvr, group, obj)
				continue
			}
			candidates[payloadSource] = append(candidates[payloadSource], budgetCandidate{
				gvr: gvr, group: group, obj: obj, size: size, source: payloadSource,
			})
		}
	}

	for source, items := range candidates {
		budget := payloadBudget(c.config, source)
		used := c.applyPayloadBudget(ctx, result, source, budget, items)
		c.metrics.RecordPayloadBudget(ctx, source, int64(budget), int64(used))
	}

	return result
}

func (c *resourceCollector) applyPayloadBudget(
	ctx context.Context,
	result map[schema.GroupVersionResource]ObjectGroup,
	source metrics.PayloadSource,
	budget int,
	items []budgetCandidate,
) int {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].size != items[j].size {
			return items[i].size < items[j].size
		}
		return budgetCandidateKey(items[i]) < budgetCandidateKey(items[j])
	})

	var used int
	var droppedCount int
	var droppedBytes int
	for _, item := range items {
		outcome := metrics.PayloadForwarded
		if used+item.size <= budget {
			used += item.size
			c.addObjectToBudgetResult(result, item.gvr, item.group, item.obj)
		} else {
			outcome = metrics.PayloadDropped
			droppedCount++
			droppedBytes += item.size
			c.metrics.RecordPayloadDropped(ctx, source, item.gvr.Group, item.obj.GetKind())
			c.logger.Debug("Dropped Kubernetes object payload because total payload budget is full",
				zap.String("source", string(source)),
				zap.String("name", item.obj.GetName()),
				zap.String("namespace", item.obj.GetNamespace()),
				zap.String("gvr", emit.FormatGVRKey(item.gvr)),
				zap.String("kind", item.obj.GetKind()),
				zap.Int("payload_size", item.size),
			)
		}
		c.metrics.RecordPayloadSize(ctx, source, outcome, item.gvr.Group, item.obj.GetKind(), int64(item.size))
	}

	if droppedCount > 0 {
		c.logger.Warn("Dropped Kubernetes object payloads because total payload budget is full",
			zap.String("source", string(source)),
			zap.Int("dropped_count", droppedCount),
			zap.Int("dropped_bytes", droppedBytes),
			zap.Int("payload_budget", budget),
			zap.Int("payload_budget_used", used),
		)
	}
	return used
}

func (c *resourceCollector) addObjectToBudgetResult(
	result map[schema.GroupVersionResource]ObjectGroup,
	gvr schema.GroupVersionResource,
	group ObjectGroup,
	obj *unstructured.Unstructured,
) {
	current := result[gvr]
	if current.Source == "" {
		current.Source = group.Source
	}
	current.Objects = append(current.Objects, obj)
	result[gvr] = current
}

func budgetCandidateKey(item budgetCandidate) string {
	return objectResourceKey(item.gvr, item.obj.GetNamespace(), item.obj.GetName())
}

func payloadSourceForObjectSource(source ObjectSource) metrics.PayloadSource {
	if source == ObjectSourceCR {
		return metrics.PayloadSourceCR
	}
	return metrics.PayloadSourceObject
}

func payloadBudget(config *Config, source metrics.PayloadSource) int {
	if source == metrics.PayloadSourceCR {
		return config.MaxCRTotalDataSizeBytes
	}
	return config.MaxObjectTotalDataSizeBytes
}
