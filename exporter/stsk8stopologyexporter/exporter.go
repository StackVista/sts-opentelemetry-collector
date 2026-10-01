package stsk8stopologyexporter

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/model/topology"
	"github.com/StackVista/stackstate-receiver-go-client/pkg/openapiclient/features"
	collectors "github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8stopologyexporter/internal/topologycollectors"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// Cluster Observer record contract; see receiver/k8sresourcereceiver/internal/emit.
const (
	eventNameObject           = "KubernetesObjectEvent"
	eventNameSnapshotBoundary = "KubernetesSnapshotBoundary"
	attrKind                  = "k8s.resource.kind"
	attrGroup                 = "k8s.resource.group"
	attrVersion               = "k8s.resource.version"
	attrBoundary              = "k8s.snapshot.boundary"
	attrSnapshotID            = "k8s.snapshot.id"
	attrSnapshotComplete      = "k8s.snapshot.complete"
	eventDeleted              = "DELETED"
)

type topologyExporter struct {
	cfg      *Config
	logger   *zap.Logger
	store    *objectStore
	sender   *intakeSender
	instance topology.Instance
	producer string
	now      func() time.Time

	// features is nil when discovery is disabled; legacy topology is then always sent.
	features *features.Client
	//nolint:containedctx // carries the Receiver credential the feature client authenticates with
	authCtx   context.Context
	discovery discoveryOptions
	mode      *modeSelector

	ready   chan struct{}
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started time.Time
	host    component.Host

	// delivery guards the cancel function of the snapshot being sent, so a
	// reset can stop it before it reaches stop_snapshot.
	delivery       sync.Mutex
	cancelDelivery context.CancelFunc
}

func newTopologyExporter(cfg *Config, logger *zap.Logger, sender *intakeSender) *topologyExporter {
	return &topologyExporter{
		cfg:    cfg,
		logger: logger,
		store:  newObjectStore(),
		sender: sender,
		// The cluster agent always reports a kubernetes instance; cluster_type only changes tags.
		instance:  topology.Instance{Type: clusterTypeKubernetes, URL: cfg.ClusterName},
		producer:  cfg.InternalHostname,
		now:       time.Now,
		ready:     make(chan struct{}, 1),
		discovery: defaultDiscoveryOptions(),
		mode:      newModeSelector(defaultDiscoveryOptions().stableObservations),
	}
}

func (e *topologyExporter) start(_ context.Context, host component.Host) error {
	e.host = host
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.started = e.now()
	initialized := make(chan struct{})
	if e.features != nil {
		e.wg.Add(1)
		go e.discover(ctx, initialized)
	} else {
		close(initialized)
	}
	e.wg.Add(1)
	go e.run(ctx, initialized)
	return nil
}

func (e *topologyExporter) shutdown(_ context.Context) error {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	e.sender.client.CloseIdleConnections()
	return nil
}

// consumeLogs only updates the object store; intake delivery runs on its own schedule.
func (e *topologyExporter) consumeLogs(_ context.Context, logs plog.Logs) error {
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		scopes := logs.ResourceLogs().At(i).ScopeLogs()
		for j := 0; j < scopes.Len(); j++ {
			records := scopes.At(j).LogRecords()
			for k := 0; k < records.Len(); k++ {
				e.consumeRecord(records.At(k))
			}
		}
	}
	return nil
}

func (e *topologyExporter) consumeRecord(record plog.LogRecord) {
	switch record.EventName() {
	case eventNameSnapshotBoundary:
		e.consumeBoundary(record.Attributes())
	case eventNameObject:
		e.consumeObject(record)
	}
}

func (e *topologyExporter) consumeBoundary(attrs pcommon.Map) {
	boundary := stringAttr(attrs, attrBoundary)
	id := stringAttr(attrs, attrSnapshotID)
	switch boundary {
	case "start":
		e.store.startSnapshot(id)
	case "end":
		complete, _ := attrs.Get(attrSnapshotComplete)
		if e.store.endSnapshot(id, complete.Bool(), e.now()) {
			select {
			case e.ready <- struct{}{}:
			default:
			}
		} else if !complete.Bool() {
			e.logger.Warn("Cluster Observer snapshot was incomplete; not sending topology until a complete snapshot arrives")
		}
	case "reset":
		e.cancelActiveDelivery()
		e.store.reset()
		e.logger.Info("Cluster Observer stopped collecting; topology sending paused")
	}
}

func (e *topologyExporter) consumeObject(record plog.LogRecord) {
	if record.Body().Type() != pcommon.ValueTypeMap {
		return
	}
	body := record.Body().Map()
	objectValue, ok := body.Get("object")
	if !ok || objectValue.Type() != pcommon.ValueTypeMap {
		return
	}
	object := objectValue.Map().AsRaw()
	metadata, _ := object["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	namespace, _ := metadata["namespace"].(string)
	if name == "" {
		return
	}
	attrs := record.Attributes()
	key := objectKey{
		group:     stringAttr(attrs, attrGroup),
		kind:      stringAttr(attrs, attrKind),
		namespace: namespace,
		name:      name,
	}
	eventType, _ := body.Get("type")
	if eventType.Str() == eventDeleted {
		e.store.remove(key)
		return
	}
	e.store.upsert(key, stringAttr(attrs, attrVersion), object)
}

func (e *topologyExporter) run(ctx context.Context, initialized <-chan struct{}) {
	defer e.wg.Done()
	// Wait for the first platform answer so a platform that no longer needs
	// legacy topology does not receive one first.
	select {
	case <-ctx.Done():
		return
	case <-initialized:
	}
	ticker := time.NewTicker(e.cfg.Interval)
	defer ticker.Stop()
	warned := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.ready:
		case <-ticker.C:
		}
		sent, err := e.sendSnapshot(ctx)
		if errors.Is(err, errCollectPanic) {
			// A panicking collector may leave correlators blocked; stop rather than leak every cycle.
			e.logger.Error("Kubernetes topology collection failed permanently; no further topology is sent", zap.Error(err))
			if e.host != nil {
				componentstatus.ReportStatus(e.host, componentstatus.NewPermanentErrorEvent(err))
			}
			return
		}
		if !sent && !warned && e.now().Sub(e.started) > e.cfg.SnapshotMaxAge {
			e.logger.Warn("No complete Cluster Observer snapshot received; check that the observer " +
				"feeds this exporter with emit_snapshot_boundaries enabled")
			warned = true
		}
	}
}

// sendSnapshot builds and sends one full topology snapshot. It reports false
// when the store has no current complete snapshot.
func (e *topologyExporter) sendSnapshot(ctx context.Context) (bool, error) {
	if !e.mode.legacyEnabled() {
		return true, nil
	}
	// Registered before reading the store: a reset either empties the store
	// first or cancels this delivery.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	e.delivery.Lock()
	e.cancelDelivery = cancel
	e.delivery.Unlock()
	defer func() {
		e.delivery.Lock()
		e.cancelDelivery = nil
		e.delivery.Unlock()
	}()

	objects, ok := e.store.view(e.now(), e.cfg.SnapshotMaxAge)
	if !ok {
		return false, nil
	}
	client := &cacheClient{objects: objects, logger: e.logger}
	result, err := collectTopology(client, e.instance, collectors.ClusterType(e.cfg.ClusterType), e.cfg, e.logger)
	if err != nil {
		e.logger.Warn("Skipping topology snapshot", zap.Error(err))
		return true, err
	}
	components := make([]topology.Component, 0, len(result.components))
	for _, component := range result.components {
		components = append(components, *component)
	}
	relations := make([]topology.Relation, 0, len(result.relations))
	for _, relation := range result.relations {
		relations = append(relations, *relation)
	}
	payloads := buildPayloads(e.producer, e.instance, components, relations, e.cfg.MaxElementsPerRequest)
	if err := e.sender.sendAll(ctx, payloads); err != nil {
		e.logger.Warn("Failed to send topology snapshot; retrying at the next interval", zap.Error(err))
		return true, nil
	}
	e.logger.Debug("Sent topology snapshot",
		zap.Int("components", len(components)),
		zap.Int("relations", len(relations)),
		zap.Int("requests", len(payloads)),
	)
	return true, nil
}

func (e *topologyExporter) cancelActiveDelivery() {
	e.delivery.Lock()
	defer e.delivery.Unlock()
	if e.cancelDelivery != nil {
		e.cancelDelivery()
	}
}

func stringAttr(attrs pcommon.Map, key string) string {
	value, ok := attrs.Get(key)
	if !ok {
		return ""
	}
	return value.Str()
}
