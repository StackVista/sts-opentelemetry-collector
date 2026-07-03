package k8sresourcereceiver

import (
	"context"
	"fmt"
	"sync"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/k8sleaderelector"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/tracker"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
)

// k8sresourceReceiver uses informers to watch CRDs and their custom resources.
type k8sresourceReceiver struct {
	settings        receiver.Settings
	config          *Config
	consumer        consumer.Logs
	metrics         metrics.Recorder
	dynamicClient   dynamic.Interface            // injectable for testing
	discoveryClient discovery.DiscoveryInterface // injectable for testing; only used when Config.Objects is non-empty

	// resolvedObjects is populated in Start() from Config.Objects via
	// discovery. Consumed by the informer layer to build dynamic informers.
	resolvedObjects []resolvedObjectWatch

	// Peer store — runs regardless of leadership for push/pull sync.
	peerStore *peerSyncCacheStore

	// Resource attribute enrichment runs on every replica so a newly elected leader has warm values.
	resourceAttributeManager   *resourceAttributeManager
	resolvedResourceAttributes []resolvedResourceAttributeEnrichment

	// Collector lifecycle — guarded by leader election when enabled.
	mu        sync.Mutex
	collector *resourceCollector
}

func newReceiver(
	params receiver.Settings,
	config *Config,
	consumer consumer.Logs,
	rec metrics.Recorder,
) (receiver.Logs, error) {
	if rec == nil {
		rec = metrics.NoopRecorder{}
	}
	return &k8sresourceReceiver{
		settings: params,
		config:   config,
		consumer: consumer,
		metrics:  rec,
	}, nil
}

func (r *k8sresourceReceiver) Start(ctx context.Context, host component.Host) error {
	if r.config.DiscoveryMode == DiscoveryModeAll && r.config.CRDAPIGroupFilters != nil {
		r.settings.Logger.Warn(
			"crd_api_group_filters is configured but has no effect when discovery_mode is 'all'; " +
				"remove the filters or switch to discovery_mode: api_groups",
		)
	}

	if r.dynamicClient == nil {
		client, err := r.config.getDynamicClient()
		if err != nil {
			return fmt.Errorf("failed to create dynamic client: %w", err)
		}
		r.dynamicClient = client
	}

	if len(r.config.Objects) > 0 || len(r.config.ResourceAttributes) > 0 {
		if err := r.ensureDiscoveryClient(); err != nil {
			return err
		}
	}

	if len(r.config.Objects) > 0 {
		resolved, err := resolveObjectGVRs(r.discoveryClient, r.config.Objects, r.settings.Logger)
		if err != nil {
			return fmt.Errorf("failed to resolve object GVRs: %w", err)
		}
		if err := classifyStaticObjectsCRDOverlap(ctx, r.dynamicClient, r.config, resolved); err != nil {
			return err
		}
		r.resolvedObjects = resolved
	}

	if len(r.config.ResourceAttributes) > 0 {
		resolved, err := resolveResourceAttributeEnrichments(
			r.discoveryClient, r.config.ResourceAttributes, r.settings.Logger,
		)
		if err != nil {
			return fmt.Errorf("failed to resolve resource attribute enrichments: %w", err)
		}
		r.resolvedResourceAttributes = resolved
		r.resourceAttributeManager = newResourceAttributeManager(r.settings.Logger, r.dynamicClient, resolved)
		if err := r.resourceAttributeManager.Start(ctx); err != nil {
			return fmt.Errorf("failed to start resource attribute enrichment: %w", err)
		}
	}

	r.peerStore = newPeerSyncCacheStore(
		r.settings.Logger,
		r.config.PeerSyncPort,
		r.config.PeerSyncDNS,
		r.metrics,
	)
	if err := r.peerStore.Start(ctx); err != nil {
		return fmt.Errorf("failed to start peer sync cache store: %w", err)
	}

	// Bootstrap on every replica — not just the leader. Secondaries need a warm
	// cache too, otherwise their state only accumulates from received deltas
	// (which are sparse on a stable cluster) and they can't serve a useful
	// snapshot if a future leader has to bootstrap from them.
	if err := r.peerStore.Bootstrap(ctx); err != nil {
		r.settings.Logger.Debug("Initial bootstrap returned error, continuing", zap.Error(err))
	}

	if r.config.K8sLeaderElector != nil {
		return r.startWithLeaderElection(ctx, host)
	}

	// Single-replica deploy: this instance is effectively always the leader.
	// Marking it as such tags its served snapshots with Source=leader so any
	// future bootstrapping peer treats them as authoritative.
	r.peerStore.SetLeader(true)
	return r.startCollector(ctx)
}

func (r *k8sresourceReceiver) ensureDiscoveryClient() error {
	if r.discoveryClient != nil {
		return nil
	}
	client, err := r.config.getDiscoveryClient()
	if err != nil {
		return fmt.Errorf("failed to create discovery client: %w", err)
	}
	r.discoveryClient = client
	return nil
}

// startCollector creates and starts the collector (called directly or from leader election callback).
// Idempotent: a duplicate onStartLeading callback (e.g., k8s lease re-acquisition firing twice)
// is a no-op rather than spawning a second collector that would leak goroutines and informers.
func (r *k8sresourceReceiver) startCollector(ctx context.Context) error {
	r.mu.Lock()
	if r.collector != nil {
		r.mu.Unlock()
		r.settings.Logger.Warn("startCollector called while collector already running; ignoring")
		return nil
	}
	r.mu.Unlock()

	ft := tracker.NewDefault()
	informers := newResourceInformers(r.settings, r.config, r.dynamicClient, r.resolvedObjects, ft, r.metrics)
	collector := newResourceCollector(
		r.settings.Logger, r.config, r.consumer, informers, r.peerStore, r.metrics, r.resourceAttributeManager,
	)

	if err := collector.Start(ctx); err != nil {
		return fmt.Errorf("failed to start resource collector: %w", err)
	}

	r.mu.Lock()
	if r.collector != nil {
		// A concurrent startCollector won the race between the check above and now.
		// Shut down the collector we just started; the other one is the survivor.
		r.mu.Unlock()
		r.settings.Logger.Warn("startCollector raced with itself; shutting down duplicate collector")
		if err := collector.Shutdown(ctx); err != nil {
			r.settings.Logger.Debug("Error shutting down duplicate collector", zap.Error(err))
		}
		return nil
	}
	r.collector = collector
	r.mu.Unlock()

	r.settings.Logger.Info("K8s Resource Receiver started",
		zap.Duration("increment_interval", r.config.IncrementInterval),
		zap.Duration("snapshot_interval", r.config.SnapshotInterval),
		zap.String("discovery_mode", string(r.config.DiscoveryMode)),
	)

	return nil
}

// stopCollector shuts down the collector (called directly or from leader election callback).
func (r *k8sresourceReceiver) stopCollector(ctx context.Context) {
	r.mu.Lock()
	collector := r.collector
	r.collector = nil
	r.mu.Unlock()

	if collector != nil {
		if err := collector.Shutdown(ctx); err != nil {
			r.settings.Logger.Debug("Error shutting down collector", zap.Error(err))
		}
	}
}

// --- Leader election via k8sleaderelector extension ---

func (r *k8sresourceReceiver) startWithLeaderElection(_ context.Context, host component.Host) error {
	ext, ok := host.GetExtensions()[*r.config.K8sLeaderElector]
	if !ok {
		return fmt.Errorf("k8s leader elector extension %q not found", r.config.K8sLeaderElector)
	}

	elector, ok := ext.(k8sleaderelector.LeaderElection)
	if !ok {
		return fmt.Errorf("extension %q does not implement k8sleaderelector.LeaderElection", r.config.K8sLeaderElector)
	}

	r.settings.Logger.Info("Registering with leader election extension",
		zap.String("extension", r.config.K8sLeaderElector.String()),
	)

	elector.SetCallBackFuncs(
		func(ctx context.Context) {
			r.settings.Logger.Info("Became leader, starting resource collection")
			r.peerStore.SetLeader(true)
			if err := r.startCollector(ctx); err != nil {
				r.settings.Logger.Error("Failed to start collector after acquiring leadership", zap.Error(err))
			}
		},
		func() {
			r.settings.Logger.Info("Lost leadership, stopping resource collection")
			r.peerStore.SetLeader(false)
			r.stopCollector(context.Background())
		},
	)

	return nil
}

// --- Shutdown ---

func (r *k8sresourceReceiver) Shutdown(ctx context.Context) error {
	r.settings.Logger.Info("Shutting down K8s Resource Receiver")
	r.stopCollector(ctx)
	if r.peerStore != nil {
		r.peerStore.Stop()
	}
	if r.resourceAttributeManager != nil {
		return r.resourceAttributeManager.Shutdown(ctx)
	}
	return nil
}
