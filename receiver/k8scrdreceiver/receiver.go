package k8scrdreceiver

import (
	"context"
	"fmt"
	"sync"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/k8sleaderelector"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/tracker"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"k8s.io/client-go/dynamic"
)

// k8scrdReceiver uses informers to watch CRDs and their custom resources.
type k8scrdReceiver struct {
	settings      receiver.Settings
	config        *Config
	consumer      consumer.Logs
	metrics       metrics.Recorder
	dynamicClient dynamic.Interface // injectable for testing

	// Peer store — runs regardless of leadership for push/pull sync.
	peerStore *peerSyncCacheStore

	// Collector lifecycle — guarded by leader election when enabled.
	mu        sync.Mutex
	collector *crdCollector
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
	return &k8scrdReceiver{
		settings: params,
		config:   config,
		consumer: consumer,
		metrics:  rec,
	}, nil
}

func (r *k8scrdReceiver) Start(ctx context.Context, host component.Host) error {
	if r.dynamicClient == nil {
		client, err := r.config.getDynamicClient()
		if err != nil {
			return fmt.Errorf("failed to create dynamic client: %w", err)
		}
		r.dynamicClient = client
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

// startCollector creates and starts the collector (called directly or from leader election callback).
// Idempotent: a duplicate onStartLeading callback (e.g., k8s lease re-acquisition firing twice)
// is a no-op rather than spawning a second collector that would leak goroutines and informers.
func (r *k8scrdReceiver) startCollector(ctx context.Context) error {
	r.mu.Lock()
	if r.collector != nil {
		r.mu.Unlock()
		r.settings.Logger.Warn("startCollector called while collector already running; ignoring")
		return nil
	}
	r.mu.Unlock()

	ft := tracker.NewDefault()
	informers := newResourceInformers(r.settings, r.config, r.dynamicClient, ft, r.metrics)
	collector := newCRDCollector(r.settings.Logger, r.config, r.consumer, informers, r.peerStore, r.metrics)

	if err := collector.Start(ctx); err != nil {
		return fmt.Errorf("failed to start CRD collector: %w", err)
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

	r.settings.Logger.Info("K8s CRD Receiver started",
		zap.Duration("increment_interval", r.config.IncrementInterval),
		zap.Duration("snapshot_interval", r.config.SnapshotInterval),
		zap.String("discovery_mode", string(r.config.DiscoveryMode)),
	)

	return nil
}

// stopCollector shuts down the collector (called directly or from leader election callback).
func (r *k8scrdReceiver) stopCollector(ctx context.Context) {
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

func (r *k8scrdReceiver) startWithLeaderElection(_ context.Context, host component.Host) error {
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
			r.settings.Logger.Info("Became leader, starting CRD collection")
			r.peerStore.SetLeader(true)
			if err := r.startCollector(ctx); err != nil {
				r.settings.Logger.Error("Failed to start collector after acquiring leadership", zap.Error(err))
			}
		},
		func() {
			r.settings.Logger.Info("Lost leadership, stopping CRD collection")
			r.peerStore.SetLeader(false)
			r.stopCollector(context.Background())
		},
	)

	return nil
}

// --- Shutdown ---

func (r *k8scrdReceiver) Shutdown(ctx context.Context) error {
	r.settings.Logger.Info("Shutting down K8s CRD Receiver")
	r.stopCollector(ctx)
	if r.peerStore != nil {
		r.peerStore.Stop()
	}
	return nil
}
