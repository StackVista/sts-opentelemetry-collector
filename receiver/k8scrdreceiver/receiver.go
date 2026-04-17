package k8scrdreceiver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/k8sleaderelector"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/tracker"
	"github.com/valkey-io/valkey-go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"k8s.io/client-go/dynamic"
)

const (
	// defaultForbiddenRetryInterval is how long to wait before retrying a resource that returned permission denied
	defaultForbiddenRetryInterval = 30 * time.Minute
)

// k8scrdReceiver uses informers to watch CRDs and their custom resources.
type k8scrdReceiver struct {
	settings      receiver.Settings
	config        *Config
	consumer      consumer.Logs
	dynamicClient dynamic.Interface // injectable for testing

	// Collector lifecycle — guarded by leader election when enabled.
	mu        sync.Mutex
	collector *crdCollector
}

func newReceiver(params receiver.Settings, config *Config, consumer consumer.Logs) (receiver.Logs, error) {
	return &k8scrdReceiver{
		settings: params,
		config:   config,
		consumer: consumer,
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

	if r.config.K8sLeaderElector != nil {
		return r.startWithLeaderElection(ctx, host)
	}

	return r.startCollector(ctx)
}

// startCollector creates and starts the collector (called directly or from leader election callback).
func (r *k8scrdReceiver) startCollector(ctx context.Context) error {
	cacheStore, err := r.createCacheStore()
	if err != nil {
		return fmt.Errorf("failed to create cache store: %w", err)
	}

	ft := tracker.NewForbiddenTracker(defaultForbiddenRetryInterval)
	informerSet := newResourceInformers(r.settings, r.config, r.dynamicClient, ft)
	collector := newCRDCollector(r.settings.Logger, r.config, r.consumer, informerSet, cacheStore)

	if err := collector.Start(ctx); err != nil {
		return fmt.Errorf("failed to start CRD collector: %w", err)
	}

	r.mu.Lock()
	r.collector = collector
	r.mu.Unlock()

	r.settings.Logger.Info("K8s CRD Receiver started",
		zap.Duration("increment_interval", r.config.IncrementInterval),
		zap.Duration("snapshot_interval", r.config.SnapshotInterval),
		zap.Bool("include_initial_state", r.config.IncludeInitialState),
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
			if err := r.startCollector(ctx); err != nil {
				r.settings.Logger.Error("Failed to start collector after acquiring leadership", zap.Error(err))
			}
		},
		func() {
			r.settings.Logger.Info("Lost leadership, stopping CRD collection")
			r.stopCollector(context.Background())
		},
	)

	return nil
}

// --- Cache store ---

func (r *k8scrdReceiver) createCacheStore() (CacheStore, error) {
	if r.config.ValkeyEndpoint == "" {
		r.settings.Logger.Info("No Valkey endpoint configured, cache will not be persisted")
		return &noopCacheStore{}, nil
	}

	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:       []string{r.config.ValkeyEndpoint},
		ForceSingleClient: true,
		DisableCache:      true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Valkey client for %s: %w", r.config.ValkeyEndpoint, err)
	}

	r.settings.Logger.Info("Valkey cache store configured",
		zap.String("endpoint", r.config.ValkeyEndpoint),
	)

	return newValkeyCacheStore(r.settings.Logger, client, r.config.ClusterName), nil
}

// --- Shutdown ---

func (r *k8scrdReceiver) Shutdown(ctx context.Context) error {
	r.settings.Logger.Info("Shutting down K8s CRD Receiver")
	r.stopCollector(ctx)
	return nil
}
