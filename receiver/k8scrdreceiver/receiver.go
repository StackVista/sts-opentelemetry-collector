package k8scrdreceiver

import (
	"context"
	"fmt"
	"time"

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
	collector     *crdCollector
}

func newReceiver(params receiver.Settings, config *Config, consumer consumer.Logs) (receiver.Logs, error) {
	return &k8scrdReceiver{
		settings: params,
		config:   config,
		consumer: consumer,
	}, nil
}

func (r *k8scrdReceiver) Start(ctx context.Context, _ component.Host) error {
	if r.dynamicClient == nil {
		client, err := r.config.getDynamicClient()
		if err != nil {
			return fmt.Errorf("failed to create dynamic client: %w", err)
		}
		r.dynamicClient = client
	}

	ft := newForbiddenTracker(defaultForbiddenRetryInterval)
	informerSet := newResourceInformers(r.settings, r.config, r.dynamicClient, ft)
	r.collector = newCRDCollector(r.settings.Logger, r.config, r.consumer, informerSet)

	if err := r.collector.Start(ctx); err != nil {
		return fmt.Errorf("failed to start CRD collector: %w", err)
	}

	r.settings.Logger.Info("K8s CRD Receiver started",
		zap.Duration("increment_interval", r.config.IncrementInterval),
		zap.Duration("snapshot_interval", r.config.SnapshotInterval),
		zap.Bool("include_initial_state", r.config.IncludeInitialState),
		zap.String("discovery_mode", string(r.config.DiscoveryMode)),
	)

	return nil
}

func (r *k8scrdReceiver) Shutdown(ctx context.Context) error {
	r.settings.Logger.Info("Shutting down K8s CRD Receiver")

	if r.collector != nil {
		return r.collector.Shutdown(ctx)
	}

	return nil
}
