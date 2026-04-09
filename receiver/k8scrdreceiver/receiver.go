package k8scrdreceiver

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

const (
	// defaultForbiddenRetryInterval is how long to wait before retrying a resource that returned permission denied
	defaultForbiddenRetryInterval = 1 * time.Hour
)

// k8scrdReceiver orchestrates pull and watch modes for CRD/CR collection.
type k8scrdReceiver struct {
	settings         receiver.Settings
	config           *Config
	consumer         consumer.Logs
	client           k8sClient
	forbiddenTracker *forbiddenTracker
	modes            []mode
}

func newReceiver(params receiver.Settings, config *Config, consumer consumer.Logs) (receiver.Logs, error) {
	return &k8scrdReceiver{
		settings:         params,
		config:           config,
		consumer:         consumer,
		forbiddenTracker: newForbiddenTracker(defaultForbiddenRetryInterval),
	}, nil
}

func (r *k8scrdReceiver) Start(ctx context.Context, _ component.Host) error {
	// Create client if not already set
	if r.client == nil {
		dynamicClient, err := r.config.getDynamicClient()
		if err != nil {
			return fmt.Errorf("failed to create dynamic client: %w", err)
		}
		r.client = newDynamicClientWrapper(dynamicClient)
	}

	modeNames := []string{}

	if r.config.Pull.Enabled {
		pm := newPullMode(r.settings, r.config, r.consumer, r.client, r.forbiddenTracker)
		if err := pm.Start(ctx); err != nil {
			return fmt.Errorf("failed to start pull mode: %w", err)
		}
		r.modes = append(r.modes, pm)
		modeNames = append(modeNames, "pull")
	}

	if r.config.Watch.Enabled {
		wm := newWatchMode(r.settings, r.config, r.consumer, r.client, r.forbiddenTracker)
		if err := wm.Start(ctx); err != nil {
			return fmt.Errorf("failed to start watch mode: %w", err)
		}
		r.modes = append(r.modes, wm)
		modeNames = append(modeNames, "watch")
	}

	r.settings.Logger.Info("K8s CRD Receiver started",
		zap.Strings("modes", modeNames),
		zap.String("discovery_mode", string(r.config.DiscoveryMode)),
		zap.Any("api_group_filters", r.config.APIGroupFilters),
	)

	return nil
}

func (r *k8scrdReceiver) Shutdown(ctx context.Context) error {
	r.settings.Logger.Info("Shutting down K8s CRD Receiver")

	var firstErr error
	for _, m := range r.modes {
		if err := m.Shutdown(ctx); err != nil {
			r.settings.Logger.Error("Mode shutdown failed", zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}
