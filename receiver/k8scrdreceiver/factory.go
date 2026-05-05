package k8scrdreceiver

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/metadata"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/metrics"
)

// NewFactory creates a factory for k8scrd receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(
			createLogsReceiver,
			metadata.LogsStability,
		),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		APIConfig: APIConfig{
			AuthType: AuthTypeServiceAccount,
		},
		IncrementInterval: 10 * time.Second,
		SnapshotInterval:  5 * time.Minute,
		DiscoveryMode:     DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: []string{"*"},
			Exclude: []string{},
		},
		PeerSyncPort: defaultPeerPort,
	}
}

func createLogsReceiver(
	_ context.Context,
	params receiver.Settings,
	cfg component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	rcfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", cfg)
	}

	m := metrics.NewMetrics(metadata.Type.String(), rcfg.ClusterName, params.TelemetrySettings)
	return newReceiver(params, rcfg, consumer, m)
}
