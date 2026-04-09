package k8scrdreceiver

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/metadata"
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
		Pull: PullConfig{
			Enabled:  true,
			Interval: 1 * time.Hour,
		},
		Watch: WatchConfig{
			Enabled:             true,
			IncludeInitialState: true,
		},
		DiscoveryMode: DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: []string{"*"}, // Watch all API groups by default
			Exclude: []string{},
		},
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

	return newReceiver(params, rcfg, consumer)
}
