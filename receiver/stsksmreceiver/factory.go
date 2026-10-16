package stsksmreceiver

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const typeStr = "stsksm"

// NewFactory creates the cluster-agent-compatible Kubernetes state metrics receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		AuthType:           authTypeServiceAccount,
		CollectionInterval: 40 * time.Second,
		ResyncPeriod:       300 * time.Second,
		Collectors:         defaultCollectors(),
	}
}

func createMetricsReceiver(
	_ context.Context, set receiver.Settings, config component.Config, next consumer.Metrics,
) (receiver.Metrics, error) {
	cfg, ok := config.(*Config)
	if !ok {
		return nil, errors.New("invalid Kubernetes state metrics receiver configuration")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newReceiver(set, cfg, next)
}
