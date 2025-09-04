package tracetotopoconnector

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

//nolint:gochecknoglobals
var (
	Type = component.MustNewType("tracetotopo")
)

// NewFactory returns a ConnectorFactory.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		Type,
		createDefaultConfig,
		connector.WithTracesToLogs(createTracesToLogsConnector, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ExpressionCacheSettings: ExpressionCacheSettings{
			Size: 1000,
			TTL:  15 * time.Minute,
		},
	}
}

func createTracesToLogsConnector(
	_ context.Context,
	params connector.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (connector.Traces, error) {
	typedCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", cfg)
	}

	return newConnector(*typedCfg, params.Logger, nextConsumer)
}
