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
		ExpressionCacheSettings: CacheSettings{
			EnableMetrics: false,
			Size:          20000,
			TTL:           15 * time.Minute,
		},
		TagRegexCacheSettings: CacheSettings{
			EnableMetrics: false,
			Size:          2000,
			TTL:           15 * time.Minute,
		},
		TagTemplateCacheSettings: CacheSettings{
			EnableMetrics: false,
			Size:          2000,
			TTL:           15 * time.Minute,
		},
	}
}

func createTracesToLogsConnector(
	ctx context.Context,
	params connector.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (connector.Traces, error) {
	typedCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", cfg)
	}

	return newConnector(
		ctx,
		*typedCfg,
		params.Logger,
		params.TelemetrySettings,
		nextConsumer,
	)
}
