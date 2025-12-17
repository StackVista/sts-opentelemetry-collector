package topologyconnector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
)

//nolint:gochecknoglobals
var (
	Type          = component.MustNewType("topology")
	globalFactory = &connectorFactory{}
)

type connectorFactory struct {
	celEvaluator         *internal.CelEvaluator
	mapper               *internal.Mapper
	deduplicator         internal.Deduplicator
	snapshotManager      *SnapshotManager
	expressionRefManager ExpressionRefManager
	init                 sync.Once
}

func (f *connectorFactory) initSharedState(
	ctx context.Context,
	logger *zap.Logger,
	telemetrySettings component.TelemetrySettings,
	connectorCfg *Config,
) error {
	var err error
	f.init.Do(func() {
		evaluator, e := internal.NewCELEvaluator(
			ctx, connectorCfg.ExpressionCache.ToMetered("cel_expression_cache", telemetrySettings),
		)
		if e != nil {
			err = e
			return
		}

		mapper := internal.NewMapper(
			ctx,
			connectorCfg.TagRegexCache.ToMetered("tag_regex_cache", telemetrySettings),
			connectorCfg.TagTemplateCache.ToMetered("tag_template_cache", telemetrySettings),
		)

		expressionRefManager := NewExpressionRefManager(logger, evaluator)
		snapshotManager := NewSnapshotManager(
			logger, []settings.OtelInputSignal{settings.TRACES, settings.METRICS}, expressionRefManager,
		)

		f.celEvaluator = evaluator
		f.deduplicator = internal.NewTopologyDeduplicator(
			ctx,
			logger,
			internal.DeduplicationConfig{
				Enabled:         connectorCfg.Deduplication.Enabled,
				RefreshFraction: connectorCfg.Deduplication.RefreshFraction,
				CacheConfig:     connectorCfg.Deduplication.Cache.ToMetered("deduplication_cache", telemetrySettings),
			},
		)
		f.mapper = mapper
		f.snapshotManager = snapshotManager
	})
	return err
}

// NewFactory returns a ConnectorFactory.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		Type,
		createDefaultConfig,
		connector.WithTracesToLogs(globalFactory.createTracesToLogsConnector, component.StabilityLevelAlpha),
		connector.WithMetricsToLogs(globalFactory.createMetricsToLogsConnector, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Deduplication: DeduplicationSettings{
			Enabled:         true,
			RefreshFraction: 0.5,
			Cache: CacheSettings{
				EnableMetrics: false,
				Size:          50000,
				TTL:           15 * time.Minute,
			},
		},
		ExpressionCache: CacheSettings{
			EnableMetrics: false,
			Size:          20000,
			TTL:           15 * time.Minute,
		},
		TagRegexCache: CacheSettings{
			EnableMetrics: false,
			Size:          2000,
			TTL:           15 * time.Minute,
		},
		TagTemplateCache: CacheSettings{
			EnableMetrics: false,
			Size:          2000,
			TTL:           15 * time.Minute,
		},
	}
}

func (f *connectorFactory) createTracesToLogsConnector(
	ctx context.Context,
	params connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (connector.Traces, error) {
	typedCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", cfg)
	}

	if err := f.initSharedState(ctx, params.Logger, params.TelemetrySettings, typedCfg); err != nil {
		return nil, err
	}

	return newConnector(
		ctx,
		*typedCfg,
		params.Logger,
		params.TelemetrySettings,
		nextConsumer,
		f.snapshotManager,
		f.expressionRefManager,
		f.celEvaluator,
		f.deduplicator,
		f.mapper,
		settings.TRACES,
	), nil
}

func (f *connectorFactory) createMetricsToLogsConnector(
	ctx context.Context,
	params connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (connector.Metrics, error) {
	typedCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", cfg)
	}

	if err := f.initSharedState(ctx, params.Logger, params.TelemetrySettings, typedCfg); err != nil {
		return nil, err
	}

	return newConnector(
		ctx,
		*typedCfg,
		params.Logger,
		params.TelemetrySettings,
		nextConsumer,
		f.snapshotManager,
		f.expressionRefManager,
		f.celEvaluator,
		f.deduplicator,
		f.mapper,
		settings.METRICS,
	), nil
}
