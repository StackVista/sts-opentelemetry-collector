package topologyconnector

import (
	"context"
	"fmt"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	stsSettingsApi "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type connectorImpl struct {
	cfg                  *Config
	logger               *zap.Logger
	settingsProvider     stsSettingsApi.StsSettingsProvider
	snapshotManager      *SnapshotManager
	expressionRefManager ExpressionRefManager
	metadataPublisher    *MetadataPublisher
	streamPublisher      *TopologyStreamPublisher
	eval                 internal.ExpressionEvaluator
	deduplicator         internal.Deduplicator
	mapper               *internal.Mapper
	metricsRecorder      metrics.ConnectorMetricsRecorder
	supportedSignal      settingsproto.OtelInputSignal
}

func newConnector(
	_ context.Context,
	cfg Config,
	logger *zap.Logger,
	telemetrySettings component.TelemetrySettings,
	_ consumer.Logs,
	snapshotManager *SnapshotManager,
	expressionRefManager ExpressionRefManager,
	metadataPublisher *MetadataPublisher,
	streamPublisher *TopologyStreamPublisher,
	eval internal.ExpressionEvaluator,
	deduplicator internal.Deduplicator,
	mapper *internal.Mapper,
	supportedSignal settingsproto.OtelInputSignal,
) *connectorImpl {
	logger.Info("Building topology connector")
	return &connectorImpl{
		cfg:                  &cfg,
		logger:               logger,
		eval:                 eval,
		deduplicator:         deduplicator,
		mapper:               mapper,
		snapshotManager:      snapshotManager,
		expressionRefManager: expressionRefManager,
		metadataPublisher:    metadataPublisher,
		streamPublisher:      streamPublisher,
		metricsRecorder:      metrics.NewConnectorMetrics(Type.String(), telemetrySettings),
		supportedSignal:      supportedSignal,
	}
}

func (p *connectorImpl) Start(ctx context.Context, host component.Host) error {
	settingsProvider, err := resolveSettingsProvider(host, p.logger)
	if err != nil {
		return err
	}
	p.settingsProvider = settingsProvider

	if err := p.snapshotManager.Start(ctx, settingsProvider, p.handleMappingRemovals); err != nil {
		return fmt.Errorf("failed to start snapshot manager: %w", err)
	}

	return nil
}

func resolveSettingsProvider(host component.Host, logger *zap.Logger) (stsSettingsApi.StsSettingsProvider, error) {
	ext, ok := host.GetExtensions()[component.MustNewID(stsSettingsApi.Type.String())]
	if !ok {
		return nil, fmt.Errorf("%s extension not found", stsSettingsApi.Type.String())
	}
	provider, ok := ext.(stsSettingsApi.StsSettingsProvider)
	if !ok {
		return nil, fmt.Errorf("extension does not implement StsSettingsProvider interface")
	}
	logger.Info("Resolved StsSettingsProvider extension")
	return provider, nil
}

func (p *connectorImpl) Shutdown(_ context.Context) error {
	p.snapshotManager.Stop()
	return nil
}

func (p *connectorImpl) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *connectorImpl) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	start := time.Now()
	collectionTimestampMs := time.Now().UnixMilli()

	componentMappings, relationMappings := p.snapshotManager.Current(p.supportedSignal)
	messagesWithKeys := internal.ConvertMetricsToTopologyStreamMessage(
		ctx,
		p.logger,
		p.eval,
		p.deduplicator,
		p.mapper,
		metrics,
		componentMappings,
		relationMappings,
		collectionTimestampMs,
		p.metricsRecorder,
	)

	duration := time.Since(start)
	p.streamPublisher.Publish(ctx, messagesWithKeys)

	p.metricsRecorder.RecordRequestDuration(
		ctx, duration,
		settingsproto.METRICS,
	)

	return nil
}

func (p *connectorImpl) ConsumeTraces(ctx context.Context, traceData ptrace.Traces) error {
	start := time.Now()
	collectionTimestampMs := start.UnixMilli()

	componentMappings, relationMappings := p.snapshotManager.Current(p.supportedSignal)
	messages := internal.ConvertSpanToTopologyStreamMessage(
		ctx,
		p.logger,
		p.eval,
		p.deduplicator,
		p.mapper,
		traceData,
		componentMappings,
		relationMappings,
		collectionTimestampMs,
		p.metricsRecorder,
	)

	duration := time.Since(start)
	p.streamPublisher.Publish(ctx, messages)

	p.metricsRecorder.RecordRequestDuration(
		ctx, duration,
		settingsproto.TRACES,
	)

	return nil
}

func (p *connectorImpl) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	start := time.Now()
	collectionTimestampMs := start.UnixMilli()

	componentMappings, relationMappings := p.snapshotManager.Current(p.supportedSignal)
	messages := internal.ConvertLogToTopologyStreamMessage(
		ctx,
		p.logger,
		p.eval,
		p.deduplicator,
		p.mapper,
		logs,
		componentMappings,
		relationMappings,
		collectionTimestampMs,
		p.metricsRecorder,
	)

	duration := time.Since(start)
	p.streamPublisher.Publish(ctx, messages)

	p.metricsRecorder.RecordRequestDuration(
		ctx, duration,
		settingsproto.LOGS,
	)

	return nil
}

func (p *connectorImpl) handleMappingRemovals(
	ctx context.Context,
	removedComponentMappings []settingsproto.OtelComponentMapping,
	removedRelationMappings []settingsproto.OtelRelationMapping,
) {
	if len(removedComponentMappings) == 0 && len(removedRelationMappings) == 0 {
		return
	}

	msgs := internal.ConvertMappingRemovalsToTopologyStreamMessage(
		ctx, p.logger, removedComponentMappings, removedRelationMappings, p.metricsRecorder,
	)
	p.streamPublisher.Publish(ctx, msgs)

	var removed []settingsproto.SettingExtension
	for i := range removedComponentMappings {
		removed = append(removed, removedComponentMappings[i])
	}
	for i := range removedRelationMappings {
		removed = append(removed, removedRelationMappings[i])
	}

	dataSources := make([]string, 0, len(removed))
	for _, r := range removed {
		dataSources = append(dataSources, r.GetIdentifier())
	}
	p.streamPublisher.OnMappingRemoved(dataSources)

	p.metadataPublisher.PublishTombstones(removed)
}

