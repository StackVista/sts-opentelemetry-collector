package topologyconnector

import (
	"context"
	"fmt"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	stsSettingsApi "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type connectorImpl struct {
	cfg              *Config
	logger           *zap.Logger
	logsConsumer     consumer.Logs
	settingsProvider stsSettingsApi.StsSettingsProvider
	snapshotManager  *SnapshotManager
	eval             *internal.CelEvaluator
	mapper           *internal.Mapper
	metricsRecorder  metrics.ConnectorMetricsRecorder
	supportedSignal  settings.OtelInputSignal
}

func newConnector(
	ctx context.Context,
	cfg Config,
	logger *zap.Logger,
	telemetrySettings component.TelemetrySettings,
	nextConsumer consumer.Logs,
	supportedSignal settings.OtelInputSignal,
) (*connectorImpl, error) {
	logger.Info("Building topology connector")
	eval, err := internal.NewCELEvaluator(
		ctx, cfg.ExpressionCacheSettings.ToMetered("cel_expression_cache", telemetrySettings),
	)
	if err != nil {
		return nil, err
	}

	mapper := internal.NewMapper(
		ctx,
		cfg.TagRegexCacheSettings.ToMetered("tag_regex_cache", telemetrySettings),
		cfg.TagRegexCacheSettings.ToMetered("tag_template_cache", telemetrySettings),
	)
	snapshotManager := NewSnapshotManager(logger, supportedSignal)
	metricsRecorder := metrics.NewConnectorMetrics(Type.String(), telemetrySettings)

	return &connectorImpl{
		cfg:             &cfg,
		logger:          logger,
		logsConsumer:    nextConsumer,
		eval:            eval,
		mapper:          mapper,
		snapshotManager: snapshotManager,
		metricsRecorder: metricsRecorder,
		supportedSignal: supportedSignal,
	}, nil
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
	componentMappings, relationMappings := p.snapshotManager.Current()

	messagesWithKeys := internal.ConvertMetricsToTopologyStreamMessage(
		ctx,
		p.logger,
		p.eval,
		p.mapper,
		metrics,
		componentMappings,
		relationMappings,
		collectionTimestampMs,
		p.metricsRecorder,
	)

	duration := time.Since(start)
	p.publishMessagesAsLogs(ctx, messagesWithKeys)

	p.metricsRecorder.RecordMappingDuration(
		ctx, duration,
		attribute.String("phase", "consume_metrics"),
		attribute.String("target", "spans"),
	)

	return nil
}

func (p *connectorImpl) ConsumeTraces(ctx context.Context, traceData ptrace.Traces) error {
	start := time.Now()
	collectionTimestampMs := start.UnixMilli()

	componentMappings, relationMappings := p.snapshotManager.Current()
	messages := internal.ConvertSpanToTopologyStreamMessage(
		ctx,
		p.logger,
		p.eval,
		p.mapper,
		traceData,
		componentMappings,
		relationMappings,
		collectionTimestampMs,
		p.metricsRecorder,
	)

	duration := time.Since(start)

	p.publishMessagesAsLogs(ctx, messages)

	p.metricsRecorder.RecordMappingDuration(
		ctx, duration,
		attribute.String("phase", "consume_traces"),
		attribute.String("target", "spans"),
	)

	return nil
}

func (p *connectorImpl) handleMappingRemovals(
	ctx context.Context,
	removedComponentMappings []settings.OtelComponentMapping,
	removedRelationMappings []settings.OtelRelationMapping,
) {
	if len(removedComponentMappings) == 0 && len(removedRelationMappings) == 0 {
		return
	}

	msgs := internal.ConvertMappingRemovalsToTopologyStreamMessage(
		ctx, p.logger, removedComponentMappings, removedRelationMappings, p.metricsRecorder,
	)
	p.publishMessagesAsLogs(ctx, msgs)
}

func (p *connectorImpl) publishMessagesAsLogs(ctx context.Context, messages []internal.MessageWithKey) {
	if len(messages) == 0 {
		return
	}

	log := plog.NewLogs()
	scopeLog := log.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()

	for _, mwk := range messages {
		if err := addEvent(&scopeLog, mwk); err != nil {
			p.logger.Error("failed to add event to scope log", zap.Error(err))
			continue
		}
		p.logger.Debug("added event to scope log", zap.Any("key", mwk.Key))
	}

	if log.LogRecordCount() == 0 {
		return
	}

	if err := p.logsConsumer.ConsumeLogs(ctx, log); err != nil {
		p.logger.Error("Error sending logs to the next component", zap.Error(err))
	}
}

// addEvent adds a new event to the scope log. The event contains the body with serialized TopologyStreamMessage and
// attribute with serialized TopologyStreamMessageKey.
func addEvent(scopeLog *plog.ScopeLogs, mwk internal.MessageWithKey) error {
	msgAsBytes, err := proto.Marshal(mwk.Message)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	keyAsBytes, err := proto.Marshal(mwk.Key)
	if err != nil {
		return fmt.Errorf("marshal key: %w", err)
	}

	logRecord := scopeLog.LogRecords().AppendEmpty()
	logRecord.Body().SetEmptyBytes().FromRaw(msgAsBytes)
	logRecord.Attributes().PutEmptyBytes(stskafkaexporter.KafkaMessageKey).FromRaw(keyAsBytes)
	return nil
}
