package tracetotopoconnector

import (
	"context"
	"fmt"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	stsSettingsApi "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
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
	eval             *internal.CelEvaluator
	mapper           *internal.Mapper
	snapshotManager  *SnapshotManager
	subscriptionCh   <-chan stsSettingsEvents.UpdateSettingsEvent
	metricsRecorder  metrics.ConnectorMetricsRecorder
}

func newConnector(
	ctx context.Context,
	cfg Config,
	logger *zap.Logger,
	telemetrySettings component.TelemetrySettings,
	nextConsumer consumer.Logs,
) (*connectorImpl, error) {
	logger.Info("Building tracetotopo connector")
	eval, err := internal.NewCELEvaluator(
		ctx, cfg.ExpressionCacheSettings.ToMetered("cel_expression_cache", telemetrySettings),
	)
	if err != nil {
		return nil, err
	}

	return &connectorImpl{
		cfg:          &cfg,
		logger:       logger,
		logsConsumer: nextConsumer,
		eval:         eval,
		mapper: internal.NewMapper(
			ctx,
			cfg.TagRegexCacheSettings.ToMetered("tag_regex_cache", telemetrySettings),
			cfg.TagRegexCacheSettings.ToMetered("tag_template_cache", telemetrySettings),
		),
		snapshotManager: NewSnapshotManager(logger),
		metricsRecorder: metrics.NewConnectorMetrics(Type.String(), telemetrySettings),
	}, nil
}

func (p *connectorImpl) Start(ctx context.Context, host component.Host) error {
	ext, ok := host.GetExtensions()[component.MustNewID(stsSettingsApi.Type.String())]
	if !ok {
		return fmt.Errorf("%s extension not found", stsSettingsApi.Type.String())
	}

	// Cast to your interface
	settingsProvider, ok := ext.(stsSettingsApi.StsSettingsProvider)
	if !ok {
		return fmt.Errorf("extension does not implement StsSettingsProvider interface")
	}

	p.logger.Info("StsSettingsProvider extension found and bound to the tracetotopo connector")
	p.settingsProvider = settingsProvider

	subscriptionCh, err := p.settingsProvider.RegisterForUpdates(
		stsSettingsModel.SettingTypeOtelComponentMapping,
		stsSettingsModel.SettingTypeOtelRelationMapping,
	)
	if err != nil {
		return err
	}
	p.subscriptionCh = subscriptionCh

	// Update mappings on a separate goroutine when settings change
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-subscriptionCh:
				p.updateMappings(ctx)
			}
		}
	}()
	p.updateMappings(ctx)

	return nil
}

func (p *connectorImpl) Shutdown(_ context.Context) error {
	p.settingsProvider.Unregister(p.subscriptionCh)
	return nil
}

func (p *connectorImpl) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *connectorImpl) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	start := time.Now()
	collectionTimestampMs := start.UnixMilli()

	componentMappings, relationMappings := p.snapshotManager.Current()
	messages := internal.ConvertSpanToTopologyStreamMessage(
		ctx,
		p.logger,
		p.eval,
		p.mapper,
		td,
		componentMappings,
		relationMappings,
		collectionTimestampMs,
		p.metricsRecorder,
	)
	p.publishMessagesAsLogs(ctx, messages)

	duration := time.Since(start)
	p.metricsRecorder.RecordMappingDuration(
		ctx, duration,
		attribute.String("phase", "consume_traces"),
		attribute.String("target", "spans"),
		attribute.Int("mapping_count", len(componentMappings)+len(relationMappings)),
	)

	return nil
}

// updateMappings updates the mappings from the settings provider
func (p *connectorImpl) updateMappings(ctx context.Context) {
	newComponents, err := stsSettingsApi.GetSettingsAs[stsSettingsModel.OtelComponentMapping](p.settingsProvider)
	if err != nil {
		p.logger.Error("failed to get component mappings", zap.Error(err))
		return
	}

	newRelations, err := stsSettingsApi.GetSettingsAs[stsSettingsModel.OtelRelationMapping](p.settingsProvider)
	if err != nil {
		p.logger.Error("failed to get relation mappings", zap.Error(err))
		return
	}

	snapshotChange := p.snapshotManager.Update(newComponents, newRelations)
	if len(snapshotChange.RemovedComponents) == 0 && len(snapshotChange.RemovedRelations) == 0 {
		return
	}

	messages := internal.ConvertMappingRemovalsToTopologyStreamMessage(
		ctx, p.logger, snapshotChange.RemovedComponents, snapshotChange.RemovedRelations, p.metricsRecorder,
	)
	p.publishMessagesAsLogs(ctx, messages)
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
