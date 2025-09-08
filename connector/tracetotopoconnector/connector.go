package tracetotopoconnector

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	stsSettingsApi "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var settingsProviderExtensionId = component.MustNewID("sts_settings_provider")

type connectorImpl struct {
	cfg              *Config
	logger            *zap.Logger
	logsConsumer      consumer.Logs
	settingsProvider  stsSettingsApi.StsSettingsProvider
	eval              *internal.CELEvaluator
	componentMappings *[]stsSettingsModel.OtelComponentMapping
	relationMappings  *[]stsSettingsModel.OtelRelationMapping
	subscriptionCh    <-chan stsSettingsEvents.UpdateSettingsEvent
}

func newConnector(cfg Config, logger *zap.Logger, nextConsumer consumer.Logs) (*connectorImpl, error) {
	logger.Info("Building tracetotopo connector")
	eval, err := internal.NewCELEvaluator()
	if err != nil {
		return nil, err
	}

	return &connectorImpl{
		cfg:          &cfg,
		logger:            logger,
		logsConsumer:      nextConsumer,
		eval:              eval,
		componentMappings: &[]stsSettingsModel.OtelComponentMapping{},
		relationMappings:  &[]stsSettingsModel.OtelRelationMapping{},
	}, nil
}

func (p *connectorImpl) Start(ctx context.Context, host component.Host) error {
	ext, ok := host.GetExtensions()[settingsProviderExtensionId]
	if !ok {
		return fmt.Errorf("sts_settings_provider extension not found")
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
				p.updateMappings()
			}
		}
	}()
	p.updateMappings()

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
	log := plog.NewLogs()
	scopeLog := log.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()

	messagesWithKeys := internal.ConvertSpanToTopologyStreamMessage(p.eval, td, *p.componentMappings, *p.relationMappings, time.Now().UnixMilli())
	for _, mwk := range messagesWithKeys {
		if err := addEvent(&scopeLog, &mwk); err != nil {
			p.logger.Error("failed to add event to scope log", zap.Error(err))
		} else {
			p.logger.Debug("added event to scope log", zap.Any("key", mwk.Key))
		}
	}
	if log.LogRecordCount() > 0 {
		p.logsConsumer.ConsumeLogs(ctx, log)
	}
	return nil
}

// updateMappings updates the mappings from the settings provider
func (p *connectorImpl) updateMappings() {
	if componentMappings, err := stsSettingsApi.GetSettingsAs[stsSettingsModel.OtelComponentMapping](p.settingsProvider, stsSettingsModel.SettingTypeOtelComponentMapping); err != nil {
		p.logger.Error("failed to get component mappings", zap.Error(err))
	} else {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&p.componentMappings)), unsafe.Pointer(&componentMappings))
	}
	if relationMappings, err := stsSettingsApi.GetSettingsAs[stsSettingsModel.OtelRelationMapping](p.settingsProvider, stsSettingsModel.SettingTypeOtelRelationMapping); err != nil {
		p.logger.Error("failed to get relation mappings", zap.Error(err))
	} else {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&p.relationMappings)), unsafe.Pointer(&relationMappings))
	}
}

// addEvent adds a new event to the scope log. The event contains the body with serialized TopologyStreamMessage and
// attribute with serialized TopologyStreamMessageKey.
func addEvent(scopeLog *plog.ScopeLogs, mwk *internal.MessageWithKey) error {
	logRecord := scopeLog.LogRecords().AppendEmpty()

	if msgAsBytes, err := proto.Marshal(mwk.Message); err != nil {
		return err
	} else {
		logRecord.Body().SetEmptyBytes().FromRaw(msgAsBytes)
	}

	if keyAsBytes, err := proto.Marshal(mwk.Key); err != nil {
		return err
	} else {
		logRecord.Attributes().PutEmptyBytes(stskafkaexporter.KafkaMessageKey).FromRaw(keyAsBytes)
	}
	return nil
}
