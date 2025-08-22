package tracetotopoconnector

import (
	"context"
	"fmt"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	stsSettingsApi "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal/events"
)

type connectorImpl struct {
	logger           *zap.Logger
	logsConsumer     consumer.Logs
	settingsProvider stsSettingsApi.StsSettingsProvider
	subscriptionCh   <-chan stsSettingsEvents.UpdateSettingsEvent
}

func newConnector(logger *zap.Logger, config component.Config, nextConsumer consumer.Logs) (*connectorImpl, error) {
	logger.Info("Building tracetotopo connector")

	return &connectorImpl{
		logger:       logger,
		logsConsumer: nextConsumer,
	}, nil
}

func (p *connectorImpl) Start(ctx context.Context, host component.Host) error {
	settingsProviderExtensionId := component.MustNewID("sts_settings_provider")

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

	subscriptionCh := p.settingsProvider.RegisterForUpdates(stsSettingsModel.SettingTypeOtelComponentMapping, stsSettingsModel.SettingTypeOtelRelationMapping)
	p.subscriptionCh = subscriptionCh

	return nil
}

func (p *connectorImpl) Shutdown(_ context.Context) error {
	return nil
}

func (p *connectorImpl) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *connectorImpl) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// TODO: use channel for updates and retrieve settings using settings provider api

	log := plog.NewLogs()
	scopeLog := log.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	scopeLog.Scope().SetName("test")   //TODO
	scopeLog.Scope().SetVersion("1.0") //TODO

	AddEvent(&scopeLog)
	p.logsConsumer.ConsumeLogs(ctx, log)

	return nil
}

func AddEvent(scopeLog *plog.ScopeLogs) error {
	logRecord := scopeLog.LogRecords().AppendEmpty()
	logRecord.Body().SetStr("todo")
	return nil
}
