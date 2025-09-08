package tracetotopoconnector

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	stsSettingsApi "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
)

type connectorImpl struct {
	cfg              *Config
	logger           *zap.Logger
	logsConsumer     consumer.Logs
	settingsProvider stsSettingsApi.StsSettingsProvider
	subscriptionCh   <-chan stsSettingsEvents.UpdateSettingsEvent
}

func newConnector(cfg Config, logger *zap.Logger, nextConsumer consumer.Logs) *connectorImpl {
	logger.Info("Building tracetotopo connector")

	return &connectorImpl{
		cfg:          &cfg,
		logger:       logger,
		logsConsumer: nextConsumer,
	}
}

func (p *connectorImpl) Start(_ context.Context, host component.Host) error {
	settingsProviderExtensionID := component.MustNewID("sts_settings_provider")

	ext, ok := host.GetExtensions()[settingsProviderExtensionID]
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

	return nil
}

func (p *connectorImpl) Shutdown(_ context.Context) error {
	return nil
}

func (p *connectorImpl) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *connectorImpl) ConsumeTraces(ctx context.Context, _ ptrace.Traces) error {
	// TODO: use channel for updates and retrieve settings using settings provider api

	log := plog.NewLogs()
	scopeLog := log.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	scopeLog.Scope().SetName("test")   // TODO
	scopeLog.Scope().SetVersion("1.0") // TODO

	err := AddEvent(&scopeLog)
	if err != nil {
		return errors.New("unable to add event")
	}

	err = p.logsConsumer.ConsumeLogs(ctx, log)
	if err != nil {
		return errors.New("unable to consume logs")
	}

	return nil
}

func AddEvent(scopeLog *plog.ScopeLogs) error {
	logRecord := scopeLog.LogRecords().AppendEmpty()
	logRecord.Body().SetStr("todo")
	return nil
}
