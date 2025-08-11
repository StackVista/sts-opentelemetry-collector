package tracetotopoconnector

import (
	"context"
	"fmt"
	stsSettings "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type connectorImpl struct {
	logger       *zap.Logger
	logsConsumer consumer.Logs
}

func newConnector(logger *zap.Logger, config component.Config, nextConsumer consumer.Logs) (*connectorImpl, error) {
	logger.Info("Building tracetotopo connector")

	return &connectorImpl{
		logger:       logger,
		logsConsumer: nextConsumer,
	}, nil
}

func (p *connectorImpl) Start(ctx context.Context, host component.Host) error {
	p.logger.Info(">>> Starting the connector....")
	ext, found := host.GetExtensions()[component.NewID(component.MustNewType("settings_provider"))]
	if !found {
		return fmt.Errorf("settings_provider extension not found")
	}

	settingsProvider, ok := ext.(stsSettings.SettingsProvider)
	if !ok {
		return fmt.Errorf("extension is not of type SettingsProvider")
	}

	updates := settingsProvider.RegisterForUpdates()
	go func() {
		for {
			select {
			case <-updates:
				p.logger.Info("Received new settings update signal.")

			case <-ctx.Done():
				p.logger.Info("Connector shutting down, stopping settings listener.")
				return
			}
		}
	}()

	return nil
}

func (p *connectorImpl) Shutdown(_ context.Context) error {
	p.logger.Info(">>> Stopping the connector....")
	return nil
}

func (p *connectorImpl) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *connectorImpl) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
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
