package tracetotopoconnector

import (
	"context"
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
	return nil
}

func (p *connectorImpl) Shutdown(_ context.Context) error {
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
