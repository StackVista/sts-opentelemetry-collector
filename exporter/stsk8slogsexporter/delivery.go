package stsk8slogsexporter

import (
	"context"
	"errors"
	"sync"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
)

type deliveryExporter struct {
	exporter.Logs
	controllerID component.ID
	delivery     *logsagent.Delivery
	shutdownOnce sync.Once
	shutdownErr  error
}

func newDeliveryExporter(
	inner exporter.Logs, cfg logsagent.DeliveryConfig, set exporter.Settings,
) (exporter.Logs, error) {
	delivery, err := logsagent.NewDelivery(cfg, set.MeterProvider, set.Logger)
	if err != nil {
		return nil, err
	}
	return &deliveryExporter{Logs: inner, controllerID: cfg.ControllerExtension, delivery: delivery}, nil
}

func (e *deliveryExporter) Start(ctx context.Context, host component.Host) error {
	controller, ok := host.GetExtensions()[e.controllerID].(logsagent.Controller)
	if !ok {
		return errors.New("delivery.controller_extension does not provide a logs agent controller")
	}
	if err := e.Logs.Start(ctx, host); err != nil {
		return err
	}
	return e.delivery.Start(controller, e.Logs)
}

func (e *deliveryExporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	return e.delivery.ConsumeLogs(ctx, logs)
}

func (e *deliveryExporter) Shutdown(ctx context.Context) error {
	e.shutdownOnce.Do(func() {
		// Admitted calls retain their existing deadlines until joined, even if shutdown is canceled.
		drainErr := e.delivery.Shutdown(context.WithoutCancel(ctx))
		cleanupErr := e.Logs.Shutdown(context.WithoutCancel(ctx))
		e.shutdownErr = errors.Join(ctx.Err(), drainErr, cleanupErr)
	})
	return e.shutdownErr
}
