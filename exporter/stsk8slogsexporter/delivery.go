package stsk8slogsexporter

import (
	"context"
	"errors"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
)

type deliveryExporter struct {
	exporter.Logs
	controllerID component.ID
	delivery     *logsagent.Delivery
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
	if controller.SelectedMode() != logsagent.PromtailMode {
		return errors.New("direct Promtail delivery requires Promtail mode")
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
	// Join admitted calls before exporterhelper stops its retry sender.
	if err := e.delivery.Shutdown(ctx); err != nil {
		return err
	}
	return e.Logs.Shutdown(ctx)
}
