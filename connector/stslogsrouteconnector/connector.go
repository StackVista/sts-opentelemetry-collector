package stslogsrouteconnector

import (
	"context"
	"errors"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/pipeline"
)

type logsConnector struct {
	*logsagent.Delivery
	cfg    Config
	router connector.LogsRouterAndConsumer
}

func (c *logsConnector) Start(_ context.Context, host component.Host) error {
	controller, ok := host.GetExtensions()[c.cfg.ControllerExtension].(logsagent.Controller)
	if !ok {
		return errors.New("controller_extension does not provide a logs agent controller")
	}
	selectedID := c.cfg.PromtailPipeline
	switch controller.SelectedMode() {
	case logsagent.PromtailMode:
	case logsagent.OTELNativeMode:
		if c.cfg.OTELNativePipeline.Signal() != pipeline.SignalLogs {
			return errors.New("native mode requires native_pipeline")
		}
		selectedID = c.cfg.OTELNativePipeline
	default:
		return errors.New("controller_extension has no valid selected mode")
	}
	selected, err := c.router.Consumer(selectedID)
	if err != nil {
		return err
	}
	return c.Delivery.Start(controller, selected)
}
