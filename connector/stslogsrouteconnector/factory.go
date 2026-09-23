package stslogsrouteconnector

import (
	"context"
	"errors"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pipeline"
)

func NewFactory() connector.Factory {
	return connector.NewFactory(
		component.MustNewType("stslogsroute"),
		createDefaultConfig,
		connector.WithLogsToLogs(createLogsToLogs, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		DeliveryConfig: logsagent.DeliveryConfig{
			ControllerExtension: component.MustNewIDWithName("stslogsagent", "logs"),
			MaxConcurrentCalls:  8,
			MaxRecordBytes:      262144,
			MaxRequestBytes:     1048576,
			ExportLifetime:      90 * time.Second,
		},
		PromtailPipeline: pipeline.NewIDWithName(pipeline.SignalLogs, "promtail"),
	}
}

func createLogsToLogs(
	_ context.Context, set connector.Settings, config component.Config, next consumer.Logs,
) (connector.Logs, error) {
	cfg, ok := config.(*Config)
	if !ok {
		return nil, errors.New("invalid logs route configuration")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	router, ok := next.(connector.LogsRouterAndConsumer)
	if !ok {
		return nil, errors.New("logs route requires a logs pipeline router")
	}
	delivery, err := logsagent.NewDelivery(cfg.DeliveryConfig, set.MeterProvider, set.Logger)
	if err != nil {
		return nil, err
	}
	return &logsConnector{
		cfg:      *cfg,
		router:   router,
		Delivery: delivery,
	}, nil
}
