package stskafkaexporter

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"time"
)

var (
	Type = component.MustNewType("stskafkaexporter") //nolint:gochecknoglobals
)

// NewFactory creates a factory for the stskafkaexporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	queueSettings := exporterhelper.NewDefaultQueueSettings()
	queueSettings.NumConsumers = 1

	return &Config{
		TimeoutSettings: exporterhelper.NewDefaultTimeoutSettings(),
		BackOffConfig:   configretry.NewDefaultBackOffConfig(),
		QueueSettings:   queueSettings,

		Brokers:        []string{"localhost:9092"},
		Topic:          "otel-topology-stream",
		ReadTimeout:    2 * time.Second,
		ProduceTimeout: 5 * time.Second,
	}
}

func kafkaExporterConstructor(c Config, set exporter.CreateSettings) (exporterComponent, error) {
	return newKafkaExporter(c, set) // returns *kafkaExporter, which satisfies the interface
}

var newKafkaExporterFn = kafkaExporterConstructor //nolint:gochecknoglobals

// createLogsExporter creates a new Kafka exporter for logs.
func createLogsExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Logs, error) {
	typedCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", cfg)
	}

	exp, err := newKafkaExporterFn(*typedCfg, set)
	if err != nil {
		return nil, fmt.Errorf("cannot configure kafka logs exp: %w", err)
	}

	// Use exporterhelper to get built-in queue, retry, timeouts (configurable in the collector-config.yaml).
	return exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		exp.exportData,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		exporterhelper.WithTimeout(typedCfg.TimeoutSettings),
		exporterhelper.WithQueue(typedCfg.QueueSettings),
		exporterhelper.WithRetry(typedCfg.BackOffConfig),
	)
}
