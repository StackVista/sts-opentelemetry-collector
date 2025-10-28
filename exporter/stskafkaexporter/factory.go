package stskafkaexporter

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

var (
	Type = component.MustNewType("sts_kafka_exporter") //nolint:gochecknoglobals
)

// Factory wraps the otel exporter.Factory but allows constructor injection for testing.
type Factory struct {
	exporter.Factory
	NewExporterFn func(Config, exporter.Settings) (InternalExporterComponent, error)
}

// NewFactory creates a new Kafka exporter factory.
func NewFactory() *Factory {
	f := &Factory{
		NewExporterFn: KafkaExporterConstructor, // default production constructor
	}
	f.Factory = exporter.NewFactory(
		Type,
		CreateDefaultConfig,
		exporter.WithLogs(f.CreateLogsExporter, component.StabilityLevelAlpha),
	)
	return f
}

func KafkaExporterConstructor(c Config, set exporter.Settings) (InternalExporterComponent, error) {
	return NewKafkaExporter(c, set) // returns KafkaExporter, which satisfies the interface
}

func CreateDefaultConfig() component.Config {
	queueSettings := exporterhelper.NewDefaultQueueConfig()
	queueSettings.NumConsumers = 1

	return &Config{
		TimeoutSettings: exporterhelper.NewDefaultTimeoutConfig(),
		BackOffConfig:   configretry.NewDefaultBackOffConfig(),
		QueueSettings:   queueSettings,

		Brokers:        []string{"localhost:9092"},
		Topic:          "sts-otel-topology",
		ReadTimeout:    2 * time.Second,
		ProduceTimeout: 5 * time.Second,
		RequiredAcks:   "leader",
	}
}

// CreateLogsExporter creates a new Kafka exporter for logs.
func (f *Factory) CreateLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	typedCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", cfg)
	}

	exp, err := f.NewExporterFn(*typedCfg, set)
	if err != nil {
		return nil, fmt.Errorf("cannot configure kafka logs exp: %w", err)
	}

	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		exp.ExportData,
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
		exporterhelper.WithTimeout(typedCfg.TimeoutSettings),
		exporterhelper.WithQueue(typedCfg.QueueSettings),
		exporterhelper.WithRetry(typedCfg.BackOffConfig),
	)
}
