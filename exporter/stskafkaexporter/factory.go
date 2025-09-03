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

// Factory wraps the otel exporter.Factory but allows constructor injection for testing.
type Factory struct {
	exporter.Factory
	NewExporterFn func(Config, exporter.CreateSettings) (InternalExporterComponent, error)
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

func KafkaExporterConstructor(c Config, set exporter.CreateSettings) (InternalExporterComponent, error) {
	return NewKafkaExporter(c, set) // returns KafkaExporter, which satisfies the interface
}

func CreateDefaultConfig() component.Config {
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
		RequiredAcks:   "leader",
	}
}

// CreateLogsExporter creates a new Kafka exporter for logs.
func (f *Factory) CreateLogsExporter(
	ctx context.Context,
	set exporter.CreateSettings,
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

	return exporterhelper.NewLogsExporter(
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
