package stskafkaexporter

import (
	"context"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
)

// exporterComponent is the interface that both production and stub exporters implement.
type exporterComponent interface {
	exportData(ctx context.Context, ld plog.Logs) error
	// export functions for other data types go here

	start(ctx context.Context, host component.Host) error
	shutdown(ctx context.Context) error
}

type kafkaExporter struct {
}

func newKafkaExporter(_ Config, _ exporter.CreateSettings) (*kafkaExporter, error) {
	return &kafkaExporter{}, nil
}

func (e *kafkaExporter) start(_ context.Context, _ component.Host) error {
	return nil
}

func (e *kafkaExporter) shutdown(_ context.Context) error {
	return nil
}

func (e *kafkaExporter) exportData(_ context.Context, _ plog.Logs) error {
	return nil
}
