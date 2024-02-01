package stackstateprocessor

import (
	"context"

	"github.com/stackvista/sts-opentelemetry-collector/processor/stackstateprocessor/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(CreateTracesProcessor, metadata.TracesStability),
		processor.WithMetrics(CreateMetricsProcessor, metadata.MetricsStability),
		processor.WithLogs(CreateLogsProcessor, metadata.MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

var _ processor.CreateTracesFunc = CreateTracesProcessor   // compile-time type check
var _ processor.CreateMetricsFunc = CreateMetricsProcessor // compile-time type check

func CreateTracesProcessor(
	ctx context.Context,
	set processor.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	ssp, err := newStackstateprocessor(ctx, set.Logger, cfg)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewTracesProcessor(ctx, set, cfg, nextConsumer, ssp.ProcessTraces, processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}))
}

func CreateMetricsProcessor(
	ctx context.Context,
	set processor.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	ssp, err := newStackstateprocessor(ctx, set.Logger, cfg)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewMetricsProcessor(ctx, set, cfg, nextConsumer, ssp.ProcessMetrics, processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}))
}

func CreateLogsProcessor(
	ctx context.Context,
	set processor.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	ssp, err := newStackstateprocessor(ctx, set.Logger, cfg)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewLogsProcessor(ctx, set, cfg, nextConsumer, ssp.ProcessLogs, processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}))
}
