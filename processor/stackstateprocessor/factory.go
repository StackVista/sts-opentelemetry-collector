package stackstateprocessor

import (
	"context"

	"github.com/stackvista/sts-opentelemetry-collector/processor/stackstateprocessor/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(CreateTracesProcessor, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

var _ processor.CreateTracesFunc = CreateTracesProcessor // compile-time type check

func CreateTracesProcessor(
	ctx context.Context,
	set processor.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	return newStackstateprocessor(ctx, set.Logger, cfg, nextConsumer)
}
