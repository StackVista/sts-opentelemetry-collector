// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package stsusageprocessor // import "github.com/stackvista/sts-opentelemetry-collector/processor/stsusageprocessor"

import (
	"context"
	"github.com/stackvista/sts-opentelemetry-collector/processor/stsusageprocessor/internal/metadata"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

func GetProcessorCapabilities() *consumer.Capabilities {
	return &consumer.Capabilities{MutatesData: true}
}

// NewFactory returns a new factory for the Resource processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
		processor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
		processor.WithLogs(createLogsProcessor, metadata.LogsStability))
}

// Note: This isn't a valid configuration because the processor would do no work.
func createDefaultConfig() component.Config {
	return &Config{}
}

func createTracesProcessor(
	ctx context.Context,
	set processor.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	meter := set.MeterProvider.Meter("stsusageprocessor")
	tracesBytesCounter, _ := meter.Int64Counter(
		"suse_observability_usage_bytes_accepted_traces",
		metric.WithDescription("Uncompressed amount of bytes received by any traces"),
		metric.WithUnit("By"),
	)

	proc := &stsUsageProcessor{logger: set.Logger, sizer: &ptrace.ProtoMarshaler{}, tracesBytes: tracesBytesCounter}
	return processorhelper.NewTracesProcessor(
		ctx,
		set,
		cfg,
		nextConsumer,
		proc.processTraces,
		processorhelper.WithCapabilities(*GetProcessorCapabilities()))
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	proc := &stsUsageProcessor{logger: set.Logger}
	return processorhelper.NewMetricsProcessor(
		ctx,
		set,
		cfg,
		nextConsumer,
		proc.processMetrics,
		processorhelper.WithCapabilities(*GetProcessorCapabilities()))
}

func createLogsProcessor(
	ctx context.Context,
	set processor.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	proc := &stsUsageProcessor{logger: set.Logger}
	return processorhelper.NewLogsProcessor(
		ctx,
		set,
		cfg,
		nextConsumer,
		proc.processLogs,
		processorhelper.WithCapabilities(*GetProcessorCapabilities()))
}
