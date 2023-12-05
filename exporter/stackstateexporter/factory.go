// Copyright StackState B.V.
// SPDX-License-Identifier: Apache-2.0
package stackstateexporter

import (
	"context"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	typeStr = "stackstate"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		typeStr,
		createDefaultConfig,
		exporter.WithTraces(createTraceExporter, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutSettings: exporterhelper.TimeoutSettings{
			Timeout: 15 * time.Second,
		},
		RetrySettings: exporterhelper.NewDefaultRetrySettings(),
		QueueSettings: exporterhelper.NewDefaultQueueSettings(),
	}
}

var _ exporter.CreateTracesFunc = createTraceExporter // compile-time check
func createTraceExporter(ctx context.Context, settings exporter.CreateSettings, cfg component.Config) (exporter.Traces, error) {
	exp, err := newTraceExporter(ctx, settings.Logger, cfg)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewTracesRequestExporter(ctx, settings, exp.RequestFromTraces)
}
