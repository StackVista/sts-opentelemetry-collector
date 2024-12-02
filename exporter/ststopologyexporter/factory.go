// Copyright StackState B.V.
// SPDX-License-Identifier: Apache-2.0
package ststopologyexporter

import (
	"context"
	"fmt"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/ststopologyexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutSettings: exporterhelper.TimeoutSettings{
			Timeout: 15 * time.Second,
		},
		QueueSettings: exporterhelper.NewDefaultQueueSettings(),
		Endpoint:      "none",
	}
}

var _ exporter.CreateMetricsFunc = createMetricsExporter // compile-time check
func createMetricsExporter(ctx context.Context, settings exporter.CreateSettings, cfg component.Config) (exporter.Metrics, error) {
	exp, err := newTopologyExporter(settings.Logger, cfg)
	if err != nil {
		return nil, err
	}
	c := cfg.(*Config)

	return exporterhelper.NewMetricsExporter(
		ctx,
		settings,
		cfg,
		exp.ConsumeMetrics,
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
	)
}

// createTracesExporter creates a new exporter for traces.
// Traces are directly insert into clickhouse.
func createTracesExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Traces, error) {
	c := cfg.(*Config)
	exporter, err := newTopologyExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure clickhouse traces exporter: %w", err)
	}

	return exporterhelper.NewTracesExporter(
		ctx,
		set,
		cfg,
		exporter.ConsumeTraces,
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
	)
}
