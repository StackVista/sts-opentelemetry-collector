// Copyright StackState B.V.
// SPDX-License-Identifier: Apache-2.0
package ststopologyexporter

import (
	"context"
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
