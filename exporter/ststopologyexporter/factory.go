// Copyright StackState B.V.
// SPDX-License-Identifier: Apache-2.0
package ststopologyexporter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/ststopologyexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

//nolint:gochecknoglobals
var processorCapabilities = consumer.Capabilities{MutatesData: true}

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
		TimeoutSettings: exporterhelper.TimeoutConfig{
			Timeout: 15 * time.Second,
		},
		QueueSettings: exporterhelper.NewDefaultQueueConfig(),
		Endpoint:      "none",
	}
}

var _ exporter.CreateMetricsFunc = createMetricsExporter // compile-time check
func createMetricsExporter(
	ctx context.Context,
	settings exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	exp, err := NewTopologyExporter(settings.Logger, cfg)
	if err != nil {
		return nil, err
	}
	c, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("unable to cast config")
	}

	return exporterhelper.NewMetrics(
		ctx,
		settings,
		cfg,
		exp.ConsumeMetrics,
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithCapabilities(processorCapabilities),
	)
}

// createTracesExporter creates a new exporter for traces.
// Traces are directly insert into clickhouse.
func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("unable to cast config")
	}
	exporter, err := NewTopologyExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure clickhouse traces exporter: %w", err)
	}

	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		exporter.ConsumeTraces,
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithCapabilities(processorCapabilities),
	)
}
