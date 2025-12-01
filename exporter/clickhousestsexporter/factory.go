// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

//nolint:lll
package clickhousestsexporter // import "github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter/internal/metadata"
)

// NewFactory creates a factory for Elastic exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		CreateDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithMetrics(createMetricExporter, metadata.MetricsStability),
	)
}

func CreateDefaultConfig() component.Config {
	queueSettings := exporterhelper.NewDefaultQueueConfig()
	queueSettings.NumConsumers = 1

	return &Config{
		TimeoutSettings:              exporterhelper.NewDefaultTimeoutConfig(),
		QueueSettings:                queueSettings,
		BackOffConfig:                configretry.NewDefaultBackOffConfig(),
		ConnectionParams:             map[string]string{},
		Database:                     defaultDatabase,
		LogsTableName:                "otel_logs",
		TracesTableName:              "otel_traces",
		MetricsTableName:             "otel_metrics",
		ResourcesTableName:           "otel_resources",
		TopologyTableName:            "otel_topology",
		TopologyTimeRangeTableName:   "otel_topology_time_range",
		TopologyFieldValuesTableName: "otel_topology_field_values",
		TopologyTimeRangeMVName:      "otel_topology_time_range_mv",
		TopologyFieldValuesMVName:    "otel_topology_field_values_mv",
		CreateTracesTable:            true,
		CreateLogsTable:              true,
		CreateMetricsTable:           true,
		CreateResourcesTable:         true,
		CreateTopologyTable:          true,
		EnableLogs:                   true,
		EnableTopology:               true,
	}
}

// createLogsExporter creates a new exporter for logs.
// Logs are directly insert into clickhouse.
func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("unable to cast config")
	}

	exporter, err := NewLogsExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure clickhouse logs exporter: %w", err)
	}

	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		exporter.PushLogsData,
		exporterhelper.WithStart(exporter.Start),
		exporterhelper.WithShutdown(exporter.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
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

	exporter, err := NewTracesExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure clickhouse traces exporter: %w", err)
	}

	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		exporter.PushTraceData,
		exporterhelper.WithStart(exporter.Start),
		exporterhelper.WithShutdown(exporter.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func createMetricExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("unable to cast config")
	}

	exporter, err := NewMetricsExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure clickhouse metrics exporter: %w", err)
	}

	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		exporter.PushMetricsData,
		exporterhelper.WithStart(exporter.Start),
		exporterhelper.WithShutdown(exporter.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}
