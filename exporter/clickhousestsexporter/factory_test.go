// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhousestsexporter_test

import (
	"context"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := clickhousestsexporter.NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestFactory_CreateLogsExporter(t *testing.T) {
	factory := clickhousestsexporter.NewFactory()
	cfg := withDefaultConfig(func(cfg *clickhousestsexporter.Config) {
		cfg.Endpoint = defaultEndpoint
	})
	params := exportertest.NewNopSettings(metadata.Type)
	exporter, err := factory.CreateLogs(context.Background(), params, cfg)
	require.NoError(t, err)
	require.NotNil(t, exporter)

	require.NoError(t, exporter.Shutdown(context.TODO()))
}

func TestFactory_CreateTracesExporter(t *testing.T) {
	factory := clickhousestsexporter.NewFactory()
	cfg := withDefaultConfig(func(cfg *clickhousestsexporter.Config) {
		cfg.Endpoint = defaultEndpoint
	})
	params := exportertest.NewNopSettings(metadata.Type)
	exporter, err := factory.CreateTraces(context.Background(), params, cfg)
	require.NoError(t, err)
	require.NotNil(t, exporter)

	require.NoError(t, exporter.Shutdown(context.TODO()))
}

func TestFactory_CreateMetricsExporter(t *testing.T) {
	factory := clickhousestsexporter.NewFactory()
	cfg := withDefaultConfig(func(cfg *clickhousestsexporter.Config) {
		cfg.Endpoint = defaultEndpoint
	})
	params := exportertest.NewNopSettings(metadata.Type)
	exporter, err := factory.CreateMetrics(context.Background(), params, cfg)
	require.NoError(t, err)
	require.NotNil(t, exporter)

	require.NoError(t, exporter.Shutdown(context.TODO()))
}
