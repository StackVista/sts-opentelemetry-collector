package stskafkaexporter_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"testing"
	"time"
)

type mockKafkaExporter struct {
	started  bool
	stopped  bool
	exported int
}

func (s *mockKafkaExporter) ExportData(_ context.Context, _ plog.Logs) error {
	s.exported++
	return nil
}

func (s *mockKafkaExporter) Start(_ context.Context, _ component.Host) error {
	s.started = true
	return nil
}

func (s *mockKafkaExporter) Shutdown(_ context.Context) error {
	s.stopped = true
	return nil
}

func TestFactory_CreateDefaultConfig(t *testing.T) {
	cfg := stskafkaexporter.CreateDefaultConfig()
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
	assert.NotNil(t, cfg)
}

func TestFactory_CreateExporter(t *testing.T) {
	mockExp := &mockKafkaExporter{}
	f := stskafkaexporter.NewFactory()

	f.NewExporterFn = func(_ stskafkaexporter.Config, _ exporter.CreateSettings) (stskafkaexporter.InternalExporterComponent, error) {
		return mockExp, nil
	}

	cfg := f.CreateDefaultConfig()

	exp, err := f.CreateLogsExporter(context.Background(), exportertest.NewNopCreateSettings(), cfg)
	require.NoError(t, err)
	require.NotNil(t, exp)

	// Start exporter
	err = exp.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	require.True(t, mockExp.started)

	// Send logs
	ld := plog.NewLogs()
	err = exp.ConsumeLogs(context.Background(), ld)
	require.NoError(t, err)

	// Give queue loop a chance to process
	require.Eventually(t, func() bool {
		return mockExp.exported == 1
	}, time.Second, 10*time.Millisecond)

	// Shutdown exporter
	err = exp.Shutdown(context.Background())
	require.NoError(t, err)
	require.True(t, mockExp.stopped)
}
