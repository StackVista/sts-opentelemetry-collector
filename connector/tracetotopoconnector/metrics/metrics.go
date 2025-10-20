package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type ConnectorMetricsRecorder interface {
	IncSpansProcessed(ctx context.Context, n int64)
	IncMappingsProduced(ctx context.Context, n int64, settingType settings.SettingType, attrs ...attribute.KeyValue)
	IncSettingsRemoved(ctx context.Context, n int64, settingType settings.SettingType)
	IncMappingErrors(ctx context.Context, n int64, settingType settings.SettingType)
	RecordMappingDuration(ctx context.Context, d time.Duration, labels ...attribute.KeyValue)
}

type ConnectorMetrics struct {
	// Counters
	spansProcessed metric.Int64Counter
	mappingsTotal  metric.Int64Counter
	errorsTotal    metric.Int64Counter

	// Histograms
	mappingDuration metric.Float64Histogram
}

// NewConnectorMetrics registers all metric instruments for this connector.
func NewConnectorMetrics(typeName string, meterProvider metric.MeterProvider) *ConnectorMetrics  {
	meter := meterProvider.Meter(typeName)
	name := newMetricNameForType(typeName)

	spansProcessed, _ := meter.Int64Counter(
		name("spans_processed_total"),
		metric.WithDescription("Total number of spans processed by the connector"),
	)
	mappingsTotal, _ := meter.Int64Counter(
		name("mappings_produced_total"),
		metric.WithDescription("Total number of mappings produced"),
	)
	errorsTotal, _ := meter.Int64Counter(
		name("mapping_errors_total"),
		metric.WithDescription("Total number of mapping errors"),
	)
	mappingDuration, _ := meter.Float64Histogram(
		name("mapping_duration_seconds"),
		metric.WithDescription("Time spent converting spans to topology stream messages"),
		metric.WithUnit("s"),
	)

	return &ConnectorMetrics{
		spansProcessed:  spansProcessed,
		mappingsTotal:   mappingsTotal,
		errorsTotal:     errorsTotal,
		mappingDuration: mappingDuration,
	}
}

func newMetricNameForType(typeName string) func(string) string {
	return func(name string) string {
		return fmt.Sprintf("connector.%s.%s", typeName, name)
	}
}

func (pm *ConnectorMetrics) RecordMappingDuration(
	ctx context.Context,
	d time.Duration,
	attrs ...attribute.KeyValue,
) {
	pm.mappingDuration.Record(ctx, d.Seconds(), metric.WithAttributes(attrs...))
}

func (pm *ConnectorMetrics) IncSpansProcessed(ctx context.Context, n int64) {
	pm.spansProcessed.Add(ctx, n)
}

func (pm *ConnectorMetrics) IncMappingsProduced(
	ctx context.Context,
	n int64,
	settingType settings.SettingType,
	attrs ...attribute.KeyValue,
) {
	attrs = append(attrs, attribute.String("setting_type", string(settingType)))
	pm.mappingsTotal.Add(ctx, n, metric.WithAttributes(attrs...))
}

func (pm *ConnectorMetrics) IncSettingsRemoved(ctx context.Context, n int64, settingType settings.SettingType) {
	pm.mappingsTotal.Add(ctx, n, metric.WithAttributes(attribute.String("setting_type", string(settingType))))
}

func (pm *ConnectorMetrics) IncMappingErrors(ctx context.Context, n int64, settingType settings.SettingType) {
	pm.errorsTotal.Add(ctx, n, metric.WithAttributes(attribute.String("setting_type", string(settingType))))
}
