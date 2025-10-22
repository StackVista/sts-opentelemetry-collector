package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type ConnectorMetricsRecorder interface {
	IncInputsProcessed(ctx context.Context, n int64, signal settings.OtelInputSignal)

	IncTopologyProduced(ctx context.Context, n int64, settingType settings.SettingType, signal settings.OtelInputSignal)
	IncMappingErrors(ctx context.Context, n int64, settingType settings.SettingType, signal settings.OtelInputSignal)
	RecordMappingDuration(ctx context.Context, d time.Duration, labels ...attribute.KeyValue)

	IncMappingsRemoved(ctx context.Context, n int64, settingType settings.SettingType)
}

type ConnectorMetrics struct {
	// Counters
	inputsProcessed metric.Int64Counter
	mappingsTotal   metric.Int64Counter
	mappingsRemoved metric.Int64Counter
	errorsTotal     metric.Int64Counter

	// Histograms
	mappingDuration metric.Float64Histogram
}

// NewConnectorMetrics registers all metric instruments for this connector.
func NewConnectorMetrics(typeName string, telemetrySettings component.TelemetrySettings) *ConnectorMetrics {
	meter := telemetrySettings.MeterProvider.Meter(typeName)
	name := newMetricNameForType(typeName)

	inputsProcessed, _ := meter.Int64Counter(
		name("inputs_processed_total"),
		metric.WithDescription("Total number of spans processed by the connector"),
	)
	topologyTotal, _ := meter.Int64Counter(
		name("topology_produced_total"),
		metric.WithDescription("Total number of topology elements produced"),
	)
	mappingsRemoved, _ := meter.Int64Counter(
		name("mappings_removed_total"),
		metric.WithDescription("Total number of mappings removed"),
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
		inputsProcessed: inputsProcessed,
		mappingsTotal:   topologyTotal,
		mappingsRemoved: mappingsRemoved,
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

func (pm *ConnectorMetrics) IncInputsProcessed(ctx context.Context, n int64, signal settings.OtelInputSignal) {
	pm.inputsProcessed.Add(ctx, n, metric.WithAttributeSet(attribute.NewSet(attribute.String("signal", string(signal)))))
}

func (pm *ConnectorMetrics) IncTopologyProduced(
	ctx context.Context,
	n int64,
	settingType settings.SettingType,
	signal settings.OtelInputSignal,
) {
	pm.mappingsTotal.Add(ctx, n, metric.WithAttributeSet(topologyAttributes(settingType, signal)))
}

func (pm *ConnectorMetrics) IncMappingsRemoved(ctx context.Context, n int64, settingType settings.SettingType) {
	pm.mappingsRemoved.Add(ctx, n, metric.WithAttributeSet(
		attribute.NewSet(attribute.String("setting_type", string(settingType))),
	))
}

func (pm *ConnectorMetrics) IncMappingErrors(ctx context.Context, n int64, settingType settings.SettingType,
	signal settings.OtelInputSignal) {
	pm.errorsTotal.Add(ctx, n, metric.WithAttributeSet(topologyAttributes(settingType, signal)))
}

func topologyAttributes(settingType settings.SettingType, signal settings.OtelInputSignal) attribute.Set {
	return attribute.NewSet(
		attribute.String("setting_type", string(settingType)),
		attribute.String("signal", string(signal)))
}
