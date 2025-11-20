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
	IncTopologyProduced(ctx context.Context, n int64, settingType settings.SettingType, signal settings.OtelInputSignal)
	IncMappingErrors(ctx context.Context, n int64, settingType settings.SettingType, signal settings.OtelInputSignal)
	RecordMappingDuration(
		ctx context.Context,
		d time.Duration,
		signal settings.OtelInputSignal,
		settingType settings.SettingType,
		mappingIdentifier string,
	)
	RecordRequestDuration(ctx context.Context, d time.Duration, signal settings.OtelInputSignal)

	IncMappingsRemoved(ctx context.Context, n int64, settingType settings.SettingType)
}

type NoopConnectorMetricsRecorder struct{}

func (n *NoopConnectorMetricsRecorder) IncTopologyProduced(
	_ context.Context, _ int64, _ settings.SettingType, _ settings.OtelInputSignal) {
}
func (n *NoopConnectorMetricsRecorder) IncMappingErrors(
	_ context.Context, _ int64, _ settings.SettingType, _ settings.OtelInputSignal) {
}
func (n *NoopConnectorMetricsRecorder) RecordMappingDuration(
	_ context.Context, _ time.Duration, _ settings.OtelInputSignal, _ settings.SettingType, _ string) {
}
func (n *NoopConnectorMetricsRecorder) RecordRequestDuration(
	_ context.Context, _ time.Duration, _ settings.OtelInputSignal) {
}
func (n *NoopConnectorMetricsRecorder) IncMappingsRemoved(_ context.Context, _ int64, _ settings.SettingType) {
}

type ConnectorMetrics struct {
	// Counters
	topologyTotal   metric.Int64Counter
	mappingsRemoved metric.Int64Counter
	errorsTotal     metric.Int64Counter

	// Histograms
	mappingDuration           metric.Float64Histogram
	requestProcessingDuration metric.Float64Histogram
}

// NewConnectorMetrics registers all metric instruments for this connector.
func NewConnectorMetrics(typeName string, telemetrySettings component.TelemetrySettings) *ConnectorMetrics {
	meter := telemetrySettings.MeterProvider.Meter(typeName)
	name := newMetricNameForType(typeName)

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
		metric.WithDescription("Time spent mapping a single input to topology"),
		metric.WithUnit("s"),
	)
	requestProcessingDuration, _ := meter.Float64Histogram(
		name("request_processing_duration_seconds"),
		metric.WithDescription("Total time spent processing a single input signal request"),
		metric.WithUnit("s"),
	)
	return &ConnectorMetrics{
		topologyTotal:             topologyTotal,
		mappingsRemoved:           mappingsRemoved,
		errorsTotal:               errorsTotal,
		mappingDuration:           mappingDuration,
		requestProcessingDuration: requestProcessingDuration,
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
	signal settings.OtelInputSignal,
	settingType settings.SettingType,
	mappingIdentifier string,
) {

	pm.mappingDuration.Record(ctx, d.Seconds(), metric.WithAttributeSet(
		attribute.NewSet(
			typeAttribute(settingType),
			attribute.String("signal", string(signal)),
			attribute.String("mapping", mappingIdentifier),
		)))
}

func (pm *ConnectorMetrics) RecordRequestDuration(
	ctx context.Context,
	d time.Duration,
	signal settings.OtelInputSignal,
) {

	pm.requestProcessingDuration.Record(ctx, d.Seconds(), metric.WithAttributeSet(
		attribute.NewSet(attribute.String("signal", string(signal))),
	))
}

func (pm *ConnectorMetrics) IncTopologyProduced(
	ctx context.Context,
	n int64,
	settingType settings.SettingType,
	signal settings.OtelInputSignal,
) {
	pm.topologyTotal.Add(ctx, n, metric.WithAttributeSet(topologyAttributes(settingType, signal)))
}

func (pm *ConnectorMetrics) IncMappingsRemoved(ctx context.Context, n int64, settingType settings.SettingType) {
	pm.mappingsRemoved.Add(ctx, n, metric.WithAttributeSet(
		attribute.NewSet(typeAttribute(settingType)),
	))
}

func (pm *ConnectorMetrics) IncMappingErrors(ctx context.Context, n int64, settingType settings.SettingType,
	signal settings.OtelInputSignal) {
	pm.errorsTotal.Add(ctx, n, metric.WithAttributeSet(topologyAttributes(settingType, signal)))
}

func topologyAttributes(settingType settings.SettingType, signal settings.OtelInputSignal) attribute.Set {
	return attribute.NewSet(
		typeAttribute(settingType),
		attribute.String("signal", string(signal)))
}

func typeAttribute(settingType settings.SettingType) attribute.KeyValue {
	switch settingType {
	case settings.SettingTypeOtelComponentMapping:
		return attribute.String("type", "component")
	case settings.SettingTypeOtelRelationMapping:
		return attribute.String("type", "relation")
	default:
		return attribute.String("type", string(settingType))
	}
}
