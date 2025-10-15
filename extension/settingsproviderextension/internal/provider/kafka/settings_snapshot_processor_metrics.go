package kafka

import (
	"context"
	"fmt"

	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type MetricsRecorder interface {
	IncIncompleteSnapshots(ctx context.Context, settingType stsSettingsModel.SettingType)
	IncCompleteSnapshots(ctx context.Context, settingType stsSettingsModel.SettingType)
	RecordSettingsCount(ctx context.Context, n int64, settingType stsSettingsModel.SettingType)
	RecordSettingsSize(ctx context.Context, n int64, settingType stsSettingsModel.SettingType)
}

type Metrics struct {
	// Counters
	incompleteSnapshotsCounter metric.Int64Counter
	completeSnapshotsCounter   metric.Int64Counter

	// Histograms
	settingsCountHistogram metric.Int64Histogram
	settingsSizeHistogram  metric.Int64Histogram
}

func NewSettingsSnapshotProcessorMetrics(telemetrySettings component.TelemetrySettings) *Metrics {
	meter := telemetrySettings.MeterProvider.Meter("settings_snapshot_processor")

	incompleteSnapshotsCounter, _ := meter.Int64Counter(
		metricName("settings_snapshot_incomplete_total"),
		metric.WithDescription("Total number of incomplete snapshots (replaced by new starts)"),
	)
	successfulEndsCounter, _ := meter.Int64Counter(
		metricName("settings_snapshot_complete_total"),
		metric.WithDescription("Total number of successful snapshots (start,settings,end) processed"),
	)
	settingsCountHistogram, _ := meter.Int64Histogram(
		metricName("settings_count_by_type"),
		metric.WithDescription("Distribution of setting count by type in completed snapshots"),
		metric.WithUnit("settings"),
	)
	settingsSizeHistogram, _ := meter.Int64Histogram(
		metricName("settings_size_by_type_bytes"),
		metric.WithDescription("Distribution of settings size by type in bytes in completed snapshots"),
		metric.WithUnit("By"),
	)

	return &Metrics{
		incompleteSnapshotsCounter: incompleteSnapshotsCounter,
		completeSnapshotsCounter:   successfulEndsCounter,
		settingsCountHistogram:     settingsCountHistogram,
		settingsSizeHistogram:      settingsSizeHistogram,
	}
}

func metricName(name string) string {
	return fmt.Sprintf("extension.sts_settings_provider.%s", name)
}

func (m *Metrics) IncIncompleteSnapshots(ctx context.Context, settingType stsSettingsModel.SettingType) {
	m.incompleteSnapshotsCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("setting_type", string(settingType)),
			attribute.String("reason", "replaced_by_new_start"),
		))
}

func (m *Metrics) IncCompleteSnapshots(ctx context.Context, settingType stsSettingsModel.SettingType) {
	m.completeSnapshotsCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("setting_type", string(settingType)),
		),
	)
}

func (m *Metrics) RecordSettingsCount(ctx context.Context, n int64, settingType stsSettingsModel.SettingType) {
	m.settingsCountHistogram.Record(ctx, n,
		metric.WithAttributes(
			attribute.String("setting_type", string(settingType)),
		),
	)
}

func (m *Metrics) RecordSettingsSize(ctx context.Context, n int64, settingType stsSettingsModel.SettingType) {
	m.settingsSizeHistogram.Record(ctx, n,
		metric.WithAttributes(
			attribute.String("setting_type", string(settingType)),
		),
	)
}
