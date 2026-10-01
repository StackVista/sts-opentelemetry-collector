package stsk8stopologyexporter

import (
	"context"

	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/otel/metric"
)

func registerModeGauge(set exporter.Settings, mode *modeSelector) error {
	_, err := set.MeterProvider.Meter("github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8stopologyexporter").
		Int64ObservableGauge("otelcol_stsk8stopology_legacy_export_enabled",
			metric.WithDescription("1 while legacy Kubernetes topology is sent, 0 while the platform does not require it."),
			metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
				if mode.legacyEnabled() {
					observer.Observe(1)
				} else {
					observer.Observe(0)
				}
				return nil
			}))
	return err
}
