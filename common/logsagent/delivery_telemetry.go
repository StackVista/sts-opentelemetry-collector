package logsagent

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type telemetry struct {
	rejectedRequests metric.Int64Counter
	rejectedRecords  metric.Int64Counter
	oversized        metric.Int64Counter
	outcomes         metric.Int64Counter
	outstanding      metric.Int64UpDownCounter
}

func newTelemetry(provider metric.MeterProvider) (telemetry, error) {
	meter := provider.Meter("github.com/stackvista/sts-opentelemetry-collector/connector/stslogsrouteconnector")
	var t telemetry
	var errs [5]error
	t.rejectedRequests, errs[0] = meter.Int64Counter("stslogsroute.pre_export_rejected_requests")
	t.rejectedRecords, errs[1] = meter.Int64Counter("stslogsroute.pre_export_rejected_records")
	t.oversized, errs[2] = meter.Int64Counter("stslogsroute.oversized_records")
	t.outcomes, errs[3] = meter.Int64Counter("stslogsroute.export_requests")
	t.outstanding, errs[4] = meter.Int64UpDownCounter("stslogsroute.outstanding_requests")
	return t, errors.Join(errs[:]...)
}

func modeAttributes(mode Mode) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("mode", metricMode(mode)))
}

func metricMode(mode Mode) string {
	switch mode {
	case PromtailMode:
		return string(mode)
	default:
		return "unselected"
	}
}

func (t telemetry) reject(ctx context.Context, mode Mode, reason string, records int) {
	attrs := metric.WithAttributes(attribute.String("mode", metricMode(mode)), attribute.String("reason", reason))
	t.rejectedRequests.Add(ctx, 1, attrs)
	t.rejectedRecords.Add(ctx, int64(records), attrs)
}

func (t telemetry) complete(ctx context.Context, mode Mode, outcome string, draining bool) {
	attrs := metric.WithAttributes(
		attribute.String("mode", metricMode(mode)),
		attribute.String("outcome", outcome),
		attribute.Bool("draining", draining),
	)
	t.outcomes.Add(ctx, 1, attrs)
	t.outstanding.Add(ctx, -1, modeAttributes(mode))
}
