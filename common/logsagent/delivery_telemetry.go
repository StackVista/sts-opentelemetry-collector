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
	meter := provider.Meter("github.com/stackvista/sts-opentelemetry-collector/common/logsagent")
	var t telemetry
	var errs [5]error
	t.rejectedRequests, errs[0] = meter.Int64Counter("stslogsagent.pre_export_rejected_requests")
	t.rejectedRecords, errs[1] = meter.Int64Counter("stslogsagent.pre_export_rejected_records")
	t.oversized, errs[2] = meter.Int64Counter("stslogsagent.oversized_records")
	t.outcomes, errs[3] = meter.Int64Counter("stslogsagent.export_requests")
	t.outstanding, errs[4] = meter.Int64UpDownCounter("stslogsagent.outstanding_requests")
	return t, errors.Join(errs[:]...)
}

func (t telemetry) reject(ctx context.Context, reason string, records int) {
	attrs := metric.WithAttributes(attribute.String("reason", reason))
	t.rejectedRequests.Add(ctx, 1, attrs)
	t.rejectedRecords.Add(ctx, int64(records), attrs)
}

func (t telemetry) complete(ctx context.Context, outcome string, draining bool) {
	attrs := metric.WithAttributes(
		attribute.String("outcome", outcome),
		attribute.Bool("draining", draining),
	)
	t.outcomes.Add(ctx, 1, attrs)
	t.outstanding.Add(ctx, -1)
}
