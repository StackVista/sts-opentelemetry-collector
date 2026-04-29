// Package metrics records the receiver's internal telemetry via the MeterProvider
// supplied by component.TelemetrySettings. The shape mirrors the topology connector:
// a single Recorder with a small core set of instruments.
package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/types"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Recorder is the interface used by the receiver to record metrics.
type Recorder interface {
	RecordEmitted(ctx context.Context, change types.ChangeType, n int64)
	RecordCycle(ctx context.Context, mode types.CycleMode, d time.Duration)
	RecordCacheSize(ctx context.Context, kind types.ResourceKind, n int64)
	RecordPeerBroadcast(ctx context.Context, outcome types.BroadcastOutcome, reason types.BroadcastFailureReason)
	RecordPeerPushAttempt(ctx context.Context, outcome types.PushOutcome, reason types.PushFailureReason)
	RecordPeerPushBytes(ctx context.Context, n int64)
	RecordPeerPushDuration(ctx context.Context, d time.Duration)
	RecordBootstrap(ctx context.Context, outcome types.BootstrapOutcome, source types.BootstrapSource)
	RecordSnapshotStreamFailure(ctx context.Context)
	RecordCRInformerReconcile(ctx context.Context, outcome types.CRInformerOutcome)
}

// NoopRecorder is a Recorder that drops every measurement. Useful in tests.
type NoopRecorder struct{}

func (NoopRecorder) RecordEmitted(_ context.Context, _ types.ChangeType, _ int64)      {}
func (NoopRecorder) RecordCycle(_ context.Context, _ types.CycleMode, _ time.Duration) {}
func (NoopRecorder) RecordCacheSize(_ context.Context, _ types.ResourceKind, _ int64)  {}
func (NoopRecorder) RecordPeerBroadcast(_ context.Context, _ types.BroadcastOutcome, _ types.BroadcastFailureReason) {
}
func (NoopRecorder) RecordPeerPushAttempt(_ context.Context, _ types.PushOutcome, _ types.PushFailureReason) {
}
func (NoopRecorder) RecordPeerPushBytes(_ context.Context, _ int64)            {}
func (NoopRecorder) RecordPeerPushDuration(_ context.Context, _ time.Duration) {}
func (NoopRecorder) RecordBootstrap(_ context.Context, _ types.BootstrapOutcome, _ types.BootstrapSource) {
}
func (NoopRecorder) RecordSnapshotStreamFailure(_ context.Context)                          {}
func (NoopRecorder) RecordCRInformerReconcile(_ context.Context, _ types.CRInformerOutcome) {}

// Metrics is the live Recorder backed by a real Meter.
type Metrics struct {
	common []attribute.KeyValue

	recordsEmitted         metric.Int64Counter
	cycleDuration          metric.Float64Histogram
	cachedResources        metric.Int64Gauge
	peerBroadcasts         metric.Int64Counter
	peerPushAttempts       metric.Int64Counter
	peerPushBytes          metric.Int64Histogram
	peerPushDuration       metric.Float64Histogram
	bootstrapTotal         metric.Int64Counter
	snapshotStreamFailures metric.Int64Counter
	crInformerReconciles   metric.Int64Counter
}

// NewMetrics registers all instruments. Errors from the meter are dropped to match the
// topology connector convention; a misbehaving MeterProvider must not block startup.
func NewMetrics(typeName, clusterName string, settings component.TelemetrySettings) *Metrics {
	meter := settings.MeterProvider.Meter(typeName)
	name := newMetricNameForType(typeName)

	recordsEmitted, _ := meter.Int64Counter(
		name("records_emitted_total"),
		metric.WithDescription("Log records emitted, labelled by change_type."),
	)
	cycleDuration, _ := meter.Float64Histogram(
		name("cycle_duration_seconds"),
		metric.WithDescription("Wall-clock duration of a collection cycle, labelled by mode."),
		metric.WithUnit("s"),
	)
	cachedResources, _ := meter.Int64Gauge(
		name("cached_resources"),
		metric.WithDescription("Resources currently held in the in-memory cache, labelled by kind."),
	)
	peerBroadcasts, _ := meter.Int64Counter(
		name("peer_broadcasts_total"),
		metric.WithDescription("Peer broadcast outcomes per ApplyDelta call, labelled by outcome."),
	)
	peerPushAttempts, _ := meter.Int64Counter(
		name("peer_push_attempts_total"),
		metric.WithDescription("Per-peer per-attempt push outcomes, labelled by outcome and (on failure) reason."),
	)
	peerPushBytes, _ := meter.Int64Histogram(
		name("peer_push_bytes"),
		metric.WithDescription("Compressed payload size pushed to a single peer, in bytes."),
		metric.WithUnit("By"),
	)
	peerPushDuration, _ := meter.Float64Histogram(
		name("peer_push_duration_seconds"),
		metric.WithDescription("Wall-clock duration of a single peer push, including all retries."),
		metric.WithUnit("s"),
	)
	bootstrapTotal, _ := meter.Int64Counter(
		name("bootstrap_total"),
		metric.WithDescription(
			"Bootstrap completion outcomes, labelled by outcome (applied|leader_empty|timed_out) "+
				"and source (leader|secondary|none — none for non-applied outcomes).",
		),
	)
	snapshotStreamFailures, _ := meter.Int64Counter(
		name("snapshot_stream_failures_total"),
		metric.WithDescription("Snapshot serve failures during NDJSON encoding (client gets a partial stream)."),
	)
	crInformerReconciles, _ := meter.Int64Counter(
		name("cr_informer_reconciles_total"),
		metric.WithDescription("Periodic reconciler attempts to start CR informers, labelled by outcome."),
	)

	return &Metrics{
		common:                 []attribute.KeyValue{attribute.String("k8s.cluster.name", clusterName)},
		recordsEmitted:         recordsEmitted,
		cycleDuration:          cycleDuration,
		cachedResources:        cachedResources,
		peerBroadcasts:         peerBroadcasts,
		peerPushAttempts:       peerPushAttempts,
		peerPushBytes:          peerPushBytes,
		peerPushDuration:       peerPushDuration,
		bootstrapTotal:         bootstrapTotal,
		snapshotStreamFailures: snapshotStreamFailures,
		crInformerReconciles:   crInformerReconciles,
	}
}

func newMetricNameForType(typeName string) func(string) string {
	return func(name string) string {
		return fmt.Sprintf("receiver.%s.%s", typeName, name)
	}
}

func (m *Metrics) RecordEmitted(ctx context.Context, change types.ChangeType, n int64) {
	if n <= 0 {
		return
	}
	m.recordsEmitted.Add(ctx, n, metric.WithAttributeSet(
		m.attrs(attribute.String("change_type", string(change))),
	))
}

func (m *Metrics) RecordCycle(ctx context.Context, mode types.CycleMode, d time.Duration) {
	m.cycleDuration.Record(ctx, d.Seconds(), metric.WithAttributeSet(
		m.attrs(attribute.String("mode", string(mode))),
	))
}

func (m *Metrics) RecordCacheSize(ctx context.Context, kind types.ResourceKind, n int64) {
	m.cachedResources.Record(ctx, n, metric.WithAttributeSet(
		m.attrs(attribute.String("kind", string(kind))),
	))
}

func (m *Metrics) RecordPeerBroadcast(
	ctx context.Context, outcome types.BroadcastOutcome, reason types.BroadcastFailureReason,
) {
	attrs := []attribute.KeyValue{attribute.String("outcome", string(outcome))}
	if reason != types.BroadcastFailureNone {
		attrs = append(attrs, attribute.String("reason", string(reason)))
	}
	m.peerBroadcasts.Add(ctx, 1, metric.WithAttributeSet(m.attrs(attrs...)))
}

func (m *Metrics) RecordPeerPushAttempt(
	ctx context.Context, outcome types.PushOutcome, reason types.PushFailureReason,
) {
	attrs := []attribute.KeyValue{attribute.String("outcome", string(outcome))}
	if reason != types.PushFailureNone {
		attrs = append(attrs, attribute.String("reason", string(reason)))
	}
	m.peerPushAttempts.Add(ctx, 1, metric.WithAttributeSet(m.attrs(attrs...)))
}

func (m *Metrics) RecordPeerPushBytes(ctx context.Context, n int64) {
	m.peerPushBytes.Record(ctx, n, metric.WithAttributeSet(m.attrs()))
}

func (m *Metrics) RecordPeerPushDuration(ctx context.Context, d time.Duration) {
	m.peerPushDuration.Record(ctx, d.Seconds(), metric.WithAttributeSet(m.attrs()))
}

func (m *Metrics) RecordBootstrap(
	ctx context.Context, outcome types.BootstrapOutcome, source types.BootstrapSource,
) {
	m.bootstrapTotal.Add(ctx, 1, metric.WithAttributeSet(
		m.attrs(
			attribute.String("outcome", string(outcome)),
			attribute.String("source", string(source)),
		),
	))
}

func (m *Metrics) RecordSnapshotStreamFailure(ctx context.Context) {
	m.snapshotStreamFailures.Add(ctx, 1, metric.WithAttributeSet(m.attrs()))
}

func (m *Metrics) RecordCRInformerReconcile(ctx context.Context, outcome types.CRInformerOutcome) {
	m.crInformerReconciles.Add(ctx, 1, metric.WithAttributeSet(
		m.attrs(attribute.String("outcome", string(outcome))),
	))
}

func (m *Metrics) attrs(extras ...attribute.KeyValue) attribute.Set {
	all := make([]attribute.KeyValue, 0, len(m.common)+len(extras))
	all = append(all, m.common...)
	all = append(all, extras...)
	return attribute.NewSet(all...)
}
