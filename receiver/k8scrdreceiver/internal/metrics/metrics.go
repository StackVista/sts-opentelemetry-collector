// Package metrics records the receiver's internal telemetry via the MeterProvider
// supplied by component.TelemetrySettings. The shape mirrors the topology connector:
// a single Recorder with a small core set of instruments.
package metrics

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type ChangeType string

const (
	ChangeAdded    ChangeType = "added"
	ChangeModified ChangeType = "modified"
	ChangeDeleted  ChangeType = "deleted"
	ChangeUnknown  ChangeType = "unknown"
)

type CycleMode string

const (
	ModeSnapshot  CycleMode = "snapshot"
	ModeIncrement CycleMode = "increment"
)

type ResourceKind string

const (
	KindCRD ResourceKind = "crd"
	KindCR  ResourceKind = "cr"
)

type BroadcastOutcome string

const (
	BroadcastSuccess BroadcastOutcome = "success"
	BroadcastFailed  BroadcastOutcome = "failed"
)

// BroadcastFailureReason categorizes why a broadcast did not satisfy its ACK threshold.
type BroadcastFailureReason string

const (
	BroadcastFailureNone        BroadcastFailureReason = ""
	BroadcastFailureDNSLookup   BroadcastFailureReason = "dns_lookup"
	BroadcastFailureGzip        BroadcastFailureReason = "gzip"
	BroadcastFailureAckTimeout  BroadcastFailureReason = "ack_timeout"
	BroadcastFailureNoAcks      BroadcastFailureReason = "no_acks"
)

// PushOutcome is the result of a single push attempt to a peer.
type PushOutcome string

const (
	PushSuccess PushOutcome = "success"
	PushFailed  PushOutcome = "failed"
)

// PushFailureReason categorizes why a push attempt failed. Empty string for success.
type PushFailureReason string

const (
	PushFailureNone          PushFailureReason = ""
	PushFailureTimeout       PushFailureReason = "timeout"
	PushFailureConnection    PushFailureReason = "connection_error"
	PushFailureHTTPStatus    PushFailureReason = "http_error"
	PushFailureRequestFailed PushFailureReason = "request_failed"
)

// Recorder is the interface used by the receiver to record metrics.
type Recorder interface {
	RecordEmitted(ctx context.Context, change ChangeType, n int64)
	RecordCycle(ctx context.Context, mode CycleMode, d time.Duration)
	RecordCacheSize(ctx context.Context, kind ResourceKind, n int64)
	RecordPeerBroadcast(ctx context.Context, outcome BroadcastOutcome, reason BroadcastFailureReason)
	RecordPeerPushAttempt(ctx context.Context, outcome PushOutcome, reason PushFailureReason)
	RecordPeerPushBytes(ctx context.Context, n int64)
	RecordPeerPushDuration(ctx context.Context, d time.Duration)
}

// NoopRecorder is a Recorder that drops every measurement. Useful in tests.
type NoopRecorder struct{}

func (NoopRecorder) RecordEmitted(_ context.Context, _ ChangeType, _ int64)      {}
func (NoopRecorder) RecordCycle(_ context.Context, _ CycleMode, _ time.Duration) {}
func (NoopRecorder) RecordCacheSize(_ context.Context, _ ResourceKind, _ int64)  {}
func (NoopRecorder) RecordPeerBroadcast(_ context.Context, _ BroadcastOutcome, _ BroadcastFailureReason) {
}
func (NoopRecorder) RecordPeerPushAttempt(_ context.Context, _ PushOutcome, _ PushFailureReason) {
}
func (NoopRecorder) RecordPeerPushBytes(_ context.Context, _ int64)           {}
func (NoopRecorder) RecordPeerPushDuration(_ context.Context, _ time.Duration) {}

// Metrics is the live Recorder backed by a real Meter.
type Metrics struct {
	common []attribute.KeyValue

	recordsEmitted    metric.Int64Counter
	cycleDuration     metric.Float64Histogram
	cachedResources   metric.Int64Gauge
	peerBroadcasts    metric.Int64Counter
	peerPushAttempts  metric.Int64Counter
	peerPushBytes     metric.Int64Histogram
	peerPushDuration  metric.Float64Histogram
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
		metric.WithDescription("Peer cache broadcast outcomes per Save() call, labelled by outcome."),
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

	return &Metrics{
		common:           []attribute.KeyValue{attribute.String("k8s.cluster.name", clusterName)},
		recordsEmitted:   recordsEmitted,
		cycleDuration:    cycleDuration,
		cachedResources:  cachedResources,
		peerBroadcasts:   peerBroadcasts,
		peerPushAttempts: peerPushAttempts,
		peerPushBytes:    peerPushBytes,
		peerPushDuration: peerPushDuration,
	}
}

func newMetricNameForType(typeName string) func(string) string {
	return func(name string) string {
		return fmt.Sprintf("receiver.%s.%s", typeName, name)
	}
}

func (m *Metrics) RecordEmitted(ctx context.Context, change ChangeType, n int64) {
	if n <= 0 {
		return
	}
	m.recordsEmitted.Add(ctx, n, metric.WithAttributeSet(
		m.attrs(attribute.String("change_type", string(change))),
	))
}

func (m *Metrics) RecordCycle(ctx context.Context, mode CycleMode, d time.Duration) {
	m.cycleDuration.Record(ctx, d.Seconds(), metric.WithAttributeSet(
		m.attrs(attribute.String("mode", string(mode))),
	))
}

func (m *Metrics) RecordCacheSize(ctx context.Context, kind ResourceKind, n int64) {
	m.cachedResources.Record(ctx, n, metric.WithAttributeSet(
		m.attrs(attribute.String("kind", string(kind))),
	))
}

func (m *Metrics) RecordPeerBroadcast(ctx context.Context, outcome BroadcastOutcome, reason BroadcastFailureReason) {
	attrs := []attribute.KeyValue{attribute.String("outcome", string(outcome))}
	if reason != BroadcastFailureNone {
		attrs = append(attrs, attribute.String("reason", string(reason)))
	}
	m.peerBroadcasts.Add(ctx, 1, metric.WithAttributeSet(m.attrs(attrs...)))
}

func (m *Metrics) RecordPeerPushAttempt(ctx context.Context, outcome PushOutcome, reason PushFailureReason) {
	attrs := []attribute.KeyValue{attribute.String("outcome", string(outcome))}
	if reason != PushFailureNone {
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

func (m *Metrics) attrs(extras ...attribute.KeyValue) attribute.Set {
	all := make([]attribute.KeyValue, 0, len(m.common)+len(extras))
	all = append(all, m.common...)
	all = append(all, extras...)
	return attribute.NewSet(all...)
}
