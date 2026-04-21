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

// Recorder is the interface used by the receiver to record metrics.
type Recorder interface {
	RecordEmitted(ctx context.Context, change ChangeType, n int64)
	RecordCycle(ctx context.Context, mode CycleMode, d time.Duration)
	RecordCacheSize(ctx context.Context, kind ResourceKind, n int64)
	RecordPeerBroadcast(ctx context.Context, outcome BroadcastOutcome)
}

// NoopRecorder is a Recorder that drops every measurement. Useful in tests.
type NoopRecorder struct{}

func (NoopRecorder) RecordEmitted(_ context.Context, _ ChangeType, _ int64)      {}
func (NoopRecorder) RecordCycle(_ context.Context, _ CycleMode, _ time.Duration) {}
func (NoopRecorder) RecordCacheSize(_ context.Context, _ ResourceKind, _ int64)  {}
func (NoopRecorder) RecordPeerBroadcast(_ context.Context, _ BroadcastOutcome)   {}

// Metrics is the live Recorder backed by a real Meter.
type Metrics struct {
	common []attribute.KeyValue

	recordsEmitted  metric.Int64Counter
	cycleDuration   metric.Float64Histogram
	cachedResources metric.Int64Gauge
	peerBroadcasts  metric.Int64Counter
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
		metric.WithDescription("Peer cache broadcast attempts, labelled by outcome."),
	)

	return &Metrics{
		common:          []attribute.KeyValue{attribute.String("k8s.cluster.name", clusterName)},
		recordsEmitted:  recordsEmitted,
		cycleDuration:   cycleDuration,
		cachedResources: cachedResources,
		peerBroadcasts:  peerBroadcasts,
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

func (m *Metrics) RecordPeerBroadcast(ctx context.Context, outcome BroadcastOutcome) {
	m.peerBroadcasts.Add(ctx, 1, metric.WithAttributeSet(
		m.attrs(attribute.String("outcome", string(outcome))),
	))
}

func (m *Metrics) attrs(extras ...attribute.KeyValue) attribute.Set {
	all := make([]attribute.KeyValue, 0, len(m.common)+len(extras))
	all = append(all, m.common...)
	all = append(all, extras...)
	return attribute.NewSet(all...)
}
