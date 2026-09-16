// Package otlpconfigexporter preserves typed OTLP queue sizers during config marshaling.
package otlpconfigexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/exporter/xexporter"
)

// NewHTTPFactory wraps the stock HTTP exporter without changing its runtime.
func NewHTTPFactory() exporter.Factory { return wrapFactory(otlphttpexporter.NewFactory()) }

// NewGRPCFactory wraps the stock gRPC exporter without changing its runtime.
func NewGRPCFactory() exporter.Factory { return wrapFactory(otlpexporter.NewFactory()) }

type upstreamFactory interface {
	xexporter.Factory
	DeprecatedAlias() component.Type
	SetDeprecatedAlias(component.Type)
}

type factory struct {
	upstreamFactory
}

func wrapFactory(stock exporter.Factory) exporter.Factory {
	upstream, ok := stock.(upstreamFactory)
	if !ok {
		panic("OTLP factory must support profiles and deprecated aliases")
	}
	return &factory{upstreamFactory: upstream}
}

func (f *factory) CreateDefaultConfig() component.Config {
	stock := f.upstreamFactory.CreateDefaultConfig()
	switch typed := stock.(type) {
	case *otlphttpexporter.Config:
		return &config{upstream: typed, queue: &typed.QueueConfig}
	case *otlpexporter.Config:
		return &config{upstream: typed, queue: &typed.QueueConfig}
	default:
		panic("unexpected upstream OTLP config type")
	}
}

func (f *factory) CreateTraces(
	ctx context.Context, set exporter.Settings, cfg component.Config,
) (exporter.Traces, error) {
	return f.upstreamFactory.CreateTraces(ctx, set, unwrap(cfg))
}

func (f *factory) CreateMetrics(
	ctx context.Context, set exporter.Settings, cfg component.Config,
) (exporter.Metrics, error) {
	return f.upstreamFactory.CreateMetrics(ctx, set, unwrap(cfg))
}

func (f *factory) CreateLogs(
	ctx context.Context, set exporter.Settings, cfg component.Config,
) (exporter.Logs, error) {
	return f.upstreamFactory.CreateLogs(ctx, set, unwrap(cfg))
}

func (f *factory) CreateProfiles(
	ctx context.Context, set exporter.Settings, cfg component.Config,
) (xexporter.Profiles, error) {
	return f.upstreamFactory.CreateProfiles(ctx, set, unwrap(cfg))
}

func unwrap(cfg component.Config) component.Config {
	if wrapped, ok := cfg.(*config); ok {
		return wrapped.upstream
	}
	return cfg
}
