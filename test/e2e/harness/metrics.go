package harness

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkMetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.uber.org/zap"
)

type GaugeSpec struct {
	Name       string
	Attributes map[string]interface{}
	Value      float64
}

type MetricSpec struct {
	ResourceAttributes map[string]string
	ScopeAttributes    map[string]string
	Gauges             []GaugeSpec
}

// BuildAndSendMetrics constructs metrics and sends them to the endpoint
func BuildAndSendMetrics(ctx context.Context, logger *zap.Logger, endpoint string, spec MetricSpec) error {
	exp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(strings.TrimPrefix(endpoint, "http://")),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return fmt.Errorf("failed to create OTLP metric exporter: %w", err)
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		convertToAttributes(spec.ResourceAttributes)...,
	)

	mp := sdkMetric.NewMeterProvider(
		sdkMetric.WithResource(res),
		sdkMetric.WithReader(sdkMetric.NewPeriodicReader(exp, sdkMetric.WithInterval(100*time.Millisecond))),
	)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mp.Shutdown(shutdownCtx)
	}()

	scopeName := "e2e-test"
	var scopeVersion string
	if name, ok := spec.ScopeAttributes["otel.scope.name"]; ok {
		scopeName = name
	}
	if version, ok := spec.ScopeAttributes["otel.scope.version"]; ok {
		scopeVersion = version
	}

	metrics := make([]metricdata.Metrics, len(spec.Gauges))
	for i, gaugeSpec := range spec.Gauges {
		metrics[i] = metricdata.Metrics{
			Name: gaugeSpec.Name,
			Data: metricdata.Gauge[float64]{
				DataPoints: []metricdata.DataPoint[float64]{
					{
						Attributes: attribute.NewSet(convertToAttributesFromInterface(gaugeSpec.Attributes)...),
						StartTime:  time.Now(),
						Time:       time.Now(),
						Value:      gaugeSpec.Value,
					},
				},
			},
		}
	}

	exportErr := exp.Export(ctx, &metricdata.ResourceMetrics{
		Resource: res,
		ScopeMetrics: []metricdata.ScopeMetrics{
			{
				Scope: instrumentation.Scope{
					Name:    scopeName,
					Version: scopeVersion,
				},
				Metrics: metrics,
			},
		},
	})

	if exportErr != nil {
		return fmt.Errorf("failed to export metrics: %w", exportErr)
	}

	logger.Info("Metrics exported")
	return nil
}
