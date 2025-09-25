package harness

import (
	"context"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"strings"
	"time"
)

type SpanSpec struct {
	Name       string
	Attributes map[string]interface{}
}

type TraceSpec struct {
	ResourceAttributes map[string]string
	ScopeAttributes    map[string]string
	Spans              []SpanSpec
}

// BuildAndSendTrace constructs a trace with the given resource + spans and sends it to the endpoint
func BuildAndSendTrace(ctx context.Context, logger *zap.Logger, endpoint string, spec TraceSpec) error {
	// TODO: when we do get to a place where we spin up multiple collector instances, we can update this function to:
	// - accept a slice of endpoints (collector grpc OTLP endpoints)
	// - with some load balancing strategy (e.g. round-robin), send spans to the endpionts
	// - cache a trace exporter/provider per endpoint

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(strings.TrimPrefix(endpoint, "http://")),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	tp := trace.NewTracerProvider(
		// The default export delay (5s) is too large for our test
		//trace.WithBatcher(exp, trace.WithBatchTimeout(250*time.Millisecond)),
		trace.WithSyncer(exp),
		trace.WithResource(resource.NewWithAttributes(
			"test",
			convertToAttributes(spec.ResourceAttributes)...,
		)),
	)
	defer func() {
		// The cancel timeout needs to be greater than the batch processor's max export delay
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tp.Shutdown(shutdownCtx)
	}()

	otel.SetTracerProvider(tp)
	tracer := tp.Tracer("e2e-test")

	for _, spanSpec := range spec.Spans {
		_, span := tracer.Start(ctx, spanSpec.Name)
		for k, v := range spanSpec.Attributes {
			switch val := v.(type) {
			case string:
				span.SetAttributes(attribute.String(k, val))
			case int:
				span.SetAttributes(attribute.Int(k, val))
			case int64:
				span.SetAttributes(attribute.Int64(k, val))
			case float64:
				span.SetAttributes(attribute.Float64(k, val))
			case bool:
				span.SetAttributes(attribute.Bool(k, val))
			default:
				span.SetAttributes(attribute.String(k, fmt.Sprintf("%v", val)))
			}
		}

		for k, v := range spec.ScopeAttributes {
			span.SetAttributes(attribute.String(k, v))
		}

		// simulate some execution time
		time.Sleep(2 * time.Millisecond)
		span.End()
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tp.ForceFlush(flushCtx); err != nil {
		return fmt.Errorf("failed to flush traces: %w", err)
	}

	logger.Info("Trace exported")
	return nil
}

// helper to convert map[string]string to []attribute.KeyValue
func convertToAttributes(attrs map[string]string) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		out = append(out, attribute.String(k, v))
	}
	return out
}
