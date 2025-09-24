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
	"math/rand"
	"strings"
	"time"
)

var r *rand.Rand

func init() {
	// obtain a local random generator
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

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
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(strings.TrimPrefix(endpoint, "http://")),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return fmt.Errorf("failed to create OTLP exporter: %w", err)
	}
	defer func() {
		// The cancel timeout needs to be greater than the batch processor's max export delay
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		_ = exp.Shutdown(shutdownCtx)
	}()

	tp := trace.NewTracerProvider(
		// The default export delay (5s) is too large for our test
		trace.WithBatcher(exp, trace.WithBatchTimeout(200*time.Millisecond)),
		trace.WithResource(resource.NewWithAttributes(
			"test",
			convertToAttributes(spec.ResourceAttributes)...,
		)),
	)
	defer func() {
		// The cancel timeout needs to be greater than the batch processor's max export delay
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		_ = tp.Shutdown(shutdownCtx)
	}()
	//defer func() { _ = tp.Shutdown(ctx) }()

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

		// simulate random execution time
		sleepMs := r.Intn(100) + 1 // rand.Intn(100) returns 0..99, +1 gives 1..100
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		span.End()
	}

	logger.Info("Spans exported")
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
