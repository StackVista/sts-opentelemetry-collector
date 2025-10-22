package harness

import (
	"context"
	"fmt"
	"strings"
	"time"

	semconv "go.opentelemetry.io/collector/semconv/v1.25.0"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdkTrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type SpanSpec struct {
	Name       string
	Attributes map[string]interface{}
	ParentID   string
	Children   []SpanSpec
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
	// - with some load balancing strategy (e.g. round-robin), send spans to the endpoints
	// - cache a trace exporter/provider per endpoint to prevent re-creating them

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(strings.TrimPrefix(endpoint, "http://")),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	tp := sdkTrace.NewTracerProvider(
		sdkTrace.WithSyncer(exp),
		sdkTrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			convertToAttributes(spec.ResourceAttributes)...,
		)),
	)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tp.Shutdown(shutdownCtx)
	}()

	otel.SetTracerProvider(tp)

	scopeName := "e2e-test"
	var scopeVersion string
	if name, ok := spec.ScopeAttributes["otel.scope.name"]; ok {
		scopeName = name
	}
	if version, ok := spec.ScopeAttributes["otel.scope.version"]; ok {
		scopeVersion = version
	}

	tracer := tp.Tracer(
		scopeName,
		trace.WithInstrumentationVersion(scopeVersion),
	)

	// This map stores the active context for a span, keyed by its "symbolic" 1-based index
	spanContexts := make(map[string]context.Context)
	// This map stores the span itself, so we can end it later
	createdSpans := make(map[string]trace.Span)

	// First pass: create all spans and their inline children
	for i, spanSpec := range spec.Spans {
		// We use a 1-based index as the symbolic ID to match the spec's "ParentID: 1"
		symbolicID := fmt.Sprintf("%d", i+1)

		var parentCtx context.Context
		if spanSpec.ParentID != "" {
			var ok bool
			// Find the parent's context using the symbolic ParentID
			parentCtx, ok = spanContexts[spanSpec.ParentID]
			if !ok {
				logger.Warn("ParentID not found in map, creating as root span",
					zap.String("ParentID", spanSpec.ParentID),
					zap.String("SpanName", spanSpec.Name),
				)
				parentCtx = ctx // Fallback to root context
			}
		} else {
			// No ParentID, this is a root span
			parentCtx = ctx
		}

		// createSpanRecursive builds the span and its children
		span := createSpanRecursive(parentCtx, tracer, spanSpec)

		// Store the new span's context and the span itself for linking and ending
		spanContexts[symbolicID] = trace.ContextWithSpan(context.Background(), span)
		createdSpans[symbolicID] = span
	}

	// Second pass: End all spans in reverse order to ensure children end before parents
	for i := len(spec.Spans) - 1; i >= 0; i-- {
		symbolicID := fmt.Sprintf("%d", i+1)
		if span, ok := createdSpans[symbolicID]; ok {
			time.Sleep(2 * time.Millisecond) // Simulate some execution time
			span.End()
		}
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tp.ForceFlush(flushCtx); err != nil {
		return fmt.Errorf("failed to flush traces: %w", err)
	}

	logger.Info("Trace exported")
	return nil
}

// createSpanRecursive is a helper to create a span and all its nested children.
// It returns the trace.Span it created (without ending it).
func createSpanRecursive(ctx context.Context, tracer trace.Tracer, spanSpec SpanSpec) trace.Span {
	spanCtx, span := tracer.Start(ctx, spanSpec.Name)
	setSpanAttributes(span, spanSpec.Attributes)

	// Recursively create/start all inline children
	for _, childSpec := range spanSpec.Children {
		// Create the child span
		childSpan := createSpanRecursive(spanCtx, tracer, childSpec)

		time.Sleep(2 * time.Millisecond) // Simulate child execution time
		childSpan.End()
	}

	// Return the parent span (it will be ended by the main function)
	return span
}

// setSpanAttributes is a helper to apply attributes from the spec to a span
func setSpanAttributes(span trace.Span, attrs map[string]interface{}) {
	for k, v := range attrs {
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
}

// helper to convert map[string]string to []attribute.KeyValue
func convertToAttributes(attrs map[string]string) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		out = append(out, attribute.String(k, v))
	}
	return out
}
