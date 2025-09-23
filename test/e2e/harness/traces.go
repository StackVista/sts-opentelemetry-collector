package harness

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
)

func SendTracesRoundRobin(ctx context.Context, endpoints []string, numSpans int) error {
	for i := 0; i < numSpans; i++ {
		ep := endpoints[i%len(endpoints)]
		if err := sendTrace(ctx, ep, fmt.Sprintf("span-%d", i)); err != nil {
			return err
		}
	}
	return nil
}

func sendTrace(ctx context.Context, endpoint, spanName string) error {
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(strings.TrimPrefix(endpoint, "http://")),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return err
	}
	defer exp.Shutdown(ctx)

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(resource.NewWithAttributes(
			"test",
			attribute.String("service.name", "orders"),
		)),
	)
	defer tp.Shutdown(ctx)
	otel.SetTracerProvider(tp)

	tracer := tp.Tracer("e2e-test")
	_, span := tracer.Start(ctx, spanName)
	time.Sleep(5 * time.Millisecond)
	span.End()
	return nil
}
