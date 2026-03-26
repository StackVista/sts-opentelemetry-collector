package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/log"
	sdkLog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.uber.org/zap"
)

type LogRecordSpec struct {
	EventName  string
	Attributes map[string]interface{}
	Body       interface{}
}

type LogSpec struct {
	ResourceAttributes map[string]string
	ScopeAttributes    map[string]string
	LogRecords         []LogRecordSpec
}

// BuildAndSendLogs constructs logs and sends them to the grpc OTLP endpoint
func BuildAndSendLogs(ctx context.Context, logger *zap.Logger, endpoint string, spec LogSpec) error {
	exp, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint(strings.TrimPrefix(endpoint, "http://")),
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		return fmt.Errorf("failed to create OTLP log exporter: %w", err)
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		convertToAttributes(spec.ResourceAttributes)...,
	)

	loggerProvider := sdkLog.NewLoggerProvider(
		sdkLog.WithResource(res),
		sdkLog.WithProcessor(sdkLog.NewBatchProcessor(exp)),
	)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = loggerProvider.Shutdown(shutdownCtx)
	}()

	scopeName := "e2e-test"
	var scopeVersion string
	if name, ok := spec.ScopeAttributes["otel.scope.name"]; ok {
		scopeName = name
	}
	if version, ok := spec.ScopeAttributes["otel.scope.version"]; ok {
		scopeVersion = version
	}

	otelLogger := loggerProvider.Logger(
		scopeName,
		log.WithInstrumentationVersion(scopeVersion),
	)

	// Emit all log records
	for _, logSpec := range spec.LogRecords {
		record := log.Record{}
		record.SetTimestamp(time.Now())
		record.SetEventName(logSpec.EventName)

		// Convert body to JSON bytes to preserve structure as a map
		bodyBytes, err := json.Marshal(logSpec.Body)
		if err != nil {
			return fmt.Errorf("failed to marshal log body to JSON: %w", err)
		}
		record.SetBody(log.BytesValue(bodyBytes))

		// Add attributes if provided
		if logSpec.Attributes != nil && len(logSpec.Attributes) > 0 {
			logAttrs := convertToLogAttributes(logSpec.Attributes)
			record.AddAttributes(logAttrs...)
		}

		// Emit the log record
		otelLogger.Emit(ctx, record)
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := loggerProvider.ForceFlush(flushCtx); err != nil {
		return fmt.Errorf("failed to flush logs: %w", err)
	}

	logger.Info("Logs exported", zap.Int("count", len(spec.LogRecords)))
	return nil
}

// convertToLogAttributes converts map[string]interface{} to []log.KeyValue
func convertToLogAttributes(attrs map[string]interface{}) []log.KeyValue {
	out := make([]log.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		switch val := v.(type) {
		case string:
			out = append(out, log.String(k, val))
		case int:
			out = append(out, log.Int(k, val))
		case int64:
			out = append(out, log.Int64(k, val))
		case float64:
			out = append(out, log.Float64(k, val))
		case bool:
			out = append(out, log.Bool(k, val))
		default:
			out = append(out, log.String(k, fmt.Sprintf("%v", val)))
		}
	}
	return out
}
