package stsk8slogsexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type logsExporter struct {
	exporter.Logs
	encoder *encoder
	invalid metric.Int64Counter
	logger  *zap.Logger
	id      attribute.KeyValue
}

func newLogsExporter(ctx context.Context, set exporter.Settings, cfg *Config, sender *sender) (exporter.Logs, error) {
	encoder, err := newEncoder(cfg.ClusterName)
	if err != nil {
		return nil, err
	}
	invalid, err := set.MeterProvider.Meter(
		"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8slogsexporter",
	).Int64Counter("otelcol_stsk8slogs_invalid_log_records",
		metric.WithDescription("Promtail-compatible log records rejected before export, by validation reason."),
		metric.WithUnit("{record}"))
	if err != nil {
		return nil, err
	}
	helper, err := exporterhelper.NewLogs(ctx, set, cfg,
		func(ctx context.Context, logs plog.Logs) error {
			return sender.send(ctx, encoder.encode(logs).payload)
		},
		// The sender owns attempt timeouts so they remain distinct from the caller's export lifetime.
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: 0}),
		exporterhelper.WithRetry(cfg.BackOffConfig),
		exporterhelper.WithQueue(cfg.QueueSettings),
		exporterhelper.WithShutdown(func(context.Context) error {
			sender.client.CloseIdleConnections()
			return nil
		}),
	)
	if err != nil {
		return nil, err
	}
	return &logsExporter{
		Logs: helper, encoder: encoder, invalid: invalid, logger: set.Logger,
		id: attribute.String("exporter", set.ID.String()),
	}, nil
}

func (e *logsExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *logsExporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	if err := ctx.Err(); err != nil {
		return contextFailure(err)
	}
	valid, invalid := e.encoder.filter(logs)
	for reason, count := range invalid {
		e.invalid.Add(ctx, int64(count), metric.WithAttributes(e.id, attribute.String("reason", string(reason))))
		e.logger.Warn("Dropping invalid Promtail-compatible log records",
			zap.String("reason", string(reason)), zap.Int("log_records", count))
	}
	if valid.LogRecordCount() == 0 {
		return nil
	}
	return e.Logs.ConsumeLogs(ctx, valid)
}

// Validation precedes retries so they neither recount drops nor acknowledge invalid records.
func (e *encoder) filter(logs plog.Logs) (plog.Logs, map[invalidReason]int) {
	valid := plog.NewLogs()
	invalid := make(map[invalidReason]int)
	logs.CopyTo(valid)
	valid.ResourceLogs().RemoveIf(func(resource plog.ResourceLogs) bool {
		_, resourceError := e.resourceIdentity(resource.Resource().Attributes())
		resource.ScopeLogs().RemoveIf(func(scope plog.ScopeLogs) bool {
			scope.LogRecords().RemoveIf(func(record plog.LogRecord) bool {
				reason := resourceError
				if reason == "" {
					reason = validateRecord(record)
				}
				if reason == "" {
					return false
				}
				invalid[reason]++
				return true
			})
			return scope.LogRecords().Len() == 0
		})
		return resource.ScopeLogs().Len() == 0
	})
	return valid, invalid
}

var _ component.Component = (*logsExporter)(nil)
