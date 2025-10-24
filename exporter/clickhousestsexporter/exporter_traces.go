// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:lll
package clickhousestsexporter // import "github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter"

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // For register database driver.
	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter/internal"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.uber.org/zap"
)

type TracesExporter struct {
	client           *sql.DB
	insertSQL        string
	resourceExporter *ResourcesExporter

	logger *zap.Logger
	cfg    *Config
}

func NewTracesExporter(logger *zap.Logger, cfg *Config) (*TracesExporter, error) {
	client, err := newClickhouseClient(cfg)
	if err != nil {
		return nil, err
	}
	resourceExporter, err := NewResourceExporter(logger, cfg)
	if err != nil {
		return nil, err
	}

	return &TracesExporter{
		client:           client,
		insertSQL:        renderInsertTracesSQL(cfg),
		resourceExporter: resourceExporter,
		logger:           logger,
		cfg:              cfg,
	}, nil
}

func (e *TracesExporter) Start(ctx context.Context, host component.Host) error {
	if err := e.resourceExporter.Start(ctx, host); err != nil {
		return err
	}

	if !e.cfg.CreateTracesTable {
		return nil
	}

	if err := createDatabase(ctx, e.cfg); err != nil {
		return err
	}

	return createTracesTable(ctx, e.cfg, e.client)
}

// shutdown will shut down the exporter.
func (e *TracesExporter) Shutdown(ctx context.Context) error {
	err := e.resourceExporter.Shutdown(ctx)
	if err != nil {
		return errors.New("unable to shutdown the resource exporter")
	}

	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

const SpanParentTypeInternal = "SPAN_PARENT_TYPE_INTERNAL"

func getSpanParentType(r ptrace.Span) string {
	if r.ParentSpanID().IsEmpty() {
		return "SPAN_PARENT_TYPE_ROOT"
	}
	switch r.Kind() {
	case ptrace.SpanKindServer:
		return "SPAN_PARENT_TYPE_EXTERNAL"
	case ptrace.SpanKindConsumer:
		return "SPAN_PARENT_TYPE_EXTERNAL"
	case ptrace.SpanKindClient:
		return SpanParentTypeInternal
	case ptrace.SpanKindInternal:
		return SpanParentTypeInternal
	case ptrace.SpanKindProducer:
		return SpanParentTypeInternal
	case ptrace.SpanKindUnspecified:
		return SpanParentTypeInternal
	default:
		return SpanParentTypeInternal
	}
}

func (e *TracesExporter) PushTraceData(ctx context.Context, td ptrace.Traces) error {
	start := time.Now()
	resources := []*ResourceModel{}

	err := doWithTx(ctx, e.client, func(tx *sql.Tx) error {
		traceStatement, err := tx.PrepareContext(ctx, e.insertSQL)
		if err != nil {
			return fmt.Errorf("PrepareContext Traces:%w", err)
		}
		defer func() {
			_ = traceStatement.Close()
		}()
		for i := 0; i < td.ResourceSpans().Len(); i++ {
			spans := td.ResourceSpans().At(i)
			res, err := NewResourceModel(spans.Resource())
			if err != nil {
				return err
			}
			resources = append(resources, res)
			var serviceName string
			if v, ok := spans.Resource().Attributes().Get(string(conventions.ServiceNameKey)); ok {
				serviceName = v.Str()
			}

			for j := 0; j < spans.ScopeSpans().Len(); j++ {
				rs := spans.ScopeSpans().At(j).Spans()
				scopeName := spans.ScopeSpans().At(j).Scope().Name()
				scopeVersion := spans.ScopeSpans().At(j).Scope().Version()
				for k := 0; k < rs.Len(); k++ {
					r := rs.At(k)
					spanAttr := attributesToMap(r.Attributes())
					status := r.Status()
					eventTimes, eventNames, eventAttrs := convertEvents(r.Events())
					linksTraceIDs, linksSpanIDs, linksTraceStates, linksAttrs := convertLinks(r.Links())
					spanParentType := getSpanParentType(r)
					_, err = traceStatement.ExecContext(ctx,
						r.StartTimestamp().AsTime(),
						res.resourceRef,
						TraceIDToHexOrEmptyString(r.TraceID()),
						SpanIDToHexOrEmptyString(r.SpanID()),
						SpanIDToHexOrEmptyString(r.ParentSpanID()),
						r.TraceState().AsRaw(),
						r.Name(),
						SpanKindStr(r.Kind()),
						serviceName,
						scopeName,
						scopeVersion,
						spanAttr,
						r.EndTimestamp().AsTime().Sub(r.StartTimestamp().AsTime()).Nanoseconds(),
						StatusCodeStr(status.Code()),
						status.Message(),
						spanParentType,
						res.authScope,
						eventTimes,
						eventNames,
						eventAttrs,
						linksTraceIDs,
						linksSpanIDs,
						linksTraceStates,
						linksAttrs,
					)
					if err != nil {
						return fmt.Errorf("ExecContext Traces:%w", err)
					}
				}
			}
		}
		return nil
	})
	duration := time.Since(start)
	e.logger.Debug("insert traces", zap.Int("records", td.SpanCount()),
		zap.String("cost", duration.String()))
	_ = e.resourceExporter.InsertResources(ctx, resources)
	return err
}

func convertEvents(events ptrace.SpanEventSlice) ([]time.Time, []string, []map[string]string) {
	var (
		times []time.Time
		names []string
		attrs []map[string]string
	)
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		times = append(times, event.Timestamp().AsTime())
		names = append(names, event.Name())
		attrs = append(attrs, attributesToMap(event.Attributes()))
	}
	return times, names, attrs
}

func convertLinks(links ptrace.SpanLinkSlice) ([]string, []string, []string, []map[string]string) {
	var (
		traceIDs []string
		spanIDs  []string
		states   []string
		attrs    []map[string]string
	)
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		traceIDs = append(traceIDs, TraceIDToHexOrEmptyString(link.TraceID()))
		spanIDs = append(spanIDs, SpanIDToHexOrEmptyString(link.SpanID()))
		states = append(states, link.TraceState().AsRaw())
		attrs = append(attrs, attributesToMap(link.Attributes()))
	}
	return traceIDs, spanIDs, states, attrs
}

const (
	// language=ClickHouse SQL
	createTracesTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
     Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
     ResourceRef UUID,
     TraceId String CODEC(ZSTD(1)),
     SpanId String CODEC(ZSTD(1)),
     ParentSpanId String CODEC(ZSTD(1)),
     TraceState String CODEC(ZSTD(1)),
     SpanName LowCardinality(String) CODEC(ZSTD(1)),
     SpanKind LowCardinality(String) CODEC(ZSTD(1)),
     ServiceName LowCardinality(String) CODEC(ZSTD(1)),
     ScopeName String CODEC(ZSTD(1)),
     ScopeVersion String CODEC(ZSTD(1)),
     SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
     Duration Int64 CODEC(ZSTD(1)),
     StatusCode LowCardinality(String) CODEC(ZSTD(1)),
     StatusMessage String CODEC(ZSTD(1)),
     SpanParentType String CODEC(ZSTD(1)),
     AuthScope Array(LowCardinality(String)),
     Events Nested (
         Timestamp DateTime64(9),
         Name LowCardinality(String),
         Attributes Map(LowCardinality(String), String)
     ) CODEC(ZSTD(1)),
     Links Nested (
         TraceId String,
         SpanId String,
         TraceState String,
         Attributes Map(LowCardinality(String), String)
     ) CODEC(ZSTD(1)),
) ENGINE MergeTree()
%s
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, ResourceRef, SpanName, toUnixTimestamp(Timestamp), TraceId)
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	insertTracesSQLTemplate = `INSERT INTO %s (
                        Timestamp,
												ResourceRef,
                        TraceId,
                        SpanId,
                        ParentSpanId,
                        TraceState,
                        SpanName,
                        SpanKind,
                        ServiceName,
                        ScopeName,
                        ScopeVersion,
                        SpanAttributes,
                        Duration,
                        StatusCode,
                        StatusMessage,
                        SpanParentType,
                        AuthScope,
                        Events.Timestamp,
                        Events.Name,
                        Events.Attributes,
                        Links.TraceId,
                        Links.SpanId,
                        Links.TraceState,
                        Links.Attributes
                        ) VALUES (
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?
                                  )`
)

func createTracesTable(ctx context.Context, cfg *Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateTracesTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create traces table sql: %w", err)
	}
	return nil
}

func renderInsertTracesSQL(cfg *Config) string {
	return fmt.Sprintf(strings.ReplaceAll(insertTracesSQLTemplate, "'", "`"), cfg.TracesTableName)
}

func renderCreateTracesTableSQL(cfg *Config) string {
	ttlExpr := internal.GenerateTTLExpr(cfg.TTLDays, cfg.TTL, "Timestamp")
	return fmt.Sprintf(createTracesTableSQL, cfg.TracesTableName, ttlExpr)
}
