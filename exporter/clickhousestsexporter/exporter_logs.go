// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhousestsexporter

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/metric"
)

const (
	logsResourceRefreshInterval = time.Hour
	logsResourceCacheSize       = 100_000
)

type LogsExporter struct {
	conn              driver.Conn
	insertSQL         string
	resourceInsertSQL string
	writtenResources  *lru.Cache[uuid.UUID, time.Time]
	telemetry         *logsTelemetry
}

type logsTelemetry struct {
	missingTimestamps      metric.Int64Counter
	severityClamped        metric.Int64Counter
	resourceCacheEvictions metric.Int64Counter
}

func newLogsTelemetry(provider metric.MeterProvider) (*logsTelemetry, error) {
	meter := provider.Meter("github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter")
	missingTimestamps, err := meter.Int64Counter("otelcol_clickhousests_missing_timestamp_records",
		metric.WithDescription("Log records dropped because neither source nor observed time is set"),
		metric.WithUnit("{record}"))
	if err != nil {
		return nil, err
	}
	severityClamped, err := meter.Int64Counter("otelcol_clickhousests_severity_clamped_records",
		metric.WithDescription("Log records with an out-of-range SeverityNumber; stored as Unspecified"),
		metric.WithUnit("{record}"))
	if err != nil {
		return nil, err
	}
	resourceCacheEvictions, err := meter.Int64Counter("otelcol_clickhousests_resource_cache_evictions",
		metric.WithDescription("Resource cache entries evicted before their refresh window"), metric.WithUnit("{eviction}"))
	if err != nil {
		return nil, err
	}
	return &logsTelemetry{
		missingTimestamps:      missingTimestamps,
		severityClamped:        severityClamped,
		resourceCacheEvictions: resourceCacheEvictions,
	}, nil
}

func NewLogsExporter(set component.TelemetrySettings, cfg *Config) (*LogsExporter, error) {
	if cfg.CreateLogsTable {
		return nil, fmt.Errorf("log tables are managed by StackState; create_logs_table must be false")
	}
	telemetry, err := newLogsTelemetry(set.MeterProvider)
	if err != nil {
		return nil, err
	}
	conn, err := newClickhouseConn(cfg)
	if err != nil {
		return nil, err
	}
	writtenResources, err := lru.NewWithEvict[uuid.UUID, time.Time](logsResourceCacheSize, func(_ uuid.UUID, _ time.Time) {
		telemetry.resourceCacheEvictions.Add(context.Background(), 1)
	})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &LogsExporter{
		conn: conn, insertSQL: renderInsertLogsSQL(cfg),
		resourceInsertSQL: fmt.Sprintf(insertLogResourcesSQLTemplate, cfg.LogsResourcesTableName),
		writtenResources:  writtenResources, telemetry: telemetry,
	}, nil
}

func (e *LogsExporter) Start(_ context.Context, _ component.Host) error { return nil }

func (e *LogsExporter) Shutdown(_ context.Context) error { return e.conn.Close() }

type logRow struct {
	sourceTimestamp   time.Time
	observedTimestamp time.Time
	resource          *logResourceModel
	scopeName         string
	scopeVersion      string
	scopeAttributes   typedAttributes
	traceID           string
	spanID            string
	traceFlags        uint8
	severityText      string
	severityNumber    uint8
	eventName         string
	body              string
	bodyOther         string
	logAttributes     typedAttributes
}

func (e *LogsExporter) PushLogsData(ctx context.Context, ld plog.Logs) error {
	rows, resources, stats, err := logRowsFromPData(ld)
	var rejection error
	if err != nil {
		rejection = consumererror.NewLogs(consumererror.NewPermanent(err), stats.rejected)
	}
	if stats.missingTimestamps > 0 {
		e.telemetry.missingTimestamps.Add(ctx, int64(stats.missingTimestamps))
	}
	if stats.severityClamped > 0 {
		e.telemetry.severityClamped.Add(ctx, int64(stats.severityClamped))
	}
	if len(rows) == 0 {
		return rejection
	}
	// Resource acknowledgement precedes facts, so a failed resource write cannot orphan a log.
	if err := e.insertResources(ctx, resources); err != nil {
		return fmt.Errorf("insert log resources: %w", err)
	}
	if err := e.insertLogs(ctx, rows); err != nil {
		return err
	}
	return rejection
}

func (e *LogsExporter) insertResources(ctx context.Context, resources []*logResourceModel) error {
	now := time.Now()
	pending := make([]*logResourceModel, 0, len(resources))
	for _, resource := range resources {
		writtenAt, cached := e.writtenResources.Get(resource.resourceRef)
		if !cached || now.Sub(writtenAt) >= logsResourceRefreshInterval {
			pending = append(pending, resource)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	batch, err := e.conn.PrepareBatch(ctx, e.resourceInsertSQL)
	if err != nil {
		return fmt.Errorf("prepare resources: %w", err)
	}
	for _, resource := range pending {
		attrs := resource.attributes
		if err := batch.Append(
			now, resource.resourceRef, attrs.strings, attrs.ints, attrs.doubles, attrs.bools, attrs.other, resource.authScope,
		); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append resource: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send resources: %w", err)
	}
	for _, resource := range pending {
		e.writtenResources.Add(resource.resourceRef, now)
	}
	return nil
}

func (e *LogsExporter) insertLogs(ctx context.Context, rows []logRow) error {
	batch, err := e.conn.PrepareBatch(ctx, e.insertSQL)
	if err != nil {
		return fmt.Errorf("prepare logs: %w", err)
	}
	for _, row := range rows {
		scope, record := row.scopeAttributes, row.logAttributes
		if err := batch.Append(
			row.sourceTimestamp, row.observedTimestamp, row.resource.resourceRef, row.resource.serviceName,
			row.scopeName, row.scopeVersion, scope.strings, scope.ints, scope.doubles, scope.bools, scope.other,
			row.traceID, row.spanID, row.traceFlags, row.severityText, row.severityNumber,
			row.eventName, row.body, row.bodyOther,
			record.strings, record.ints, record.doubles, record.bools, record.other, row.resource.authScope,
		); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append log: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send logs: %w", err)
	}
	return nil
}

type logRowsStats struct {
	missingTimestamps, severityClamped int
	rejected                           plog.Logs
	diagnostics                        error
}

func (s *logRowsStats) reject(ld plog.Logs, resourceIndex, scopeIndex, recordIndex int, err error) {
	s.diagnostics = errors.Join(s.diagnostics, err)
	resource := ld.ResourceLogs().At(resourceIndex)
	targetResource := s.rejected.ResourceLogs().AppendEmpty()
	if scopeIndex < 0 {
		resource.CopyTo(targetResource)
		return
	}
	resource.Resource().CopyTo(targetResource.Resource())
	targetResource.SetSchemaUrl(resource.SchemaUrl())
	scope := resource.ScopeLogs().At(scopeIndex)
	targetScope := targetResource.ScopeLogs().AppendEmpty()
	if recordIndex < 0 {
		scope.CopyTo(targetScope)
		return
	}
	scope.Scope().CopyTo(targetScope.Scope())
	targetScope.SetSchemaUrl(scope.SchemaUrl())
	scope.LogRecords().At(recordIndex).CopyTo(targetScope.LogRecords().AppendEmpty())
}

func logRowsFromPData(ld plog.Logs) ([]logRow, []*logResourceModel, logRowsStats, error) {
	rows := make([]logRow, 0, ld.LogRecordCount())
	resources := make([]*logResourceModel, 0, ld.ResourceLogs().Len())
	seenResources := make(map[uuid.UUID]struct{}, ld.ResourceLogs().Len())
	stats := logRowsStats{rejected: plog.NewLogs()}
resourceLoop:
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		resourceLogs := ld.ResourceLogs().At(i)
		var resource *logResourceModel
	scopeLoop:
		for j := 0; j < resourceLogs.ScopeLogs().Len(); j++ {
			scopeLogs := resourceLogs.ScopeLogs().At(j)
			scope := scopeLogs.Scope()
			var scopeAttributes typedAttributes
			for k := 0; k < scopeLogs.LogRecords().Len(); k++ {
				record := scopeLogs.LogRecords().At(k)
				if record.Timestamp() == 0 && record.ObservedTimestamp() == 0 {
					stats.missingTimestamps++
					continue
				}
				var err error
				if resource == nil {
					resource, err = newLogResourceModel(resourceLogs.Resource())
					if err != nil {
						stats.reject(ld, i, -1, -1, fmt.Errorf("resource group %d: %w", i, err))
						continue resourceLoop
					}
				}
				if scopeAttributes.strings == nil {
					scopeAttributes, err = encodeLogAttributes(scope.Attributes())
					if err != nil {
						stats.reject(ld, i, j, -1, fmt.Errorf("resource group %d scope group %d: %w", i, j, err))
						continue scopeLoop
					}
				}
				attributes, err := encodeLogAttributes(record.Attributes())
				if err != nil {
					stats.reject(ld, i, j, k, fmt.Errorf("resource group %d scope group %d record %d: %w", i, j, k, err))
					continue
				}
				body, bodyOther := "", ""
				if record.Body().Type() == pcommon.ValueTypeStr && record.Body().Str() != "" {
					body = record.Body().Str()
				} else if record.Body().Type() != pcommon.ValueTypeEmpty {
					bodyOther, err = encodeLogAnyValue(record.Body())
					if err != nil {
						stats.reject(ld, i, j, k, fmt.Errorf("resource group %d scope group %d record %d body: %w", i, j, k, err))
						continue
					}
				}
				severity, clamped := severityNumber(record.SeverityNumber())
				if clamped {
					stats.severityClamped++
				}
				if _, seen := seenResources[resource.resourceRef]; !seen {
					seenResources[resource.resourceRef] = struct{}{}
					resources = append(resources, resource)
				}
				traceID, spanID := record.TraceID(), record.SpanID()
				rows = append(rows, logRow{
					sourceTimestamp: record.Timestamp().AsTime(), observedTimestamp: record.ObservedTimestamp().AsTime(),
					resource:  resource,
					scopeName: scope.Name(), scopeVersion: scope.Version(), scopeAttributes: scopeAttributes,
					traceID: string(traceID[:]), spanID: string(spanID[:]), traceFlags: uint8(record.Flags() & 0xff),
					severityText: record.SeverityText(), severityNumber: severity, eventName: record.EventName(),
					body: body, bodyOther: bodyOther,
					logAttributes: attributes,
				})
			}
		}
	}
	return rows, resources, stats, stats.diagnostics
}

func severityNumber(n plog.SeverityNumber) (uint8, bool) {
	if n < plog.SeverityNumberUnspecified || n > plog.SeverityNumberFatal4 {
		return uint8(plog.SeverityNumberUnspecified), true
	}
	return uint8(n), false
}

func attributesToMap(attributes pcommon.Map) map[string]string {
	m := make(map[string]string, attributes.Len())
	attributes.Range(func(k string, v pcommon.Value) bool { m[k] = v.AsString(); return true })
	return m
}

const insertLogsSQLTemplate = `INSERT INTO %s (
    SourceTimestamp, ObservedTimestamp, ResourceRef, ServiceName, ScopeName, ScopeVersion,
    ScopeStrings, ScopeInts, ScopeDoubles, ScopeBools, ScopeOther, TraceId, SpanId, TraceFlags,
    SeverityText, SeverityNumber, EventName, Body, BodyOther, Strings, Ints, Doubles, Bools, Other, AuthScope
)`

const insertLogResourcesSQLTemplate = `INSERT INTO %s (
    Timestamp, ResourceRef, Strings, Ints, Doubles, Bools, Other, AuthScope
)`

// newClickhouseConn opens a native-API connection, used for batch inserts.
func newClickhouseConn(cfg *Config) (driver.Conn, error) {
	dsn, err := cfg.BuildDSN(cfg.Database)
	if err != nil {
		return nil, err
	}
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return clickhouse.Open(opts)
}

// newClickhouseClient create a clickhouse client.
func newClickhouseClient(cfg *Config) (*sql.DB, error) {
	db, err := cfg.BuildDB(cfg.Database)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func createDatabase(ctx context.Context, cfg *Config) error {
	// use default database to create new database
	if cfg.Database == defaultDatabase {
		return nil
	}

	db, err := cfg.BuildDB(defaultDatabase)
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", cfg.Database)
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create database:%w", err)
	}
	return nil
}

func renderInsertLogsSQL(cfg *Config) string {
	return fmt.Sprintf(insertLogsSQLTemplate, cfg.LogsTableName)
}

func doWithTx(_ context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("db.Begin: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
