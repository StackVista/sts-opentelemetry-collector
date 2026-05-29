// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:lll
package clickhousestsexporter // import "github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter"

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // For register database driver.
	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter/internal"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.uber.org/zap"
)

type LogsExporter struct {
	client           *sql.DB
	insertSQL        string
	resourceExporter *ResourcesExporter

	logger *zap.Logger
	cfg    *Config
}

func NewLogsExporter(logger *zap.Logger, cfg *Config) (*LogsExporter, error) {
	client, err := newClickhouseClient(cfg)
	if err != nil {
		return nil, err
	}
	resourceExporter, err := NewResourceExporter(logger, cfg)
	if err != nil {
		return nil, err
	}

	return &LogsExporter{
		client:           client,
		insertSQL:        renderInsertLogsSQL(cfg),
		resourceExporter: resourceExporter,
		logger:           logger,
		cfg:              cfg,
	}, nil
}

func (e *LogsExporter) Start(ctx context.Context, host component.Host) error {
	if err := e.resourceExporter.Start(ctx, host); err != nil {
		return err
	}

	if !e.cfg.CreateLogsTable {
		return nil
	}

	if err := createDatabase(ctx, e.cfg); err != nil {
		return err
	}

	return createLogsTable(ctx, e.cfg, e.client)
}

// shutdown will shut down the exporter.
func (e *LogsExporter) Shutdown(ctx context.Context) error {
	err := e.resourceExporter.Shutdown(ctx)
	if err != nil {
		return errors.New("unable to shutdown the resource exporter")
	}

	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (e *LogsExporter) PushLogsData(ctx context.Context, ld plog.Logs) error {
	start := time.Now()
	records, resources, err := logRowsFromPData(ld)
	if err != nil {
		return err
	}

	if err := e.resourceExporter.InsertResources(ctx, resources); err != nil {
		return err
	}

	err = doWithTx(ctx, e.client, func(tx *sql.Tx) error {
		statement, err := tx.PrepareContext(ctx, e.insertSQL)
		if err != nil {
			return fmt.Errorf("PrepareContext:%w", err)
		}
		defer func() {
			_ = statement.Close()
		}()

		for _, record := range records {
			_, err = statement.ExecContext(ctx,
				record.timestamp,
				record.observedTimestamp,
				record.timestampSource,
				record.logID,
				record.sourceSystem,
				record.sourceRecordID,
				record.migrationBatchID,
				record.traceID,
				record.spanID,
				record.traceFlags,
				record.severityText,
				record.severityNumber,
				record.body,
				record.bodyType,
				record.bodyJSON,
				record.eventName,
				record.resourceSchemaURL,
				record.resource.resourceRef,
				record.scopeSchemaURL,
				record.scopeName,
				record.scopeVersion,
				record.scopeAttributes,
				record.scopeAttributesJSON,
				record.serviceName,
				record.logAttributes,
				record.logAttributesJSON,
				record.resource.authScope,
			)
			if err != nil {
				return fmt.Errorf("ExecContext:%w", err)
			}
		}
		return nil
	})
	duration := time.Since(start)
	e.logger.Debug("insert logs", zap.Int("records", ld.LogRecordCount()),
		zap.String("cost", duration.String()))
	return err
}

type logRow struct {
	timestamp           time.Time
	observedTimestamp   time.Time
	timestampSource     string
	logID               string
	sourceSystem        string
	sourceRecordID      string
	migrationBatchID    string
	traceID             string
	spanID              string
	traceFlags          uint8
	severityText        string
	severityNumber      uint8
	body                string
	bodyType            string
	bodyJSON            string
	eventName           string
	resourceSchemaURL   string
	resource            *ResourceModel
	scopeSchemaURL      string
	scopeName           string
	scopeVersion        string
	scopeAttributes     map[string]string
	scopeAttributesJSON string
	serviceName         string
	logAttributes       map[string]string
	logAttributesJSON   string
}

func logRowsFromPData(ld plog.Logs) ([]logRow, []*ResourceModel, error) {
	records := make([]logRow, 0, ld.LogRecordCount())
	resources := make([]*ResourceModel, 0, ld.ResourceLogs().Len())
	seenResources := make(map[string]struct{}, ld.ResourceLogs().Len())

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		logs := ld.ResourceLogs().At(i)
		res, err := NewResourceModel(logs.Resource())
		if err != nil {
			return nil, nil, err
		}
		resourceRef := res.resourceRef.String()
		if _, ok := seenResources[resourceRef]; !ok {
			resources = append(resources, res)
			seenResources[resourceRef] = struct{}{}
		}

		resourceSchemaURL := logs.SchemaUrl()
		serviceName := ""
		if v, ok := logs.Resource().Attributes().Get(string(conventions.ServiceNameKey)); ok {
			serviceName = v.Str()
		}

		for j := 0; j < logs.ScopeLogs().Len(); j++ {
			scopeLogs := logs.ScopeLogs().At(j)
			scopeSchemaURL := scopeLogs.SchemaUrl()
			scope := scopeLogs.Scope()
			scopeAttributes := attributesToMap(scope.Attributes())
			scopeAttributesJSON := mustJSON(scopeAttributes)
			rs := scopeLogs.LogRecords()

			for k := 0; k < rs.Len(); k++ {
				record := rs.At(k)
				timestamp, observedTimestamp, timestampSource := logTimestamps(record)
				body, bodyType, bodyJSON := logBody(record.Body())
				logAttributes := attributesToMap(record.Attributes())
				logAttributesJSON := mustJSON(logAttributes)
				eventName := ""
				if v, ok := record.Attributes().Get("event.name"); ok {
					eventName = v.AsString()
				}
				logID := logRecordID(res, record, i, j, k)

				records = append(records, logRow{
					timestamp:           timestamp,
					observedTimestamp:   observedTimestamp,
					timestampSource:     timestampSource,
					logID:               logID,
					sourceSystem:        "otlp-collector",
					sourceRecordID:      logID,
					traceID:             TraceIDToHexOrEmptyString(record.TraceID()),
					spanID:              SpanIDToHexOrEmptyString(record.SpanID()),
					traceFlags:          uint8(record.Flags()),
					severityText:        record.SeverityText(),
					severityNumber:      uint8(record.SeverityNumber()),
					body:                body,
					bodyType:            bodyType,
					bodyJSON:            bodyJSON,
					eventName:           eventName,
					resourceSchemaURL:   resourceSchemaURL,
					resource:            res,
					scopeSchemaURL:      scopeSchemaURL,
					scopeName:           scope.Name(),
					scopeVersion:        scope.Version(),
					scopeAttributes:     scopeAttributes,
					scopeAttributesJSON: scopeAttributesJSON,
					serviceName:         serviceName,
					logAttributes:       logAttributes,
					logAttributesJSON:   logAttributesJSON,
				})
			}
		}
	}
	return records, resources, nil
}

func logTimestamps(record plog.LogRecord) (time.Time, time.Time, string) {
	timestamp := record.Timestamp()
	observedTimestamp := record.ObservedTimestamp()
	if timestamp != 0 {
		if observedTimestamp == 0 {
			observedTimestamp = timestamp
		}
		return timestamp.AsTime(), observedTimestamp.AsTime(), "timestamp"
	}
	if observedTimestamp != 0 {
		return observedTimestamp.AsTime(), observedTimestamp.AsTime(), "observed_timestamp"
	}
	now := time.Now()
	return now, now, "ingested_at"
}

func logBody(body pcommon.Value) (string, string, string) {
	switch body.Type() {
	case pcommon.ValueTypeEmpty:
		return "", "empty", ""
	case pcommon.ValueTypeStr:
		return body.Str(), "string", ""
	case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		return body.AsString(), strings.ToLower(body.Type().String()), mustJSON(body.AsRaw())
	default:
		return body.AsString(), strings.ToLower(body.Type().String()), ""
	}
}

func logRecordID(resource *ResourceModel, record plog.LogRecord, resourceIndex, scopeIndex, logIndex int) string {
	parts := []string{
		resource.resourceRef.String(),
		record.Timestamp().String(),
		record.ObservedTimestamp().String(),
		TraceIDToHexOrEmptyString(record.TraceID()),
		SpanIDToHexOrEmptyString(record.SpanID()),
		record.SeverityText(),
		fmt.Sprintf("%d", record.SeverityNumber()),
		record.Body().AsString(),
		mustJSON(attributesToMap(record.Attributes())),
		fmt.Sprintf("%d/%d/%d", resourceIndex, scopeIndex, logIndex),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func mustJSON(value any) string {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(bytes)
}

func attributesToMap(attributes pcommon.Map) map[string]string {
	m := make(map[string]string, attributes.Len())
	attributes.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})
	return m
}

const (
	// language=ClickHouse SQL
	createLogsTableSQL = `
	CREATE TABLE IF NOT EXISTS %s (
	     Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
	     ObservedTimestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
	     TimestampSource LowCardinality(String) CODEC(ZSTD(1)),
	     IngestedAt DateTime64(9) DEFAULT now64(9) CODEC(Delta, ZSTD(1)),
	     LogId String CODEC(ZSTD(1)),
	     SourceSystem LowCardinality(String) CODEC(ZSTD(1)),
	     SourceRecordId String CODEC(ZSTD(1)),
	     MigrationBatchId String CODEC(ZSTD(1)),
	     TraceId String CODEC(ZSTD(1)),
	     SpanId String CODEC(ZSTD(1)),
	     TraceFlags UInt8 CODEC(ZSTD(1)),
	     SeverityText LowCardinality(String) CODEC(ZSTD(1)),
	     SeverityNumber UInt8 CODEC(ZSTD(1)),
	     Body String CODEC(ZSTD(1)),
	     BodyType LowCardinality(String) CODEC(ZSTD(1)),
	     BodyJson String CODEC(ZSTD(1)),
	     EventName String CODEC(ZSTD(1)),
	     ResourceSchemaUrl String CODEC(ZSTD(1)),
	     ResourceRef UUID CODEC(ZSTD(1)),
	     ScopeSchemaUrl String CODEC(ZSTD(1)),
	     ScopeName String CODEC(ZSTD(1)),
	     ScopeVersion String CODEC(ZSTD(1)),
	     ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	     ScopeAttributesJson String CODEC(ZSTD(1)),
	     ServiceName LowCardinality(String) CODEC(ZSTD(1)),
	     LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	     LogAttributesJson String CODEC(ZSTD(1)),
	     AuthScope Array(LowCardinality(String)) CODEC(ZSTD(1)),
	     INDEX idx_log_id LogId TYPE bloom_filter(0.001) GRANULARITY 1,
	     INDEX idx_service_name ServiceName TYPE bloom_filter(0.001) GRANULARITY 1,
	     INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
	     INDEX idx_span_id SpanId TYPE bloom_filter(0.001) GRANULARITY 1,
	     INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	     INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	     INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	     INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	     INDEX idx_body_ngram_lc lowerUTF8(Body) TYPE ngrambf_v1(3, 65536, 3, 0) GRANULARITY 1,
	     INDEX idx_severity SeverityNumber TYPE minmax GRANULARITY 1
	) ENGINE MergeTree()
	%s
	PARTITION BY toDate(Timestamp)
	ORDER BY (ResourceRef, toUnixTimestamp(Timestamp), LogId)
	SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
	`
	// language=ClickHouse SQL
	insertLogsSQLTemplate = `INSERT INTO %s (
	                        Timestamp,
	                        ObservedTimestamp,
	                        TimestampSource,
	                        LogId,
	                        SourceSystem,
	                        SourceRecordId,
	                        MigrationBatchId,
	                        TraceId,
	                        SpanId,
	                        TraceFlags,
	                        SeverityText,
	                        SeverityNumber,
	                        Body,
	                        BodyType,
	                        BodyJson,
	                        EventName,
	                        ResourceSchemaUrl,
	                        ResourceRef,
	                        ScopeSchemaUrl,
	                        ScopeName,
	                        ScopeVersion,
	                        ScopeAttributes,
	                        ScopeAttributesJson,
	                        ServiceName,
	                        LogAttributes,
	                        LogAttributesJson,
	                        AuthScope
	                        ) VALUES (
	                                  ?, ?, ?, ?, ?, ?, ?, ?, ?,
	                                  ?, ?, ?, ?, ?, ?, ?, ?, ?,
	                                  ?, ?, ?, ?, ?, ?, ?, ?, ?
	                                  )`
)

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

func createLogsTable(ctx context.Context, cfg *Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateLogsTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create logs table sql: %w", err)
	}
	return nil
}

func renderCreateLogsTableSQL(cfg *Config) string {
	ttlExpr := internal.GenerateTTLExpr(cfg.TTLDays, cfg.TTL, "Timestamp")
	return fmt.Sprintf(createLogsTableSQL, cfg.LogsTableName, ttlExpr)
}

func renderInsertLogsSQL(cfg *Config) string {
	return fmt.Sprintf(strings.ReplaceAll(insertLogsSQLTemplate, "'", "`"), cfg.LogsTableName)
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
