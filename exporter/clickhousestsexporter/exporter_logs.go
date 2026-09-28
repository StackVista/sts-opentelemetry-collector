// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:lll
package clickhousestsexporter // import "github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter"

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter/internal"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.uber.org/zap"
)

const (
	// Active resources are re-written at most once per interval; the resources table TTL is one day
	// longer than the logs TTL so a resource never expires before its newest logs.
	logsResourceRefreshInterval = time.Hour
	logsResourceCacheSize       = 100_000
)

type LogsExporter struct {
	// client runs DDL; inserts use the native batch API on conn.
	client           *sql.DB
	conn             driver.Conn
	insertSQL        string
	resourceExporter *ResourcesExporter
	writtenResources *lru.Cache[uuid.UUID, time.Time]

	logger *zap.Logger
	cfg    *Config
}

func NewLogsExporter(logger *zap.Logger, cfg *Config) (*LogsExporter, error) {
	client, err := newClickhouseClient(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := newClickhouseConn(cfg)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	resourceExporter, err := newResourceExporter(
		logger, withOneDayTTLSlack(cfg), cfg.LogsResourcesTableName, cfg.CreateLogsTable,
	)
	if err != nil {
		_ = conn.Close()
		_ = client.Close()
		return nil, err
	}
	writtenResources, err := lru.New[uuid.UUID, time.Time](logsResourceCacheSize)
	if err != nil {
		_ = conn.Close()
		_ = client.Close()
		return nil, err
	}

	return &LogsExporter{
		client:           client,
		conn:             conn,
		insertSQL:        renderInsertLogsSQL(cfg),
		resourceExporter: resourceExporter,
		writtenResources: writtenResources,
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
	if err := e.resourceExporter.Shutdown(ctx); err != nil {
		return errors.New("unable to shutdown the resource exporter")
	}

	var errs error
	if e.conn != nil {
		errs = errors.Join(errs, e.conn.Close())
	}
	if e.client != nil {
		errs = errors.Join(errs, e.client.Close())
	}
	return errs
}

type logRow struct {
	timestamp         time.Time
	observedTimestamp time.Time
	resource          *ResourceModel
	resourceSchemaURL string
	serviceName       string
	scopeName         string
	scopeVersion      string
	scopeSchemaURL    string
	scopeAttributes   map[string]string
	traceID           string
	spanID            string
	traceFlags        uint8
	severityText      string
	severityNumber    uint8
	body              string
	logAttributes     map[string]string
}

// PushLogsData writes log records to ClickHouse. Records with an EventName are OTel events, not logs, and are skipped.
func (e *LogsExporter) PushLogsData(ctx context.Context, ld plog.Logs) error {
	start := time.Now()
	rows, resources, err := logRowsFromPData(ld)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	// Resources go first: a retry after a failed logs insert only re-writes resources, which the ReplacingMergeTree collapses.
	if err := e.insertResources(ctx, resources); err != nil {
		return fmt.Errorf("insert log resources: %w", err)
	}

	err = e.insertLogs(ctx, rows)
	e.logger.Debug("insert logs", zap.Int("records", len(rows)),
		zap.Int("skipped", ld.LogRecordCount()-len(rows)),
		zap.String("cost", time.Since(start).String()))
	return err
}

func (e *LogsExporter) insertResources(ctx context.Context, resources []*ResourceModel) error {
	now := time.Now()
	pending := make([]*ResourceModel, 0, len(resources))
	for _, resource := range resources {
		if writtenAt, ok := e.writtenResources.Get(resource.resourceRef); !ok || now.Sub(writtenAt) >= logsResourceRefreshInterval {
			pending = append(pending, resource)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	if err := e.resourceExporter.InsertResources(ctx, pending); err != nil {
		return err
	}
	for _, resource := range pending {
		e.writtenResources.Add(resource.resourceRef, now)
	}
	return nil
}

func (e *LogsExporter) insertLogs(ctx context.Context, rows []logRow) error {
	batch, err := e.conn.PrepareBatch(ctx, e.insertSQL)
	if err != nil {
		return fmt.Errorf("PrepareBatch:%w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.timestamp,
			row.observedTimestamp,
			row.resource.resourceRef,
			row.resourceSchemaURL,
			row.serviceName,
			row.scopeName,
			row.scopeVersion,
			row.scopeSchemaURL,
			row.scopeAttributes,
			row.traceID,
			row.spanID,
			row.traceFlags,
			row.severityText,
			row.severityNumber,
			row.body,
			row.logAttributes,
			row.resource.authScope,
		); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("Append:%w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("Send:%w", err)
	}
	return nil
}

func logRowsFromPData(ld plog.Logs) ([]logRow, []*ResourceModel, error) {
	rows := make([]logRow, 0, ld.LogRecordCount())
	resources := make([]*ResourceModel, 0, ld.ResourceLogs().Len())
	seenResources := make(map[uuid.UUID]struct{}, ld.ResourceLogs().Len())

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		resourceLogs := ld.ResourceLogs().At(i)
		var resource *ResourceModel
		var serviceName string
		if v, ok := resourceLogs.Resource().Attributes().Get(string(conventions.ServiceNameKey)); ok {
			serviceName = v.Str()
		}

		for j := 0; j < resourceLogs.ScopeLogs().Len(); j++ {
			scopeLogs := resourceLogs.ScopeLogs().At(j)
			scope := scopeLogs.Scope()
			var scopeAttributes map[string]string
			records := scopeLogs.LogRecords()
			for k := 0; k < records.Len(); k++ {
				record := records.At(k)
				if record.EventName() != "" {
					continue
				}
				if resource == nil {
					var err error
					if resource, err = NewResourceModel(resourceLogs.Resource()); err != nil {
						return nil, nil, err
					}
					if _, seen := seenResources[resource.resourceRef]; !seen {
						seenResources[resource.resourceRef] = struct{}{}
						resources = append(resources, resource)
					}
				}
				if scopeAttributes == nil {
					scopeAttributes = attributesToMap(scope.Attributes())
				}
				rows = append(rows, logRow{
					timestamp:         logTimestamp(record),
					observedTimestamp: record.ObservedTimestamp().AsTime(),
					resource:          resource,
					resourceSchemaURL: resourceLogs.SchemaUrl(),
					serviceName:       serviceName,
					scopeName:         scope.Name(),
					scopeVersion:      scope.Version(),
					scopeSchemaURL:    scopeLogs.SchemaUrl(),
					scopeAttributes:   scopeAttributes,
					traceID:           TraceIDToHexOrEmptyString(record.TraceID()),
					spanID:            SpanIDToHexOrEmptyString(record.SpanID()),
					traceFlags:        uint8(record.Flags() & 0xff),
					severityText:      record.SeverityText(),
					severityNumber:    severityNumber(record.SeverityNumber()),
					body:              record.Body().AsString(),
					logAttributes:     attributesToMap(record.Attributes()),
				})
			}
		}
	}
	return rows, resources, nil
}

func severityNumber(n plog.SeverityNumber) uint8 {
	if n < 0 || n > math.MaxUint8 {
		return uint8(plog.SeverityNumberUnspecified)
	}
	return uint8(n)
}

// logTimestamp follows the OTel log data model: fall back to the observed time when the event time is unknown.
func logTimestamp(record plog.LogRecord) time.Time {
	if record.Timestamp() != 0 {
		return record.Timestamp().AsTime()
	}
	if record.ObservedTimestamp() != 0 {
		return record.ObservedTimestamp().AsTime()
	}
	return time.Now()
}

// withOneDayTTLSlack returns a copy of cfg whose table TTL is one day longer; a disabled TTL stays disabled.
func withOneDayTTLSlack(cfg *Config) *Config {
	c := *cfg
	switch {
	case c.TTL > 0:
		c.TTL += 24 * time.Hour
	case c.TTLDays > 0:
		c.TTLDays++
	}
	return &c
}

func attributesToMap(attributes pcommon.Map) map[string]string {
	m := make(map[string]string, attributes.Len())
	attributes.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})
	return m
}

// StackState owns the deployed schema (stackstate-traces OtelSchema); this is its non-replicated
// equivalent for running the exporter standalone. Keep the two in sync.
const (
	// language=ClickHouse SQL
	createLogsTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
     Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
     ObservedTimestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
     ResourceRef UUID CODEC(ZSTD(1)),
     ResourceSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
     ServiceName LowCardinality(String) CODEC(ZSTD(1)),
     ScopeName String CODEC(ZSTD(1)),
     ScopeVersion String CODEC(ZSTD(1)),
     ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
     ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
     TraceId String CODEC(ZSTD(1)),
     SpanId String CODEC(ZSTD(1)),
     TraceFlags UInt8 CODEC(ZSTD(1)),
     SeverityText LowCardinality(String) CODEC(ZSTD(1)),
     SeverityNumber UInt8 CODEC(ZSTD(1)),
     Body String CODEC(ZSTD(1)),
     LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
     AuthScope Array(LowCardinality(String)),
     INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
     INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
     INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
     INDEX idx_severity_number SeverityNumber TYPE minmax GRANULARITY 1,
     INDEX idx_body lowerUTF8(Body) TYPE ngrambf_v1(3, 65536, 3, 0) GRANULARITY 1
) ENGINE MergeTree()
%s
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, ResourceRef, toUnixTimestamp(Timestamp))
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	insertLogsSQLTemplate = `INSERT INTO %s (
                        Timestamp,
                        ObservedTimestamp,
                        ResourceRef,
                        ResourceSchemaUrl,
                        ServiceName,
                        ScopeName,
                        ScopeVersion,
                        ScopeSchemaUrl,
                        ScopeAttributes,
                        TraceId,
                        SpanId,
                        TraceFlags,
                        SeverityText,
                        SeverityNumber,
                        Body,
                        LogAttributes,
                        AuthScope
                        )`
)

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
