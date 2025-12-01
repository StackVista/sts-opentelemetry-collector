// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:lll
package clickhousestsexporter // import "github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter"

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // For register database driver.
	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter/internal"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.uber.org/zap"
)

type LogsExporter struct {
	client    *sql.DB
	insertSQL string

	logger           *zap.Logger
	cfg              *Config
	topologyExporter *TopologyExporter
}

func NewLogsExporter(logger *zap.Logger, cfg *Config) (*LogsExporter, error) {
	client, err := newClickhouseClient(cfg)
	if err != nil {
		return nil, err
	}

	topologyExporter, err := NewTopologyExporter(logger, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create topology exporter: %w", err)
	}

	return &LogsExporter{
		client:           client,
		insertSQL:        renderInsertLogsSQL(cfg),
		logger:           logger,
		cfg:              cfg,
		topologyExporter: topologyExporter,
	}, nil
}

func (e *LogsExporter) Start(ctx context.Context, host component.Host) error {
	if err := e.topologyExporter.Start(ctx, host); err != nil {
		return fmt.Errorf("failed to start topology exporter: %w", err)
	}
	if err := createDatabase(ctx, e.cfg); err != nil {
		return err
	}

	return createLogsTable(ctx, e.cfg, e.client)
}

// shutdown will shut down the exporter.
func (e *LogsExporter) Shutdown(ctx context.Context) error {
	if err := e.topologyExporter.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown topology exporter: %w", err)
	}
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (e *LogsExporter) PushLogsData(ctx context.Context, ld plog.Logs) error {
	regularLogs := plog.NewLogs()
	topologyLogs := plog.NewLogs()

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		destRlRegular := regularLogs.ResourceLogs().AppendEmpty()
		rl.Resource().CopyTo(destRlRegular.Resource())

		destRlTopology := topologyLogs.ResourceLogs().AppendEmpty()
		rl.Resource().CopyTo(destRlTopology.Resource())

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			destSlRegular := destRlRegular.ScopeLogs().AppendEmpty()
			sl.Scope().CopyTo(destSlRegular.Scope())

			destSlTopology := destRlTopology.ScopeLogs().AppendEmpty()
			sl.Scope().CopyTo(destSlTopology.Scope())

			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				if _, ok := lr.Attributes().Get(stskafkaexporter.KafkaMessageKey); ok {
					lr.CopyTo(destSlTopology.LogRecords().AppendEmpty())
				} else {
					lr.CopyTo(destSlRegular.LogRecords().AppendEmpty())
				}
			}
		}
	}

	var regularErr error
	if regularLogs.LogRecordCount() > 0 {
		regularErr = e.pushRegularLogs(ctx, regularLogs)
	}

	var topologyErr error
	if topologyLogs.LogRecordCount() > 0 {
		topologyErr = e.topologyExporter.PushLogsData(ctx, topologyLogs)
	}

	if regularErr != nil && topologyErr != nil {
		return fmt.Errorf("failed to push logs: regular: %v, topology: %v", regularErr, topologyErr)
	}
	if regularErr != nil {
		return fmt.Errorf("failed to push regular logs: %w", regularErr)
	}
	if topologyErr != nil {
		return fmt.Errorf("failed to push topology logs: %w", topologyErr)
	}

	return nil
}

func (e *LogsExporter) pushRegularLogs(ctx context.Context, ld plog.Logs) error {
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
     TraceId String CODEC(ZSTD(1)),
     SpanId String CODEC(ZSTD(1)),
     TraceFlags UInt32 CODEC(ZSTD(1)),
     SeverityText LowCardinality(String) CODEC(ZSTD(1)),
     SeverityNumber Int32 CODEC(ZSTD(1)),
     ServiceName LowCardinality(String) CODEC(ZSTD(1)),
     Body String CODEC(ZSTD(1)),
     ResourceSchemaUrl String CODEC(ZSTD(1)),
     ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
     ScopeSchemaUrl String CODEC(ZSTD(1)),
     ScopeName String CODEC(ZSTD(1)),
     ScopeVersion String CODEC(ZSTD(1)),
     ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
     LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
     INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
     INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
     INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
     INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
     INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
     INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
     INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
     INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1
) ENGINE MergeTree()
%s
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SeverityText, toUnixTimestamp(Timestamp), TraceId)
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	insertLogsSQLTemplate = `INSERT INTO %s (
                        Timestamp,
                        TraceId,
                        SpanId,
                        TraceFlags,
                        SeverityText,
                        SeverityNumber,
                        ServiceName,
                        Body,
                        ResourceSchemaUrl,
                        ResourceAttributes,
                        ScopeSchemaUrl,
                        ScopeName,
                        ScopeVersion,
                        ScopeAttributes,
                        LogAttributes
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
                                  ?
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
