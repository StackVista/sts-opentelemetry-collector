// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:lll
package clickhousestsexporter // import "github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter"

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // For register database driver.
	topostream "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter/internal"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter/internal/topology"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type LogsExporter struct {
	client            *sql.DB
	insertSQL         string
	insertTopologySQL string

	logger *zap.Logger
	cfg    *Config
}

func NewLogsExporter(logger *zap.Logger, cfg *Config) (*LogsExporter, error) {
	client, err := newClickhouseClient(cfg)
	if err != nil {
		return nil, err
	}

	return &LogsExporter{
		client:            client,
		insertSQL:         renderInsertLogsSQL(cfg),
		insertTopologySQL: topology.RenderInsertTopologySQL(cfg),
		logger:            logger,
		cfg:               cfg,
	}, nil
}

func (e *LogsExporter) Start(ctx context.Context, _ component.Host) error {
	if err := createDatabase(ctx, e.cfg); err != nil {
		return err
	}

	if e.cfg.EnableLogs && e.cfg.CreateLogsTable {
		if err := createLogsTable(ctx, e.cfg, e.client); err != nil {
			return err
		}
	}

	if e.cfg.EnableTopology && e.cfg.CreateTopologyTable {
		if err := topology.CreateTopologyTable(ctx, e.cfg, e.client); err != nil {
			return fmt.Errorf("failed to create topology table: %w", err)
		}
		if err := topology.CreateTopologyTimeRangeTable(ctx, e.cfg, e.client); err != nil {
			return fmt.Errorf("failed to create topology time range table: %w", err)
		}
		if err := topology.CreateTopologyTimeRangeMV(ctx, e.cfg, e.client); err != nil {
			return fmt.Errorf("failed to create topology time range materialized view: %w", err)
		}
		if err := topology.CreateTopologyFieldValuesTable(ctx, e.cfg, e.client); err != nil {
			return fmt.Errorf("failed to create topology field values table: %w", err)
		}
		if err := topology.CreateTopologyFieldValuesMV(ctx, e.cfg, e.client); err != nil {
			return fmt.Errorf("failed to create topology field values materialized view: %w", err)
		}
	}

	if e.cfg.EnableLogs {
		e.logger.Info("Started logs exporter.")
	}
	if e.cfg.EnableTopology {
		e.logger.Info("Started topology logs exporter.")
	}
	return nil
}

// shutdown will shut down the exporter.
func (e *LogsExporter) Shutdown(_ context.Context) error {
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (e *LogsExporter) PushLogsData(ctx context.Context, ld plog.Logs) error {
	start := time.Now()
	err := doWithTx(ctx, e.client, func(tx *sql.Tx) error {
		var logStatement *sql.Stmt
		if e.cfg.EnableLogs {
			logStatement, err := tx.PrepareContext(ctx, e.insertSQL)
			if err != nil {
				return fmt.Errorf("PrepareContext(log):%w", err)
			}
			defer func() { _ = logStatement.Close() }()
		}

		var topologyStatement *sql.Stmt
		if e.cfg.EnableTopology {
			topologyStatement, err := tx.PrepareContext(ctx, e.insertTopologySQL)
			if err != nil {
				return fmt.Errorf("PrepareContext(topology):%w", err)
			}
			defer func() { _ = topologyStatement.Close() }()
		}

		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			logs := ld.ResourceLogs().At(i)
			res := logs.Resource()
			for j := 0; j < logs.ScopeLogs().Len(); j++ {
				scopeLogs := logs.ScopeLogs().At(j)
				for k := 0; k < scopeLogs.LogRecords().Len(); k++ {
					r := scopeLogs.LogRecords().At(k)

					if _, ok := r.Attributes().Get(stskafkaexporter.KafkaMessageKey); ok {
						if e.cfg.EnableTopology {
							if err := e.pushTopologyLogRecord(ctx, topologyStatement, r); err != nil {
								return err
							}
						}
					} else {
						if e.cfg.EnableLogs {
							if err := e.pushRegularLogRecord(ctx, logStatement, res, scopeLogs, r); err != nil {
								return err
							}
						}
					}
				}
			}
		}
		return nil
	})
	duration := time.Since(start)
	e.logger.Debug("insert logs", zap.Int("records", ld.LogRecordCount()),
		zap.String("cost", duration.String()))
	return err
}

func (e *LogsExporter) pushRegularLogRecord(ctx context.Context, statement *sql.Stmt, res pcommon.Resource, scopeLogs plog.ScopeLogs, r plog.LogRecord) error {
	var serviceName string
	if v, ok := res.Attributes().Get(string(conventions.ServiceNameKey)); ok {
		serviceName = v.Str()
	}
	resURL := scopeLogs.SchemaUrl()
	resAttr := attributesToMap(res.Attributes())
	scopeURL := scopeLogs.SchemaUrl()
	scopeName := scopeLogs.Scope().Name()
	scopeVersion := scopeLogs.Scope().Version()
	scopeAttr := attributesToMap(scopeLogs.Scope().Attributes())
	logAttr := attributesToMap(r.Attributes())

	_, err := statement.ExecContext(ctx,
		r.Timestamp().AsTime(),
		TraceIDToHexOrEmptyString(r.TraceID()),
		SpanIDToHexOrEmptyString(r.SpanID()),
		uint32(r.Flags()),
		r.SeverityText(),
		int32(r.SeverityNumber()),
		serviceName,
		r.Body().AsString(),
		resURL,
		resAttr,
		scopeURL,
		scopeName,
		scopeVersion,
		scopeAttr,
		logAttr,
	)
	if err != nil {
		return fmt.Errorf("ExecContext:%w", err)
	}
	return nil
}

func (e *LogsExporter) pushTopologyLogRecord(ctx context.Context, statement *sql.Stmt, r plog.LogRecord) error {
	body := r.Body().Bytes().AsRaw()
	if body == nil {
		return nil
	}

	var msg topostream.TopologyStreamMessage
	if err := proto.Unmarshal(body, &msg); err != nil {
		return fmt.Errorf("failed to unmarshal topology stream message: %w", err)
	}

	var components []*topostream.TopologyStreamComponent
	var relations []*topostream.TopologyStreamRelation

	switch payload := msg.Payload.(type) {
	case *topostream.TopologyStreamMessage_TopologyStreamSnapshotData:
		components = payload.TopologyStreamSnapshotData.GetComponents()
		relations = payload.TopologyStreamSnapshotData.GetRelations()
	case *topostream.TopologyStreamMessage_TopologyStreamRepeatElementsData:
		components = payload.TopologyStreamRepeatElementsData.GetComponents()
		relations = payload.TopologyStreamRepeatElementsData.GetRelations()
	}
	timestamp := time.UnixMilli(msg.GetCollectionTimestamp())

	for _, component := range components {
		resourceDefBytes, err := json.Marshal(component.GetResourceDefinition())
		if err != nil {
			return fmt.Errorf("ExecContext component:%w", err)
		}
		statusDataBytes, err := json.Marshal(component.GetStatusData())
		if err != nil {
			return fmt.Errorf("ExecContext component:%w", err)
		}
		_, err = statement.ExecContext(ctx,
			timestamp,
			component.GetExternalId(),
			"component",
			component.GetName(),
			component.GetTags(),
			component.GetTypeName(),
			component.GetTypeIdentifier(),
			component.GetLayerName(),
			component.GetLayerIdentifier(),
			component.GetDomainName(),
			component.GetDomainIdentifier(),
			component.GetIdentifiers(),
			string(resourceDefBytes),
			string(statusDataBytes),
			// Relation fields are nil
			nil,
			nil,
		)
		if err != nil {
			return fmt.Errorf("ExecContext component:%w", err)
		}
	}

	for _, relation := range relations {
		_, err := statement.ExecContext(ctx,
			timestamp,
			relation.GetExternalId(),
			"relation",
			relation.GetName(),
			relation.GetTags(),
			relation.GetTypeName(),
			relation.GetTypeIdentifier(),
			// Component fields are nil
			nil, nil, nil, nil, nil, nil, nil,
			// Relation fields
			relation.GetSourceIdentifier(),
			relation.GetTargetIdentifier(),
		)
		if err != nil {
			return fmt.Errorf("ExecContext relation:%w", err)
		}
	}
	return nil
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
