// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:lll
package clickhousestsexporter // import "github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter"

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
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
	client             *sql.DB
	insertSQL          string
	insertComponentSQL string
	insertRelationSQL  string

	logger *zap.Logger
	cfg    *Config
}

func NewLogsExporter(logger *zap.Logger, cfg *Config) (*LogsExporter, error) {
	client, err := newClickhouseClient(cfg)
	if err != nil {
		return nil, err
	}

	return &LogsExporter{
		client:             client,
		insertSQL:          renderInsertLogsSQL(cfg),
		insertComponentSQL: topology.RenderInsertComponentsSQL(cfg),
		insertRelationSQL:  topology.RenderInsertRelationsSQL(cfg),
		logger:             logger,
		cfg:                cfg,
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
		if err := topology.CreateComponentsTable(ctx, e.cfg, e.client); err != nil {
			return fmt.Errorf("failed to create components table: %w", err)
		}
		if err := topology.CreateRelationsTable(ctx, e.cfg, e.client); err != nil {
			return fmt.Errorf("failed to create relations table: %w", err)
		}
		if err := topology.CreateComponentsFirstSeenTable(ctx, e.cfg, e.client); err != nil {
			return fmt.Errorf("failed to create components time range table: %w", err)
		}
		if err := topology.CreateRelationsFirstSeenTable(ctx, e.cfg, e.client); err != nil {
			return fmt.Errorf("failed to create relations time range table: %w", err)
		}
		if err := topology.CreateComponentsFirstSeenMV(ctx, e.cfg, e.client); err != nil {
			return fmt.Errorf("failed to create components time range materialized view: %w", err)
		}
		if err := topology.CreateRelationsFirstSeenMV(ctx, e.cfg, e.client); err != nil {
			return fmt.Errorf("failed to create relations time range materialized view: %w", err)
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

type componentData struct {
	Timestamp        time.Time
	Component        *topostream.TopologyStreamComponent
	ExpiryIntervalMs int64
}

type relationData struct {
	Timestamp        time.Time
	Relation         *topostream.TopologyStreamRelation
	ExpiryIntervalMs int64
}

type logsData struct {
	resource  pcommon.Resource
	scopeLogs plog.ScopeLogs
	logRecord plog.LogRecord
}

func (e *LogsExporter) PushLogsData(ctx context.Context, ld plog.Logs) error {
	start := time.Now()

	var logsToInsert []logsData
	var componentsToInsert []componentData
	var relationsToInsert []relationData

	err := e.iterateLogsData(ld, func(res pcommon.Resource, scopeLogs plog.ScopeLogs, lr plog.LogRecord) error {
		if _, ok := lr.Attributes().Get(stskafkaexporter.KafkaMessageKey); ok {
			if e.cfg.EnableTopology {
				comps, rels, err := e.parseTopologyLogRecord(lr)
				if err != nil {
					return err
				}
				componentsToInsert = append(componentsToInsert, comps...)
				relationsToInsert = append(relationsToInsert, rels...)
			}
		} else if e.cfg.EnableLogs {
			logsData := logsData{
				resource:  res,
				scopeLogs: scopeLogs,
				logRecord: lr,
			}
			logsToInsert = append(logsToInsert, logsData)
		}
		return nil
	})
	if err != nil {
		e.logger.Error("Failed to iterate over logs data", zap.Error(err))
		return err
	}

	// Insert regular logs
	if e.cfg.EnableLogs && len(logsToInsert) > 0 {
		err := doWithTx(ctx, e.client, func(tx *sql.Tx) error {
			logStatement, err := tx.PrepareContext(ctx, e.insertSQL)
			if err != nil {
				return fmt.Errorf("PrepareContext(log):%w", err)
			}
			defer func() { _ = logStatement.Close() }()

			for _, logsData := range logsToInsert {
				if err := e.pushRegularLogRecord(ctx, logStatement, logsData.resource, logsData.scopeLogs, logsData.logRecord); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// Insert components
	if e.cfg.EnableTopology && len(componentsToInsert) > 0 {
		err := doWithTx(ctx, e.client, func(tx *sql.Tx) error {
			componentStatement, err := tx.PrepareContext(ctx, e.insertComponentSQL)
			if err != nil {
				return fmt.Errorf("PrepareContext(component):%w", err)
			}
			defer func() { _ = componentStatement.Close() }()

			for _, cd := range componentsToInsert {
				if err := e.pushComponentLogRecord(ctx, componentStatement, cd); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			e.logger.Error("Failed to store components", zap.Error(err))
			return err
		}
	}

	// Insert relations
	if e.cfg.EnableTopology && len(relationsToInsert) > 0 {
		err := doWithTx(ctx, e.client, func(tx *sql.Tx) error {
			relationStatement, err := tx.PrepareContext(ctx, e.insertRelationSQL)
			if err != nil {
				return fmt.Errorf("PrepareContext(relation):%w", err)
			}
			defer func() { _ = relationStatement.Close() }()

			for _, rd := range relationsToInsert {
				if err := e.pushRelationLogRecord(ctx, relationStatement, rd); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			e.logger.Error("Failed to store relations", zap.Error(err))
			return err
		}
	}

	duration := time.Since(start)
	e.logger.Debug("insert logs and topology", zap.Int("records", ld.LogRecordCount()),
		zap.String("cost", duration.String()))
	return nil
}

func (e *LogsExporter) iterateLogsData(ld plog.Logs, f func(pcommon.Resource, plog.ScopeLogs, plog.LogRecord) error) error {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		logs := ld.ResourceLogs().At(i)
		res := logs.Resource()
		for j := 0; j < logs.ScopeLogs().Len(); j++ {
			scopeLogs := logs.ScopeLogs().At(j)
			for k := 0; k < scopeLogs.LogRecords().Len(); k++ {
				lr := scopeLogs.LogRecords().At(k)
				err := f(res, scopeLogs, lr)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
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

func (e *LogsExporter) parseTopologyLogRecord(r plog.LogRecord) ([]componentData, []relationData, error) {
	body := r.Body().Bytes().AsRaw()
	if body == nil {
		return nil, nil, nil
	}

	var msg topostream.TopologyStreamMessage
	if err := proto.Unmarshal(body, &msg); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal topology stream message: %w", err)
	}

	var components []*topostream.TopologyStreamComponent
	var relations []*topostream.TopologyStreamRelation
	var expiryIntervalMs *int64

	switch payload := msg.Payload.(type) {
	// TODO: We now ignore the difference between snapshots and repeat elements data.
	case *topostream.TopologyStreamMessage_TopologyStreamSnapshotData:
		expiryIntervalMs = payload.TopologyStreamSnapshotData.ExpiryIntervalMs
		components = payload.TopologyStreamSnapshotData.GetComponents()
		relations = payload.TopologyStreamSnapshotData.GetRelations()
	case *topostream.TopologyStreamMessage_TopologyStreamRepeatElementsData:
		expiryIntervalMs = &payload.TopologyStreamRepeatElementsData.ExpiryIntervalMs
		components = payload.TopologyStreamRepeatElementsData.GetComponents()
		relations = payload.TopologyStreamRepeatElementsData.GetRelations()
	}
	timestamp := time.UnixMilli(msg.GetCollectionTimestamp())

	expiryMs := (int64)(60 * 1000 * 10) // default expiry for snapshots without one
	if expiryIntervalMs != nil {
		expiryMs = *expiryIntervalMs
	}

	compsData := make([]componentData, 0, len(components))
	for _, comp := range components {
		compsData = append(compsData, componentData{Timestamp: timestamp, Component: comp, ExpiryIntervalMs: expiryMs})
	}

	relsData := make([]relationData, 0, len(relations))
	for _, rel := range relations {
		relsData = append(relsData, relationData{Timestamp: timestamp, Relation: rel, ExpiryIntervalMs: expiryMs})
	}

	return compsData, relsData, nil
}

func (e *LogsExporter) pushComponentLogRecord(ctx context.Context, statement *sql.Stmt, c componentData) error {
	resourceDefBytes, err := json.Marshal(c.Component.GetResourceDefinition())
	if err != nil {
		return fmt.Errorf("ExecContext component: marshal resource definition:%w", err)
	}
	statusDataBytes, err := json.Marshal(c.Component.GetStatusData())
	if err != nil {
		return fmt.Errorf("ExecContext component: marshal status data:%w", err)
	}

	sortedLabels := c.Component.GetTags()
	slices.Sort(sortedLabels)

	expiresAt := c.Timestamp.Add(time.Duration(c.ExpiryIntervalMs) * time.Millisecond)
	_, err = statement.ExecContext(ctx,
		c.Timestamp,
		// TODO: This uses externalId which is the main identifier
		// for open telemetry data, but this doesn't have to be like that
		c.Component.GetExternalId(),
		c.Component.GetName(),
		sortedLabels,
		topoTagsToMap(c.Component.GetTags()),
		c.Component.GetTypeName(),
		c.Component.GetTypeIdentifier(),
		c.Component.GetLayerName(),
		c.Component.GetLayerIdentifier(),
		c.Component.GetDomainName(),
		c.Component.GetDomainIdentifier(),
		c.Component.GetIdentifiers(),
		string(resourceDefBytes),
		string(statusDataBytes),
		expiresAt,
	)
	if err != nil {
		return fmt.Errorf("ExecContext component:%w", err)
	}
	return nil
}

func (e *LogsExporter) pushRelationLogRecord(ctx context.Context, statement *sql.Stmt, r relationData) error {
	expiresAt := r.Timestamp.Add(time.Duration(r.ExpiryIntervalMs) * time.Millisecond)

	sortedLabels := r.Relation.GetTags()
	slices.Sort(sortedLabels)

	_, err := statement.ExecContext(ctx,
		r.Timestamp,
		r.Relation.GetName(),
		sortedLabels,
		topoTagsToMap(r.Relation.GetTags()),
		r.Relation.GetTypeName(),
		r.Relation.GetTypeIdentifier(),
		r.Relation.GetSourceIdentifier(),
		r.Relation.GetTargetIdentifier(),
		expiresAt,
	)
	if err != nil {
		return fmt.Errorf("ExecContext relation:%w", err)
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

func topoTagsToMap(tags []string) map[string]string {
	tagMap := make(map[string]string)

	for _, tag := range tags {
		trimmedTag := strings.TrimSpace(tag)
		if trimmedTag == "" {
			continue // Skip empty strings
		}

		parts := strings.SplitN(trimmedTag, ":", 2)

		key := strings.TrimSpace(parts[0])
		value := ""

		if len(parts) > 1 {
			value = strings.TrimSpace(parts[1])
		}
		tagMap[key] = value
	}

	return tagMap
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
