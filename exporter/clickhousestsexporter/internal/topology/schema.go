// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package topology

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter/internal"
)

const (
	// language=ClickHouse SQL
	createTopologyTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
    Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    Identifier String CODEC(ZSTD(1)),
    Hash UInt64 CODEC(ZSTD(1)),
    Type LowCardinality(String) CODEC(ZSTD(1)), -- 'component' or 'relation'

    -- Common fields
    Name String CODEC(ZSTD(1)),
    Tags Array(String) CODEC(ZSTD(1)),
    TypeName LowCardinality(String) CODEC(ZSTD(1)),
    TypeIdentifier String CODEC(ZSTD(1)),

    -- Component fields
    LayerName LowCardinality(String) CODEC(ZSTD(1)),
    LayerIdentifier String CODEC(ZSTD(1)),
    DomainName LowCardinality(String) CODEC(ZSTD(1)),
    DomainIdentifier String CODEC(ZSTD(1)),
    ComponentIdentifiers Array(String) CODEC(ZSTD(1)),
    ResourceDefinition String CODEC(ZSTD(1)), -- JSON
    StatusData String CODEC(ZSTD(1)),         -- JSON

    -- Relation fields
    SourceIdentifier String CODEC(ZSTD(1)),
    TargetIdentifier String CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree()
%s
PARTITION BY toDate(Timestamp)
ORDER BY (Identifier, Hash)
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`

	// language=ClickHouse SQL
	createTopologyTimeRangeTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
    Identifier String,
    Hash UInt64,
    minTimestamp AggregateFunction(min, DateTime64(9)),
    maxTimestamp AggregateFunction(max, DateTime64(9))
) ENGINE = AggregatingMergeTree()
ORDER BY (Identifier, Hash);
`

	// language=ClickHouse SQL
	createTopologyTimeRangeMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s
AS SELECT
    Identifier,
    Hash,
    minState(Timestamp) as minTimestamp,
    maxState(Timestamp) as maxTimestamp
FROM %s
GROUP BY Identifier, Hash;
`

	// language=ClickHouse SQL
	createTopologyFieldValuesTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
    Identifier String,
    Hash UInt64,
    FieldName LowCardinality(String),
    FieldValue String,
    Timestamp DateTime64(9)
) ENGINE = ReplacingMergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (Identifier, Hash, FieldName, FieldValue);
`

	// language=ClickHouse SQL
	createTopologyFieldValuesMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s
AS
-- Component Fields
SELECT Identifier, Hash, 'type_name' as FieldName, TypeName as FieldValue, Timestamp FROM %s WHERE Type = 'component' AND TypeName != ''
UNION ALL
SELECT Identifier, Hash, 'layer_name' as FieldName, LayerName as FieldValue, Timestamp FROM %s WHERE Type = 'component' AND LayerName != ''
UNION ALL
SELECT Identifier, Hash, 'domain_name' as FieldName, DomainName as FieldValue, Timestamp FROM %s WHERE Type = 'component' AND DomainName != ''
UNION ALL
SELECT Identifier, Hash, 'tag' as FieldName, arrayJoin(Tags) as FieldValue, Timestamp FROM %s WHERE Type = 'component' AND length(Tags) > 0
UNION ALL
-- Relation Fields
SELECT Identifier, Hash, 'type_name' as FieldName, TypeName as FieldValue, Timestamp FROM %s WHERE Type = 'relation' AND TypeName != ''
UNION ALL
SELECT Identifier, Hash, 'tag' as FieldName, arrayJoin(Tags) as FieldValue, Timestamp FROM %s WHERE Type = 'relation' AND length(Tags) > 0;
`

	// language=ClickHouse SQL
	insertTopologySQLTemplate = `INSERT INTO %s (
    Timestamp,
    Identifier,
    Type,
    Name,
    Tags,
    TypeName,
    TypeIdentifier,
    -- Component
    LayerName,
    LayerIdentifier,
    DomainName,
    DomainIdentifier,
    ComponentIdentifiers,
    ResourceDefinition,
    StatusData,
    -- Relation
    SourceIdentifier,
    TargetIdentifier,
    -- Hash
    Hash
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?,
    sipHash64(
        (
            Identifier, Type, Name, Tags, TypeName, TypeIdentifier,
            LayerName, LayerIdentifier, DomainName, DomainIdentifier, ComponentIdentifiers, ResourceDefinition, StatusData,
            SourceIdentifier, TargetIdentifier
        )
    )
)`
)

type Config interface {
	GetTTLDays() uint
	GetTTL() string
	GetTopologyTableName() string
	GetTopologyTimeRangeTableName() string
	GetTopologyFieldValuesTableName() string
	GetTopologyTimeRangeMVName() string
	GetTopologyFieldValuesMVName() string
}

func CreateTopologyTable(ctx context.Context, cfg Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateTopologyTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create topology table sql: %w", err)
	}
	return nil
}

func CreateTopologyTimeRangeTable(ctx context.Context, cfg Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateTopologyTimeRangeTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create topology time range table sql: %w", err)
	}
	return nil
}

func CreateTopologyTimeRangeMV(ctx context.Context, cfg Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateTopologyTimeRangeMV(cfg)); err != nil {
		return fmt.Errorf("exec create topology time range mv sql: %w", err)
	}
	return nil
}

func CreateTopologyFieldValuesTable(ctx context.Context, cfg Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateTopologyFieldValuesTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create topology field values table sql: %w", err)
	}
	return nil
}

func CreateTopologyFieldValuesMV(ctx context.Context, cfg Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateTopologyFieldValuesMV(cfg)); err != nil {
		return fmt.Errorf("exec create topology field values mv sql: %w", err)
	}
	return nil
}

func renderCreateTopologyTableSQL(cfg Config) string {
	ttlExpr := internal.GenerateTTLExpr(cfg.GetTTLDays(), cfg.GetTTL(), "Timestamp")
	return fmt.Sprintf(createTopologyTableSQL, cfg.GetTopologyTableName(), ttlExpr)
}

func renderCreateTopologyTimeRangeTableSQL(cfg Config) string {
	return fmt.Sprintf(createTopologyTimeRangeTableSQL, cfg.GetTopologyTimeRangeTableName())
}

func renderCreateTopologyTimeRangeMV(cfg Config) string {
	return fmt.Sprintf(createTopologyTimeRangeMV, cfg.GetTopologyTimeRangeMVName(), cfg.GetTopologyTimeRangeTableName(), cfg.GetTopologyTableName())
}

func renderCreateTopologyFieldValuesTableSQL(cfg Config) string {
	return fmt.Sprintf(createTopologyFieldValuesTableSQL, cfg.GetTopologyFieldValuesTableName())
}

func renderCreateTopologyFieldValuesMV(cfg Config) string {
	return fmt.Sprintf(createTopologyFieldValuesMV, cfg.GetTopologyFieldValuesMVName(), cfg.GetTopologyFieldValuesTableName(), cfg.GetTopologyTableName(), cfg.GetTopologyTableName(), cfg.GetTopologyTableName(), cfg.GetTopologyTableName(), cfg.GetTopologyTableName(), cfg.GetTopologyTableName())
}

func RenderInsertTopologySQL(cfg Config) string {
	return fmt.Sprintf(insertTopologySQLTemplate, cfg.GetTopologyTableName())
}
