// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package topology

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter/internal"
)

const (
	// language=ClickHouse SQL
	createComponentsTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
    Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    Identifier String CODEC(ZSTD(1)),
    Hash UInt64 DEFAULT sipHash64( Identifier, Name, Tags, TypeName, TypeIdentifier, LayerName, LayerIdentifier, DomainName, DomainIdentifier, ComponentIdentifiers, ResourceDefinition, StatusData ) CODEC(ZSTD(1)),
    Name String CODEC(ZSTD(1)),
    Tags Array(String) CODEC(ZSTD(1)),
    TypeName LowCardinality(String) CODEC(ZSTD(1)),
    TypeIdentifier String CODEC(ZSTD(1)),
    LayerName LowCardinality(String) CODEC(ZSTD(1)),
    LayerIdentifier String CODEC(ZSTD(1)),
    DomainName LowCardinality(String) CODEC(ZSTD(1)),
    DomainIdentifier String CODEC(ZSTD(1)),
    ComponentIdentifiers Array(String) CODEC(ZSTD(1)),
    ResourceDefinition String CODEC(ZSTD(1)), -- JSON
    StatusData String CODEC(ZSTD(1))         -- JSON
) ENGINE = ReplacingMergeTree()
%s
PARTITION BY toDate(Timestamp)
ORDER BY (Identifier, Hash)
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`
// language=ClickHouse SQL
	createRelationsTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
    Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    Identifier String CODEC(ZSTD(1)),
    Hash UInt64 DEFAULT sipHash64(
			Identifier, Name, Tags, TypeName, TypeIdentifier, SourceIdentifier, TargetIdentifier
		) CODEC(ZSTD(1)),
    Name String CODEC(ZSTD(1)),
    Tags Array(String) CODEC(ZSTD(1)),
    TypeName LowCardinality(String) CODEC(ZSTD(1)),
    TypeIdentifier String CODEC(ZSTD(1)),
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
	createComponentsFieldValuesMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s
AS
SELECT Identifier, Hash, 'type_name' as FieldName, TypeName as FieldValue, Timestamp FROM %s WHERE TypeName != ''
UNION ALL
SELECT Identifier, Hash, 'layer_name' as FieldName, LayerName as FieldValue, Timestamp FROM %s WHERE LayerName != ''
UNION ALL
SELECT Identifier, Hash, 'domain_name' as FieldName, DomainName as FieldValue, Timestamp FROM %s WHERE DomainName != ''
UNION ALL
SELECT Identifier, Hash, 'tag' as FieldName, arrayJoin(Tags) as FieldValue, Timestamp FROM %s WHERE length(Tags) > 0;
`
	// language=ClickHouse SQL
	createRelationsFieldValuesMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s
AS
SELECT Identifier, Hash, 'type_name' as FieldName, TypeName as FieldValue, Timestamp FROM %s WHERE TypeName != ''
UNION ALL
SELECT Identifier, Hash, 'tag' as FieldName, arrayJoin(Tags) as FieldValue, Timestamp FROM %s WHERE length(Tags) > 0;
`
	// language=ClickHouse SQL
	insertComponentsSQLTemplate = `INSERT INTO %s (
    Timestamp, Identifier, Name, Tags, TypeName, TypeIdentifier,
    LayerName, LayerIdentifier, DomainName, DomainIdentifier, ComponentIdentifiers,
    ResourceDefinition, StatusData
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)`
	// language=ClickHouse SQL
	insertRelationsSQLTemplate = `INSERT INTO %s (
    Timestamp, Identifier, Name, Tags, TypeName, TypeIdentifier, SourceIdentifier, TargetIdentifier
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?
)`
)

type Config interface {
	GetTTLDays() uint

	GetTTL() time.Duration

	GetComponentsTableName() string

	GetRelationsTableName() string

	GetComponentsTimeRangeTableName() string

	GetRelationsTimeRangeTableName() string

	GetComponentsFieldValuesTableName() string

	GetRelationsFieldValuesTableName() string

	GetComponentsTimeRangeMVName() string

	GetRelationsTimeRangeMVName() string

	GetComponentsFieldValuesMVName() string

	GetRelationsFieldValuesMVName() string
}

func CreateComponentsTable(ctx context.Context, cfg Config, db *sql.DB) error {

	if _, err := db.ExecContext(ctx, renderCreateComponentsTableSQL(cfg)); err != nil {

		return fmt.Errorf("exec create components table sql: %w", err)

	}
	return nil
}

func CreateRelationsTable(ctx context.Context, cfg Config, db *sql.DB) error {

	if _, err := db.ExecContext(ctx, renderCreateRelationsTableSQL(cfg)); err != nil {

		return fmt.Errorf("exec create relations table sql: %w", err)

	}
	return nil
}

func CreateComponentsTimeRangeTable(ctx context.Context, cfg Config, db *sql.DB) error {

	if _, err := db.ExecContext(ctx, renderCreateComponentsTimeRangeTableSQL(cfg)); err != nil {

		return fmt.Errorf("exec create components time range table sql: %w", err)

	}
	return nil
}

func CreateRelationsTimeRangeTable(ctx context.Context, cfg Config, db *sql.DB) error {

	if _, err := db.ExecContext(ctx, renderCreateRelationsTimeRangeTableSQL(cfg)); err != nil {

		return fmt.Errorf("exec create relations time range table sql: %w", err)

	}
	return nil
}

func CreateComponentsTimeRangeMV(ctx context.Context, cfg Config, db *sql.DB) error {

	if _, err := db.ExecContext(ctx, renderCreateComponentsTimeRangeMV(cfg)); err != nil {

		return fmt.Errorf("exec create components time range mv sql: %w", err)

	}
	return nil
}

func CreateRelationsTimeRangeMV(ctx context.Context, cfg Config, db *sql.DB) error {

	if _, err := db.ExecContext(ctx, renderCreateRelationsTimeRangeMV(cfg)); err != nil {

		return fmt.Errorf("exec create relations time range mv sql: %w", err)

	}
	return nil
}

func CreateComponentsFieldValuesTable(ctx context.Context, cfg Config, db *sql.DB) error {

	if _, err := db.ExecContext(ctx, renderCreateComponentsFieldValuesTableSQL(cfg)); err != nil {

		return fmt.Errorf("exec create components field values table sql: %w", err)

	}
	return nil
}

func CreateRelationsFieldValuesTable(ctx context.Context, cfg Config, db *sql.DB) error {

	if _, err := db.ExecContext(ctx, renderCreateRelationsFieldValuesTableSQL(cfg)); err != nil {

		return fmt.Errorf("exec create relations field values table sql: %w", err)

	}
	return nil
}

func CreateComponentsFieldValuesMV(ctx context.Context, cfg Config, db *sql.DB) error {

	if _, err := db.ExecContext(ctx, renderCreateComponentsFieldValuesMV(cfg)); err != nil {

		return fmt.Errorf("exec create components field values mv sql: %w", err)

	}
	return nil
}

func CreateRelationsFieldValuesMV(ctx context.Context, cfg Config, db *sql.DB) error {

	if _, err := db.ExecContext(ctx, renderCreateRelationsFieldValuesMV(cfg)); err != nil {

		return fmt.Errorf("exec create relations field values mv sql: %w", err)

	}
	return nil
}

func renderCreateComponentsTableSQL(cfg Config) string {

	ttlExpr := internal.GenerateTTLExpr(cfg.GetTTLDays(), cfg.GetTTL(), "Timestamp")
	return fmt.Sprintf(createComponentsTableSQL, cfg.GetComponentsTableName(), ttlExpr)
}

func renderCreateRelationsTableSQL(cfg Config) string {

	ttlExpr := internal.GenerateTTLExpr(cfg.GetTTLDays(), cfg.GetTTL(), "Timestamp")
	return fmt.Sprintf(createRelationsTableSQL, cfg.GetRelationsTableName(), ttlExpr)
}

func renderCreateComponentsTimeRangeTableSQL(cfg Config) string {
	return fmt.Sprintf(createTopologyTimeRangeTableSQL, cfg.GetComponentsTimeRangeTableName())
}

func renderCreateRelationsTimeRangeTableSQL(cfg Config) string {
	return fmt.Sprintf(createTopologyTimeRangeTableSQL, cfg.GetRelationsTimeRangeTableName())
}

func renderCreateComponentsTimeRangeMV(cfg Config) string {
	return fmt.Sprintf(createTopologyTimeRangeMV, cfg.GetComponentsTimeRangeMVName(),
		cfg.GetComponentsTimeRangeTableName(), cfg.GetComponentsTableName())
}

func renderCreateRelationsTimeRangeMV(cfg Config) string {
	return fmt.Sprintf(createTopologyTimeRangeMV, cfg.GetRelationsTimeRangeMVName(),
		cfg.GetRelationsTimeRangeTableName(), cfg.GetRelationsTableName())
}

func renderCreateComponentsFieldValuesTableSQL(cfg Config) string {
	return fmt.Sprintf(createTopologyFieldValuesTableSQL, cfg.GetComponentsFieldValuesTableName())
}

func renderCreateRelationsFieldValuesTableSQL(cfg Config) string {
	return fmt.Sprintf(createTopologyFieldValuesTableSQL, cfg.GetRelationsFieldValuesTableName())
}

func renderCreateComponentsFieldValuesMV(cfg Config) string {
	return fmt.Sprintf(createComponentsFieldValuesMV, cfg.GetComponentsFieldValuesMVName(),
		cfg.GetComponentsFieldValuesTableName(), cfg.GetComponentsTableName(), cfg.GetComponentsTableName(),
		cfg.GetComponentsTableName(), cfg.GetComponentsTableName())
}

func renderCreateRelationsFieldValuesMV(cfg Config) string {
	return fmt.Sprintf(createRelationsFieldValuesMV, cfg.GetRelationsFieldValuesMVName(),
		cfg.GetRelationsFieldValuesTableName(), cfg.GetRelationsTableName(), cfg.GetRelationsTableName())
}

func RenderInsertComponentsSQL(cfg Config) string {
	return fmt.Sprintf(insertComponentsSQLTemplate, cfg.GetComponentsTableName())
}

func RenderInsertRelationsSQL(cfg Config) string {
	return fmt.Sprintf(insertRelationsSQLTemplate, cfg.GetRelationsTableName())
}
