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
    LastSeen DateTime64(6) CODEC(Delta, ZSTD(1)),
		LastSeenHour DateTime DEFAULT toStartOfHour(LastSeen) CODEC(ZSTD(1)),
    Identifier String CODEC(ZSTD(1)),
    Hash UInt64 DEFAULT cityHash64(
			Identifier, Name, mapSort(Tags), TypeName, TypeIdentifier, LayerName, LayerIdentifier, 
			DomainName, DomainIdentifier, Identifiers, ResourceDefinition, StatusData ) CODEC(ZSTD(1)
		),
    Name String CODEC(ZSTD(1)),
    Tags Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    TypeName LowCardinality(String) CODEC(ZSTD(1)),
    TypeIdentifier String CODEC(ZSTD(1)),
    LayerName LowCardinality(String) CODEC(ZSTD(1)),
    LayerIdentifier String CODEC(ZSTD(1)),
    DomainName LowCardinality(String) CODEC(ZSTD(1)),
    DomainIdentifier String CODEC(ZSTD(1)),
    Identifiers Array(String) CODEC(ZSTD(1)),
    ResourceDefinition String CODEC(ZSTD(1)), -- JSON
    StatusData String CODEC(ZSTD(1)),         -- JSON
		ExpiresAt DateTime64(6) CODEC(Delta, ZSTD(1)),
		INDEX idx_name Name TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx_tags_key mapKeys(Tags) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_tags_value mapValues(Tags) TYPE bloom_filter(0.01) GRANULARITY 1,
		INDEX idx_type_name TypeName TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx_layer_name LayerName TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx__domain_name DomainName TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx_identifiers Identifiers TYPE bloom_filter(0.001) GRANULARITY 1,
  ) ENGINE = ReplacingMergeTree()
%s
PARTITION BY toDate(LastSeen)
ORDER BY (Identifier, Hash, LastSeenHour)
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	createRelationsTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
    LastSeen DateTime64(6) CODEC(Delta, ZSTD(1)),
		LastSeenHour DateTime DEFAULT toStartOfHour(LastSeen) CODEC(ZSTD(1)),
    Identifier String CODEC(ZSTD(1)),
    Hash UInt64 DEFAULT cityHash64(
			Identifier, Name, mapSort(Tags), TypeName, TypeIdentifier, SourceIdentifier, TargetIdentifier
		) CODEC(ZSTD(1)),
    Name String CODEC(ZSTD(1)),
    Tags Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    TypeName LowCardinality(String) CODEC(ZSTD(1)),
    TypeIdentifier String CODEC(ZSTD(1)),
    SourceIdentifier String CODEC(ZSTD(1)),
    TargetIdentifier String CODEC(ZSTD(1)),
		ExpiresAt DateTime64(6) CODEC(Delta, ZSTD(1)),
		INDEX idx_name Name TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx_tags_key mapKeys(Tags) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_tags_value mapValues(Tags) TYPE bloom_filter(0.01) GRANULARITY 1,
		INDEX idx_type_name TypeName TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx_source_identifier SourceIdentifier TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx_target_identifier TargetIdentifier TYPE bloom_filter(0.001) GRANULARITY 1
	) ENGINE = ReplacingMergeTree()
%s
PARTITION BY toDate(LastSeen)
ORDER BY (Identifier, Hash, LastSeenHour)
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`

	// language=ClickHouse SQL
	createTopologyTimeRangeTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
    Identifier String,
    Hash UInt64,
    minTimestamp AggregateFunction(min, DateTime64(6)),
    maxTimestamp AggregateFunction(max, DateTime64(6))
) ENGINE = AggregatingMergeTree()
ORDER BY (Identifier, Hash);
`

	// language=ClickHouse SQL
	createTopologyTimeRangeMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s
AS SELECT
    Identifier,
    Hash,
    minState(LastSeen) as minTimestamp,
    maxState(ExpiresAt) as maxTimestamp
FROM %s
GROUP BY Identifier, Hash;
`

	// language=ClickHouse SQL
	insertComponentsSQLTemplate = `INSERT INTO %s (
    LastSeen, Identifier, Name, Tags, TypeName, TypeIdentifier,
    LayerName, LayerIdentifier, DomainName, DomainIdentifier, Identifiers,
    ResourceDefinition, StatusData, ExpiresAt
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)`
	// language=ClickHouse SQL
	insertRelationsSQLTemplate = `INSERT INTO %s (
    LastSeen, Identifier, Name, Tags, TypeName, TypeIdentifier, SourceIdentifier, TargetIdentifier, ExpiresAt
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?
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

func renderCreateComponentsTableSQL(cfg Config) string {

	ttlExpr := internal.GenerateTTLExpr(cfg.GetTTLDays(), cfg.GetTTL(), "LastSeen")
	return fmt.Sprintf(createComponentsTableSQL, cfg.GetComponentsTableName(), ttlExpr)
}

func renderCreateRelationsTableSQL(cfg Config) string {

	ttlExpr := internal.GenerateTTLExpr(cfg.GetTTLDays(), cfg.GetTTL(), "LastSeen")
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

func RenderInsertComponentsSQL(cfg Config) string {
	return fmt.Sprintf(insertComponentsSQLTemplate, cfg.GetComponentsTableName())
}

func RenderInsertRelationsSQL(cfg Config) string {
	return fmt.Sprintf(insertRelationsSQLTemplate, cfg.GetRelationsTableName())
}
