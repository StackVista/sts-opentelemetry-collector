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
    LastSeen DateTime CODEC(Delta, ZSTD(1)),
		LastSeenHour DateTime DEFAULT toStartOfHour(LastSeen) CODEC(ZSTD(1)),
    Identifier String CODEC(ZSTD(1)),
    Hash UInt64 DEFAULT cityHash64(
			Identifier, Name, Labels, TypeName, TypeIdentifier, LayerName, LayerIdentifier, 
			DomainName, DomainIdentifier, Identifiers, ResourceDefinition, StatusData ) CODEC(ZSTD(1)
		),
    Name String CODEC(ZSTD(1)),
		Labels Array(String) CODEC(ZSTD(1)),
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
		ExpiresAt DateTime CODEC(Delta, ZSTD(1)),
		INDEX idx_name Name TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx_tags_key mapKeys(Tags) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_tags_value mapValues(Tags) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_labels Labels TYPE bloom_filter(0.01) GRANULARITY 1,
		INDEX idx_type_name TypeName TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx_layer_name LayerName TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx__domain_name DomainName TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx_identifiers Identifiers TYPE bloom_filter(0.001) GRANULARITY 1,
  ) ENGINE = ReplacingMergeTree()
%s
PARTITION BY toDate(LastSeen)
ORDER BY (TypeName, Identifier, toUnixTimestamp(LastSeenHour), Hash)
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	createRelationsTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
    LastSeen DateTime CODEC(Delta, ZSTD(1)),
		LastSeenHour DateTime DEFAULT toStartOfHour(LastSeen) CODEC(ZSTD(1)),
    Hash UInt64 DEFAULT cityHash64(
			Name, Labels, TypeName, TypeIdentifier, SourceIdentifier, TargetIdentifier
		) CODEC(ZSTD(1)),
    Name String CODEC(ZSTD(1)),
		Labels Array(String) CODEC(ZSTD(1)),
    Tags Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    TypeName LowCardinality(String) CODEC(ZSTD(1)),
    TypeIdentifier String CODEC(ZSTD(1)),
    SourceIdentifier String CODEC(ZSTD(1)),
    TargetIdentifier String CODEC(ZSTD(1)),
		ExpiresAt DateTime CODEC(Delta, ZSTD(1)),
		INDEX idx_name Name TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx_tags_key mapKeys(Tags) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_tags_value mapValues(Tags) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_labels Labels TYPE bloom_filter(0.01) GRANULARITY 1,
		INDEX idx_type_name TypeName TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx_source_identifier SourceIdentifier TYPE bloom_filter(0.001) GRANULARITY 1,
		INDEX idx_target_identifier TargetIdentifier TYPE bloom_filter(0.001) GRANULARITY 1
	) ENGINE = ReplacingMergeTree()
%s
PARTITION BY toDate(LastSeen)
ORDER BY (SourceIdentifier, TargetIdentifier, LastSeenHour, Hash)
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`

	// language=ClickHouse SQL
	createComponentFirstSeenTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
	Identifier String,
	Hash UInt64,
	FirstSeen DateTime,
	Version Int32
) ENGINE = ReplacingMergeTree(Version)
ORDER BY (Identifier, Hash) SETTINGS index_granularity = 8192;;
`

	// language=ClickHouse SQL
	createRelationsFirstSeenTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
  SourceIdentifier String,
  TargetIdentifier String,
  Hash UInt64,
  FirstSeen DateTime,
  Version Int32,
) ENGINE = ReplacingMergeTree(Version)
ORDER BY (SourceIdentifier, TargetIdentifier, Hash) SETTINGS index_granularity = 8192;
`

	// language=ClickHouse SQL
	createComponentFirstSeenMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s
AS SELECT Identifier,
  Hash,
  LastSeen as FirstSeen,
  - toUnixTimestamp(LastSeen) as Version
FROM %s;
`

	// language=ClickHouse SQL
	createRelationsFirstSeenMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s
AS SELECT
	SourceIdentifier,
  TargetIdentifier,
  Hash,
  LastSeen as FirstSeen,
  - toUnixTimestamp(LastSeen) as Version
FROM %s;
`

	// language=ClickHouse SQL
	insertComponentsSQLTemplate = `INSERT INTO %s (
    LastSeen, Identifier, Name, Labels, Tags, TypeName, TypeIdentifier,
    LayerName, LayerIdentifier, DomainName, DomainIdentifier, Identifiers,
    ResourceDefinition, StatusData, ExpiresAt
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)`
	// language=ClickHouse SQL
	insertRelationsSQLTemplate = `INSERT INTO %s (
    LastSeen, Name, Labels, Tags, TypeName, TypeIdentifier, SourceIdentifier, TargetIdentifier, ExpiresAt
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?
)`
)

type Config interface {
	GetTTLDays() uint

	GetTTL() time.Duration

	GetComponentsTableName() string

	GetRelationsTableName() string

	GetComponentsFirstSeenTableName() string

	GetRelationsFirstSeenTableName() string

	GetComponentsFieldValuesTableName() string

	GetRelationsFieldValuesTableName() string

	GetComponentsFirstSeenMVName() string

	GetRelationsFirstSeenMVName() string

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

func CreateComponentsFirstSeenTable(ctx context.Context, cfg Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateComponentsFirstSeenTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create components time range table sql: %w", err)
	}
	return nil
}

func CreateRelationsFirstSeenTable(ctx context.Context, cfg Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateRelationsFirstSeenTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create relations time range table sql: %w", err)
	}
	return nil
}

func CreateComponentsFirstSeenMV(ctx context.Context, cfg Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateComponentsFirstSeenMV(cfg)); err != nil {
		return fmt.Errorf("exec create components time range mv sql: %w", err)
	}
	return nil
}

func CreateRelationsFirstSeenMV(ctx context.Context, cfg Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateRelationsFirstSeenMV(cfg)); err != nil {
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

func renderCreateComponentsFirstSeenTableSQL(cfg Config) string {
	return fmt.Sprintf(createComponentFirstSeenTableSQL, cfg.GetComponentsFirstSeenTableName())
}

func renderCreateRelationsFirstSeenTableSQL(cfg Config) string {
	return fmt.Sprintf(createRelationsFirstSeenTableSQL, cfg.GetRelationsFirstSeenTableName())
}

func renderCreateComponentsFirstSeenMV(cfg Config) string {
	return fmt.Sprintf(createComponentFirstSeenMV, cfg.GetComponentsFirstSeenMVName(),
		cfg.GetComponentsFirstSeenTableName(), cfg.GetComponentsTableName())
}

func renderCreateRelationsFirstSeenMV(cfg Config) string {
	return fmt.Sprintf(createRelationsFirstSeenMV, cfg.GetRelationsFirstSeenMVName(),
		cfg.GetRelationsFirstSeenTableName(), cfg.GetRelationsTableName())
}

func RenderInsertComponentsSQL(cfg Config) string {
	return fmt.Sprintf(insertComponentsSQLTemplate, cfg.GetComponentsTableName())
}

func RenderInsertRelationsSQL(cfg Config) string {
	return fmt.Sprintf(insertRelationsSQLTemplate, cfg.GetRelationsTableName())
}
