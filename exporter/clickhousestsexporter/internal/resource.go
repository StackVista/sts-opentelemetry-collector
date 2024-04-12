package internal

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"

	"go.uber.org/zap"
)

type resourceWriter struct {
	insertStatement *sql.Stmt
	logger          *zap.Logger
	context         context.Context
}

func NewResourceWriter(logger *zap.Logger, ctx context.Context, tx *sql.Tx, tableName string) (*resourceWriter, error) {
	resourceStatement, err := tx.PrepareContext(ctx, renderInsertResourcesSQL(tableName))

	if err != nil {
		return nil, err
	}

	return &resourceWriter{
		insertStatement: resourceStatement,
		logger:          logger,
		context:         ctx,
	}, nil
}

func (rw *resourceWriter) InsertResource(resourceRef [16]byte, serviceName string, resourceAttributes map[string]string) error {
	ref := big.Int{}
	ref.SetBytes(resourceRef[:])
	_, err := rw.insertStatement.ExecContext(rw.context,
		ref,
		serviceName,
		resourceAttributes,
	)
	return err
}

const (
	// language=ClickHouse SQL
	createResourcesTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
     Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
		 ResourceRef UInt128,
     ServiceName LowCardinality(String) CODEC(ZSTD(1)),
     ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
) ENGINE = ReplacingMergeTree
%s
ORDER BY (ResourceRef, toUnixTimestamp(Timestamp))
SETTINGS index_granularity=512, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	insertResourcesSQLTemplate = `INSERT INTO %s (
                        Timestamp,
												ResourceRef,
                        ServiceName,
                        ResourceAttributes,
                        ) VALUES (
																	now(),
                                  ?,
                                  ?,
                                  ?
                                  )`
)

func CreateResourcesTable(ctx context.Context, ttlDays uint, ttl time.Duration, tableName string, db *sql.DB) error {
	ttlExpr := GenerateTTLExpr(ttlDays, ttl, "Timestamp")
	if _, err := db.ExecContext(ctx, renderCreateResourcesTableSQL(ttlExpr, tableName)); err != nil {
		return fmt.Errorf("exec create resources table sql: %w", err)
	}
	return nil
}

func renderInsertResourcesSQL(tableName string) string {
	return fmt.Sprintf(strings.ReplaceAll(insertResourcesSQLTemplate, "'", "`"), tableName)
}

func renderCreateResourcesTableSQL(ttlExpr string, tableName string) string {
	return fmt.Sprintf(createResourcesTableSQL, tableName, ttlExpr)
}
