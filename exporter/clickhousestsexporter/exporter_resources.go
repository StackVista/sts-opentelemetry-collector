package clickhousestsexporter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter/internal"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	conventions "go.opentelemetry.io/collector/semconv/v1.18.0"
	"go.uber.org/zap"
)

type resourcesExporter struct {
	client    *sql.DB
	insertSQL string

	logger *zap.Logger
	cfg    *Config
}

type resourceModel struct {
	resourceRef uuid.UUID
	serviceName string
	attributes  map[string]string
}

func newResourceModel(resource pcommon.Resource) (*resourceModel, error) {
	var serviceName string
	if v, ok := resource.Attributes().Get(conventions.AttributeServiceName); ok {
		serviceName = v.Str()
	}
	resourceRef := pdatautil.MapHash(resource.Attributes())
	refUUID, err := uuid.FromBytes(resourceRef[:])
	if err != nil {
		return nil, err
	}

	resAttr := attributesToMap(resource.Attributes())
	return &resourceModel{
		resourceRef: refUUID,
		serviceName: serviceName,
		attributes:  resAttr,
	}, nil
}

func newResourceExporter(logger *zap.Logger, cfg *Config) (*resourcesExporter, error) {
	client, err := newClickhouseClient(cfg)
	if err != nil {
		return nil, err
	}

	return &resourcesExporter{
		client: client,
		logger: logger,

		insertSQL: renderInsertResourcesSQL(cfg.ResourcesTableName),
		cfg:       cfg,
	}, nil
}

// shutdown will shut down the exporter.
func (e *resourcesExporter) shutdown(_ context.Context) error {
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (e *resourcesExporter) start(ctx context.Context, _ component.Host) error {
	if !e.cfg.CreateResourcesTable {
		return nil
	}

	if err := createDatabase(ctx, e.cfg); err != nil {
		return err
	}

	return createResourcesTable(ctx, e.cfg.TTLDays, e.cfg.TTL, e.cfg.ResourcesTableName, e.client)
}

func (e *resourcesExporter) InsertResources(ctx context.Context, resources []*resourceModel) error {
	start := time.Now()

	err := doWithTx(ctx, e.client, func(tx *sql.Tx) error {

		resourceStatement, err := tx.PrepareContext(ctx, e.insertSQL)
		if err != nil {
			return fmt.Errorf("PrepareContext Traces:%w", err)
		}
		defer func() {
			_ = resourceStatement.Close()
		}()

		for _, resource := range resources {
			_, err := resourceStatement.ExecContext(ctx,
				time.Now(),
				resource.resourceRef,
				resource.serviceName,
				resource.attributes,
			)
			if err != nil {
				return err
			}
		}
		return nil
	})
	duration := time.Since(start)
	e.logger.Debug("insert resources", zap.Int("records", len(resources)),
		zap.String("cost", duration.String()))

	return err
}

const (
	// language=ClickHouse SQL
	createResourcesTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
     Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
		 ResourceRef UUID,
     ServiceName LowCardinality(String) CODEC(ZSTD(1)),
     ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
) ENGINE = ReplacingMergeTree
%s
ORDER BY (ResourceRef, toUnixTimestamp(Timestamp))
SETTINGS index_granularity=512, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	insertResourcesSQLTemplate = `INSERT INTO %s (Timestamp, ResourceRef, ServiceName, ResourceAttributes) VALUES (?, ?, ?, ?)`
)

func createResourcesTable(ctx context.Context, ttlDays uint, ttl time.Duration, tableName string, db *sql.DB) error {
	ttlExpr := internal.GenerateTTLExpr(ttlDays, ttl, "Timestamp")
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
