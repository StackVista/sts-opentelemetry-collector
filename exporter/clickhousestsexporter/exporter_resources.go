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
	"go.uber.org/zap"
)

type ResourcesExporter struct {
	client    *sql.DB
	insertSQL string

	logger *zap.Logger
	cfg    *Config
}

type ResourceModel struct {
	resourceRef uuid.UUID
	attributes  map[string]string
	authScope   []string
}

func NewResourceModel(resource pcommon.Resource) (*ResourceModel, error) {
	resourceRef := pdatautil.MapHash(resource.Attributes())
	refUUID, err := uuid.FromBytes(resourceRef[:])
	if err != nil {
		return nil, err
	}

	resAttr := attributesToMap(resource.Attributes())
	authScope := attributesToAuthScope(resource.Attributes())
	return &ResourceModel{
		resourceRef: refUUID,
		attributes:  resAttr,
		authScope:   authScope,
	}, nil
}

func attributesToAuthScope(attrs pcommon.Map) []string {
	clusterName, ok := attrs.Get("k8s.cluster.name")
	if ok {
		namespaceName, nsok := attrs.Get("k8s.namespace.name")
		if nsok {
			return []string{
				fmt.Sprintf("k8s.cluster.name:%s", clusterName.AsString()),
				fmt.Sprintf("k8s.scope:%s/%s", clusterName.AsString(), namespaceName.AsString()),
			}
		}
		return []string{
			fmt.Sprintf("k8s.cluster.name:%s", clusterName.AsString()),
		}
	}
	return []string{}
}

func NewResourceExporter(logger *zap.Logger, cfg *Config) (*ResourcesExporter, error) {
	client, err := newClickhouseClient(cfg)
	if err != nil {
		return nil, err
	}

	return &ResourcesExporter{
		client: client,
		logger: logger,

		insertSQL: renderInsertResourcesSQL(cfg.ResourcesTableName),
		cfg:       cfg,
	}, nil
}

// shutdown will shut down the exporter.
func (e *ResourcesExporter) Shutdown(_ context.Context) error {
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (e *ResourcesExporter) Start(ctx context.Context, _ component.Host) error {
	if !e.cfg.CreateResourcesTable {
		return nil
	}

	if err := createDatabase(ctx, e.cfg); err != nil {
		return err
	}

	return createResourcesTable(ctx, e.cfg.TTLDays, e.cfg.TTL, e.cfg.ResourcesTableName, e.client)
}

func (e *ResourcesExporter) InsertResources(ctx context.Context, resources []*ResourceModel) error {
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
				resource.attributes,
				resource.authScope,
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
     ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	 AuthScope Array(LowCardinality(String)),
) ENGINE = ReplacingMergeTree
%s
ORDER BY (ResourceRef, toUnixTimestamp(Timestamp))
SETTINGS index_granularity=512, ttl_only_drop_parts = 1;
`
	// language=ClickHouse SQL
	//nolint:lll
	insertResourcesSQLTemplate = `INSERT INTO %s (Timestamp, ResourceRef, ResourceAttributes, AuthScope) VALUES (?, ?, ?, ?)`
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
