// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhousestsexporter

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // For register database driver.
	topostream "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter/internal/topology"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type TopologyExporter struct {
	client    *sql.DB
	insertSQL string

	logger *zap.Logger
	cfg    *Config
}

func NewTopologyExporter(logger *zap.Logger, cfg *Config) (*TopologyExporter, error) {
	client, err := newClickhouseClient(cfg)
	if err != nil {
		return nil, err
	}

	return &TopologyExporter{
		client:    client,
		insertSQL: topology.RenderInsertTopologySQL(cfg),
		logger:    logger,
		cfg:       cfg,
	}, nil
}

func (e *TopologyExporter) Start(ctx context.Context, _ component.Host) error {
	if !e.cfg.CreateTopologyTable {
		return nil
	}

	if err := createDatabase(ctx, e.cfg); err != nil {
		return err
	}

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
	return nil
}

// shutdown will shut down the exporter.
func (e *TopologyExporter) Shutdown(_ context.Context) error {
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (e *TopologyExporter) PushLogsData(ctx context.Context, ld plog.Logs) error {
	start := time.Now()
	err := doWithTx(ctx, e.client, func(tx *sql.Tx) error {
		statement, err := tx.PrepareContext(ctx, e.insertSQL)
		if err != nil {
			return fmt.Errorf("PrepareContext:%w", err)
		}
		defer func() {
			_ = statement.Close()
		}()

		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			sl := ld.ResourceLogs().At(i).ScopeLogs()
			for j := 0; j < sl.Len(); j++ {
				rs := sl.At(j).LogRecords()
				for k := 0; k < rs.Len(); k++ {
					r := rs.At(k)
					body := r.Body().Bytes().AsRaw()
					if body == nil {
						continue
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
						resourceDefBytes, _ := json.Marshal(component.GetResourceDefinition())
						statusDataBytes, _ := json.Marshal(component.GetStatusData())

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
						_, err = statement.ExecContext(ctx,
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
				}
			}
		}
		return nil
	})
	duration := time.Since(start)
	e.logger.Debug("insert topology", zap.Int("records", ld.LogRecordCount()),
		zap.String("cost", duration.String()))
	return err
}
