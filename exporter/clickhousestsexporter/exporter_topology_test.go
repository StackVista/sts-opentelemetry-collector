// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhousestsexporter_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"

	topostream "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPushTopologyData(t *testing.T) {
	var topologyItems int
	var logItems int

	// Test Config
	cfg := withDefaultConfig(func(cfg *clickhousestsexporter.Config) {
		cfg.EnableTopology = true
		cfg.EnableLogs = false // Disable regular logs to isolate topology testing
		cfg.SetDriverName(t.Name())
		cfg.Endpoint = defaultEndpoint
	})

	initClickhouseTestServer(t, func(query string, values []driver.Value) error {
		if strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.LogsTableName)) {
			logItems++
		} else if strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.TopologyTableName)) {
			topologyItems++
			require.Equal(t, "urn:test:component", values[1])                        // Identifier
			require.Equal(t, "component", values[2])                                 // Type
			require.Equal(t, "test-component", values[3])                            // Name
			require.ElementsMatch(t, []string{"tag1", "tag2"}, values[4].([]string)) // Tags
			require.Equal(t, "service", values[5])                                   // TypeName
			require.Equal(t, "type_id", values[6])                                   // TypeIdentifier
			require.Equal(t, "layer_name", values[7])                                // LayerName
			require.Equal(t, "layer_id", values[8])                                  // LayerIdentifier
			require.Equal(t, "domain_name", values[9])                               // DomainName
			require.Equal(t, "domain_id", values[10])                                // DomainIdentifier
			require.ElementsMatch(t, []string{"id1", "id2"}, values[11].([]string))  // ComponentIdentifiers
			require.Equal(t, `{"key":"value"}`, values[12])                          // ResourceDefinition (JSON)
			require.Equal(t, `{"status":"ok"}`, values[13])                          // StatusData (JSON)
			require.Nil(t, values[14])                                               // SourceIdentifier
			require.Nil(t, values[15])                                               // TargetIdentifier
		}
		return nil
	})

	// Create Exporter
	exporter := newTestTopologyLogsExporter(t, defaultEndpoint, cfg)

	// Test Data
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()

	// Create a topology message
	ts := time.Now()

	resourceDefStruct, err := structpb.NewStruct(map[string]interface{}{"key": "value"})
	require.NoError(t, err)
	statusDataStruct, err := structpb.NewStruct(map[string]interface{}{"status": "ok"})
	require.NoError(t, err)

	typeId := "type_id"
	layerName := "layer_name"
	layerId := "layer_id"
	domainName := "domain_name"
	domainId := "domain_id"

	topoMsg := &topostream.TopologyStreamMessage{
		CollectionTimestamp: ts.UnixMilli(),
		Payload: &topostream.TopologyStreamMessage_TopologyStreamSnapshotData{
			TopologyStreamSnapshotData: &topostream.TopologyStreamSnapshotData{
				Components: []*topostream.TopologyStreamComponent{
					{
						ExternalId:         "urn:test:component",
						Name:               "test-component",
						TypeName:           "service",
						TypeIdentifier:     &typeId,
						LayerName:          layerName,
						LayerIdentifier:    &layerId,
						DomainName:         domainName,
						DomainIdentifier:   &domainId,
						Identifiers:        []string{"id1", "id2"},
						ResourceDefinition: resourceDefStruct,
						StatusData:         statusDataStruct,
						Tags:               []string{"tag1", "tag2"},
					},
				},
			},
		},
	}
	body, err := proto.Marshal(topoMsg)
	require.NoError(t, err)
	lr.Body().SetEmptyBytes().FromRaw(body)
	lr.Attributes().PutStr(stskafkaexporter.KafkaMessageKey, "some-key")

	// Push Data
	err = exporter.PushLogsData(context.Background(), logs)
	require.NoError(t, err)
	require.Equal(t, 1, topologyItems)
	require.Equal(t, 0, logItems)
}

func TestPushTopologyData_Disabled(t *testing.T) {
	var topologyItems int
	var logItems int

	// Test Config
	cfg := withDefaultConfig(func(cfg *clickhousestsexporter.Config) {
		cfg.EnableTopology = false // <-- Topology disabled
		cfg.EnableLogs = false
		cfg.SetDriverName(t.Name())
	})
	initClickhouseTestServer(t, func(query string, values []driver.Value) error {
		if strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.LogsTableName)) {
			logItems++
		} else if strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.TopologyTableName)) {
			topologyItems++
		}
		return nil
	})

	// Create Exporter
	exporter := newTestTopologyLogsExporter(t, defaultEndpoint, cfg)

	// Test Data (same as above, but should be ignored)
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	topoMsg := &topostream.TopologyStreamMessage{}
	body, err := proto.Marshal(topoMsg)
	require.NoError(t, err)
	lr.Body().SetEmptyBytes().FromRaw(body)
	lr.Attributes().PutStr(stskafkaexporter.KafkaMessageKey, "some-key")

	// Push Data
	err = exporter.PushLogsData(context.Background(), logs)
	require.NoError(t, err)

	require.Equal(t, 0, topologyItems)
	require.Equal(t, 0, logItems)
}

func TestPushMixedLogs(t *testing.T) {
	var topologyItems int
	var logItems int

	cfg := withDefaultConfig(func(cfg *clickhousestsexporter.Config) {
		cfg.EnableTopology = true
		cfg.EnableLogs = true
		cfg.SetDriverName(t.Name())
	})

	initClickhouseTestServer(t, func(query string, values []driver.Value) error {
		if strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.LogsTableName)) {
			logItems++
			require.Equal(t, "test-service", values[6]) // ServiceName for regular log
			require.Equal(t, "This is a regular log", values[7])
		} else if strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.TopologyTableName)) {
			topologyItems++
			require.Equal(t, "urn:test:relation", values[1])                    // Identifier
			require.Equal(t, "relation", values[2])                             // Type
			require.Equal(t, "test-relation", values[3])                        // Name
			require.ElementsMatch(t, []string{"rel_tag"}, values[4].([]string)) // Tags
			require.Equal(t, "uses", values[5])                                 // TypeName
			require.Equal(t, "rel_type_id", values[6])                          // TypeIdentifier
			require.Nil(t, values[7])                                           // LayerName
			require.Nil(t, values[8])                                           // LayerIdentifier
			require.Nil(t, values[9])                                           // DomainName
			require.Nil(t, values[10])                                          // DomainIdentifier
			require.Nil(t, values[11])                                          // ComponentIdentifiers
			require.Nil(t, values[12])                                          // ResourceDefinition
			require.Nil(t, values[13])                                          // StatusData
			require.Equal(t, "source_id", values[14])                           // SourceIdentifier
			require.Equal(t, "target_id", values[15])                           // TargetIdentifier
		}
		return nil
	})

	exporter := newTestTopologyLogsExporter(t, defaultEndpoint, cfg)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(string(conventions.ServiceNameKey), "test-service")
	sl := rl.ScopeLogs().AppendEmpty()

	// Topology Log Record
	ts := time.Now()
	relTypeId := "rel_type_id"
	topoMsg := &topostream.TopologyStreamMessage{
		CollectionTimestamp: ts.UnixMilli(),
		Payload: &topostream.TopologyStreamMessage_TopologyStreamSnapshotData{
			TopologyStreamSnapshotData: &topostream.TopologyStreamSnapshotData{
				Relations: []*topostream.TopologyStreamRelation{
					{
						ExternalId:       "urn:test:relation",
						Name:             "test-relation",
						TypeName:         "uses",
						TypeIdentifier:   &relTypeId,
						Tags:             []string{"rel_tag"},
						SourceIdentifier: "source_id",
						TargetIdentifier: "target_id",
					},
				},
			},
		},
	}
	topoBody, err := proto.Marshal(topoMsg)
	require.NoError(t, err)
	topoLr := sl.LogRecords().AppendEmpty()
	topoLr.Body().SetEmptyBytes().FromRaw(topoBody)
	topoLr.Attributes().PutStr(stskafkaexporter.KafkaMessageKey, "some-key")

	// Regular Log Record
	regLr := sl.LogRecords().AppendEmpty()
	regLr.Body().SetStr("This is a regular log")
	regLr.SetTimestamp(pcommon.NewTimestampFromTime(ts))

	// Push Data
	err = exporter.PushLogsData(context.Background(), logs)
	require.NoError(t, err)

	require.Equal(t, 1, topologyItems)
	require.Equal(t, 1, logItems)
}

// Below are the helper functions copied from exporter_logs_test.go, adapted for this package

func newTestTopologyLogsExporter(t *testing.T, dsn string, config *clickhousestsexporter.Config) *clickhousestsexporter.LogsExporter {
	exporter, err := clickhousestsexporter.NewLogsExporter(zaptest.NewLogger(t), config)
	require.NoError(t, err)
	require.NoError(t, exporter.Start(context.TODO(), nil))

	t.Cleanup(func() { _ = exporter.Shutdown(context.TODO()) })
	return exporter
}
