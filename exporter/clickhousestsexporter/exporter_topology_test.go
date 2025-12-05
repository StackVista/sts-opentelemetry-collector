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
	"google.golang.org/protobuf/types/known/structpb"

	topostream "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
)

//nolint:forcetypeassert
func TestPushTopologyData(t *testing.T) {
	var componentItems int
	var relationItems int
	var componentValues []driver.Value

	var logItems int
	// Test Config
	cfg := withDefaultConfig(func(cfg *clickhousestsexporter.Config) {
		cfg.EnableTopology = true
		cfg.EnableLogs = false // Disable regular logs to isolate topology testing
		cfg.SetDriverName(t.Name())
	})

	initClickhouseTestServer(t, func(query string, values []driver.Value) error {
		switch {
		case strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.ComponentsTableName)):
			componentItems++
			componentValues = values
		case strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.RelationsTableName)):
			relationItems++
		case strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.LogsTableName)):
			logItems++
		}
		return nil
	})

	// Create Exporter
	exporter := newTestTopologyLogsExporter(t, cfg)

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

	typeID := "type_id"
	layerNameStr := "layer_name"
	layerID := "layer_id"
	domainNameStr := "domain_name"
	domainID := "domain_id"

	topoMsg := &topostream.TopologyStreamMessage{
		CollectionTimestamp: ts.UnixMilli(),
		Payload: &topostream.TopologyStreamMessage_TopologyStreamSnapshotData{
			TopologyStreamSnapshotData: &topostream.TopologyStreamSnapshotData{
				Components: []*topostream.TopologyStreamComponent{
					{
						ExternalId:         "urn:test:component",
						Name:               "test-component",
						TypeName:           "service",
						TypeIdentifier:     &typeID,
						LayerName:          layerNameStr,
						LayerIdentifier:    &layerID,
						DomainName:         domainNameStr,
						DomainIdentifier:   &domainID,
						Identifiers:        []string{"id1", "id2"},
						ResourceDefinition: resourceDefStruct,
						StatusData:         statusDataStruct,
						Tags:               []string{"tag1", "tag2:value"},
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
	require.Equal(t, 1, componentItems)
	require.Equal(t, 0, relationItems)
	require.Equal(t, 0, logItems)

	require.Equal(t, "urn:test:component", componentValues[1])                                               // Identifier
	require.Equal(t, "test-component", componentValues[2])                                                   // Name
	require.Equal(t, []string{"tag1", "tag2:value"}, componentValues[3].([]string))                          // Tags
	require.Equal(t, map[string]string{"tag1": "", "tag2": "value"}, componentValues[4].(map[string]string)) // Tags
	require.Equal(t, "service", componentValues[5])                                                          // TypeName
	require.Equal(t, "type_id", componentValues[6])                                                          // TypeIdentifier
	require.Equal(t, "layer_name", componentValues[7])                                                       // LayerName
	require.Equal(t, "layer_id", componentValues[8])                                                         // LayerIdentifier
	require.Equal(t, "domain_name", componentValues[9])                                                      // DomainName
	require.Equal(t, "domain_id", componentValues[10])                                                       // DomainIdentifier
	require.ElementsMatch(t, []string{"id1", "id2"}, componentValues[11].([]string))                         // ComponentIdentifiers
	require.Equal(t, `{"key":"value"}`, componentValues[12])                                                 // ResourceDefinition (JSON)
	require.Equal(t, `{"status":"ok"}`, componentValues[13])                                                 // StatusData (JSON)
}

func TestPushTopologyData_Disabled(t *testing.T) {
	var componentItems int
	var relationItems int
	var logItems int

	// Test Config
	cfg := withDefaultConfig(func(cfg *clickhousestsexporter.Config) {
		cfg.EnableTopology = false // <-- Topology disabled
		cfg.EnableLogs = false
		cfg.SetDriverName(t.Name())
	})

	//nolint:revive
	initClickhouseTestServer(t, func(query string, values []driver.Value) error {
		switch {
		case strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.ComponentsTableName)):
			componentItems++
		case strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.RelationsTableName)):
			relationItems++
		case strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.LogsTableName)):
			logItems++
		}
		return nil
	})

	// Create Exporter
	exporter := newTestTopologyLogsExporter(t, cfg)

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

	require.Equal(t, 0, componentItems)
	require.Equal(t, 0, relationItems)
	require.Equal(t, 0, logItems)
}

//nolint:forcetypeassert
func TestPushMixedLogs(t *testing.T) {
	var componentItems int
	var relationItems int
	// var logItems int
	var componentValues []driver.Value
	var relationValues []driver.Value
	// var logValues []driver.Value

	cfg := withDefaultConfig(func(cfg *clickhousestsexporter.Config) {
		cfg.EnableTopology = true
		cfg.EnableLogs = true
		cfg.SetDriverName(t.Name())
	})
	initClickhouseTestServer(t, func(query string, values []driver.Value) error {
		switch {
		case strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.LogsTableName)):
			// logItems++
			// logValues = values
		case strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.ComponentsTableName)):
			componentItems++
			componentValues = values
		case strings.HasPrefix(query, fmt.Sprintf("INSERT INTO %s", cfg.RelationsTableName)):
			relationItems++
			relationValues = values
		}
		return nil
	})

	exporter := newTestTopologyLogsExporter(t, cfg)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(string(conventions.ServiceNameKey), "test-service")
	sl := rl.ScopeLogs().AppendEmpty()

	// Topology component record
	ts := time.Now()
	resourceDefStruct, err := structpb.NewStruct(map[string]interface{}{"key": "value"})
	require.NoError(t, err)
	statusDataStruct, err := structpb.NewStruct(map[string]interface{}{"status": "ok"})
	require.NoError(t, err)

	typeID := "type_id"
	layerNameStr := "layer_name"
	layerID := "layer_id"
	domainNameStr := "domain_name"
	domainID := "domain_id"

	componentTopoMsg := &topostream.TopologyStreamMessage{
		CollectionTimestamp: ts.UnixMilli(),
		Payload: &topostream.TopologyStreamMessage_TopologyStreamSnapshotData{
			TopologyStreamSnapshotData: &topostream.TopologyStreamSnapshotData{
				Components: []*topostream.TopologyStreamComponent{
					{
						ExternalId:         "urn:test:component",
						Name:               "test-component",
						TypeName:           "service",
						TypeIdentifier:     &typeID,
						LayerName:          layerNameStr,
						LayerIdentifier:    &layerID,
						DomainName:         domainNameStr,
						DomainIdentifier:   &domainID,
						Identifiers:        []string{"id1"},
						ResourceDefinition: resourceDefStruct,
						StatusData:         statusDataStruct,
						Tags:               []string{"tag1", "tag2:value"},
					},
				},
			},
		},
	}
	componentTopoBody, err := proto.Marshal(componentTopoMsg)
	require.NoError(t, err)
	componentLr := sl.LogRecords().AppendEmpty()
	componentLr.Body().SetEmptyBytes().FromRaw(componentTopoBody)
	componentLr.Attributes().PutStr(stskafkaexporter.KafkaMessageKey, "some-component-key")

	// Topology Relation Record
	relTypeID := "rel_type_id"
	relationTopoMsg := &topostream.TopologyStreamMessage{
		CollectionTimestamp: ts.UnixMilli(),
		Payload: &topostream.TopologyStreamMessage_TopologyStreamSnapshotData{
			TopologyStreamSnapshotData: &topostream.TopologyStreamSnapshotData{
				Relations: []*topostream.TopologyStreamRelation{
					{
						ExternalId:       "urn:test:relation",
						Name:             "test-relation",
						TypeName:         "uses",
						TypeIdentifier:   &relTypeID,
						Tags:             []string{"rel_tag:value"},
						SourceIdentifier: "source_id",
						TargetIdentifier: "target_id",
					},
				},
			},
		},
	}
	relationTopoBody, err := proto.Marshal(relationTopoMsg)
	require.NoError(t, err)
	relationLr := sl.LogRecords().AppendEmpty()
	relationLr.Body().SetEmptyBytes().FromRaw(relationTopoBody)
	relationLr.Attributes().PutStr(stskafkaexporter.KafkaMessageKey, "some-relation-key")

	// Regular Log Record
	regLr := sl.LogRecords().AppendEmpty()
	regLr.Body().SetStr("This is a regular log")
	regLr.SetTimestamp(pcommon.NewTimestampFromTime(ts))

	// Push Data
	err = exporter.PushLogsData(context.Background(), logs)
	require.NoError(t, err)

	require.Equal(t, 1, componentItems)
	require.Equal(t, 1, relationItems)
	// require.Equal(t, 1, logItems)
	// require.Equal(t, "test-service", logValues[6]) // ServiceName for regular log
	// require.Equal(t, "This is a regular log", logValues[7])

	require.Equal(t, "urn:test:component", componentValues[1])                                               // Identifier
	require.Equal(t, "test-component", componentValues[2])                                                   // Name
	require.Equal(t, []string{"tag1", "tag2:value"}, componentValues[3].([]string))                          // Tags
	require.Equal(t, map[string]string{"tag1": "", "tag2": "value"}, componentValues[4].(map[string]string)) // Tags
	require.Equal(t, "service", componentValues[5])                                                          // TypeName
	require.Equal(t, "type_id", componentValues[6])                                                          // TypeIdentifier
	require.Equal(t, "layer_name", componentValues[7])                                                       // LayerName
	require.Equal(t, "layer_id", componentValues[8])                                                         // LayerIdentifier
	require.Equal(t, "domain_name", componentValues[9])                                                      // DomainName
	require.Equal(t, "domain_id", componentValues[10])                                                       // DomainIdentifier
	require.ElementsMatch(t, []string{"id1"}, componentValues[11].([]string))                                // ComponentIdentifiers
	require.Equal(t, `{"key":"value"}`, componentValues[12])                                                 // ResourceDefinition (JSON)
	require.Equal(t, `{"status":"ok"}`, componentValues[13])                                                 // StatusData (JSON)

	require.Equal(t, "test-relation", relationValues[1])                                           // Name
	require.Equal(t, []string{"rel_tag:value"}, relationValues[2].([]string))                      // Labels
	require.Equal(t, map[string]string{"rel_tag": "value"}, relationValues[3].(map[string]string)) // Tags
	require.Equal(t, "uses", relationValues[4])                                                    // TypeName
	require.Equal(t, "rel_type_id", relationValues[5])                                             // TypeIdentifier
	require.Equal(t, "source_id", relationValues[6])                                               // SourceIdentifier
	require.Equal(t, "target_id", relationValues[7])                                               // TargetIdentifier
}

// Below are the helper functions copied from exporter_logs_test.go, adapted for this package
func newTestTopologyLogsExporter(t *testing.T, config *clickhousestsexporter.Config) *clickhousestsexporter.LogsExporter {
	exporter, err := clickhousestsexporter.NewLogsExporter(zaptest.NewLogger(t), config)
	require.NoError(t, err)
	require.NoError(t, exporter.Start(context.TODO(), nil))

	t.Cleanup(func() { _ = exporter.Shutdown(context.TODO()) })
	return exporter
}
