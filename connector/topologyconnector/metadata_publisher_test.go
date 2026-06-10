//nolint:testpackage
package topologyconnector

import (
	"fmt"
	"testing"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
)

func extractMetadataRecords(t *testing.T, logs plog.Logs) []plog.LogRecord {
	t.Helper()
	var records []plog.LogRecord
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rl := logs.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				records = append(records, sl.LogRecords().At(k))
			}
		}
	}
	return records
}

func assertMetadataLogRecord(t *testing.T, lr plog.LogRecord, expectedDataSource, expectedDisplayName string) {
	t.Helper()

	topicAttr, ok := lr.Attributes().Get(stskafkaexporter.KafkaMessageTopic)
	require.True(t, ok, "missing stskafka.topic attribute")
	assert.Equal(t, "sts_topology_stream_metadata", topicAttr.Str())

	keyAttr, ok := lr.Attributes().Get(stskafkaexporter.KafkaMessageKey)
	require.True(t, ok, "missing stskafka.key attribute")
	var key topostreamv1.TopologyStreamMetadataMessageKey
	require.NoError(t, proto.Unmarshal(keyAttr.Bytes().AsRaw(), &key))
	assert.Equal(t, topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL, key.GetOwner())
	assert.Equal(t, expectedDataSource, key.GetDataSource())

	var msg topostreamv1.TopologyStreamMetadataMessage
	require.NoError(t, proto.Unmarshal(lr.Body().Bytes().AsRaw(), &msg))
	assert.Equal(t, expectedDisplayName, msg.GetDisplayName())
}

func TestMetadataPublisher(t *testing.T) {
	t.Run("publishes metadata for component and relation mappings", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		sink := &consumertest.LogsSink{}
		publisher := NewMetadataPublisher(logger)
		publisher.SetLogsConsumer(sink)

		componentMappings := map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping{
			settingsproto.TRACES: {
				{
					Id:         "comp1",
					Identifier: "urn:otel-component-mapping:comp1",
					Name:       "My Component Mapping",
					Input:      settingsproto.OtelInput{Signal: []settingsproto.OtelInputSignal{settingsproto.TRACES}},
				},
			},
		}
		relationMappings := map[settingsproto.OtelInputSignal][]settingsproto.OtelRelationMapping{
			settingsproto.TRACES: {
				{
					Id:         "rel1",
					Identifier: "urn:otel-relation-mapping:rel1",
					Name:       "My Relation Mapping",
					Input:      settingsproto.OtelInput{Signal: []settingsproto.OtelInputSignal{settingsproto.TRACES}},
				},
			},
		}

		publisher.Update(
			[]settingsproto.OtelInputSignal{settingsproto.TRACES},
			componentMappings,
			relationMappings,
		)

		allLogs := sink.AllLogs()
		require.Len(t, allLogs, 1)
		records := extractMetadataRecords(t, allLogs[0])
		require.Len(t, records, 2)

		byDataSource := make(map[string]plog.LogRecord)
		for _, r := range records {
			keyAttr, _ := r.Attributes().Get(stskafkaexporter.KafkaMessageKey)
			var key topostreamv1.TopologyStreamMetadataMessageKey
			_ = proto.Unmarshal(keyAttr.Bytes().AsRaw(), &key)
			byDataSource[key.GetDataSource()] = r
		}

		assertMetadataLogRecord(t, byDataSource["urn:otel-component-mapping:comp1"], "urn:otel-component-mapping:comp1", "My Component Mapping")
		assertMetadataLogRecord(t, byDataSource["urn:otel-relation-mapping:rel1"], "urn:otel-relation-mapping:rel1", "My Relation Mapping")
	})

	t.Run("deduplicates mappings across signals", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		sink := &consumertest.LogsSink{}
		publisher := NewMetadataPublisher(logger)
		publisher.SetLogsConsumer(sink)

		mapping := settingsproto.OtelComponentMapping{
			Id:         "shared",
			Identifier: "urn:otel-component-mapping:shared",
			Name:       "Shared Mapping",
			Input: settingsproto.OtelInput{
				Signal: []settingsproto.OtelInputSignal{settingsproto.TRACES, settingsproto.METRICS},
			},
		}
		componentMappings := map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping{
			settingsproto.TRACES:  {mapping},
			settingsproto.METRICS: {mapping},
		}

		publisher.Update(
			[]settingsproto.OtelInputSignal{settingsproto.TRACES, settingsproto.METRICS},
			componentMappings,
			nil,
		)

		allLogs := sink.AllLogs()
		require.Len(t, allLogs, 1)
		records := extractMetadataRecords(t, allLogs[0])
		assert.Len(t, records, 1)
	})

	t.Run("republishes on repeated snapshots", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		sink := &consumertest.LogsSink{}
		publisher := NewMetadataPublisher(logger)
		publisher.SetLogsConsumer(sink)

		mappings := map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping{
			settingsproto.TRACES: {
				{Id: "m1", Identifier: "urn:m1", Name: "M1"},
			},
		}

		publisher.Update([]settingsproto.OtelInputSignal{settingsproto.TRACES}, mappings, nil)
		publisher.Update([]settingsproto.OtelInputSignal{settingsproto.TRACES}, mappings, nil)
		assert.Len(t, sink.AllLogs(), 2, "should republish on every snapshot — compaction handles dedup")
	})

	t.Run("skips publish when no consumer is set", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		publisher := NewMetadataPublisher(logger)

		assert.NotPanics(t, func() {
			publisher.Update(
				[]settingsproto.OtelInputSignal{settingsproto.TRACES},
				map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping{
					settingsproto.TRACES: {
						{Id: "x", Identifier: "urn:x", Name: "X"},
					},
				},
				nil,
			)
		})
	})

	t.Run("skips publish when no mappings", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		sink := &consumertest.LogsSink{}
		publisher := NewMetadataPublisher(logger)
		publisher.SetLogsConsumer(sink)

		publisher.Update(nil, nil, nil)

		assert.Empty(t, sink.AllLogs())
	})

	t.Run("selector_properties contains nested mapping struct", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		sink := &consumertest.LogsSink{}
		publisher := NewMetadataPublisher(logger)
		publisher.SetLogsConsumer(sink)

		publisher.Update(
			[]settingsproto.OtelInputSignal{settingsproto.TRACES},
			map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping{
				settingsproto.TRACES: {
					{
						Id:         "m1",
						Identifier: fmt.Sprintf("urn:otel-component-mapping:%s", "m1"),
						Name:       "My Mapping",
					},
				},
			},
			nil,
		)

		records := extractMetadataRecords(t, sink.AllLogs()[0])
		var msg topostreamv1.TopologyStreamMetadataMessage
		require.NoError(t, proto.Unmarshal(records[0].Body().Bytes().AsRaw(), &msg))

		sp := msg.GetSelectorProperties()
		require.NotNil(t, sp)
		mappingStruct := sp.GetFields()["mapping"].GetStructValue()
		require.NotNil(t, mappingStruct)
		assert.Equal(t, "urn:otel-component-mapping:m1", mappingStruct.GetFields()["identifier"].GetStringValue())
		assert.Equal(t, "My Mapping", mappingStruct.GetFields()["name"].GetStringValue())
	})

	t.Run("publishes tombstones for removed mappings", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		sink := &consumertest.LogsSink{}
		publisher := NewMetadataPublisher(logger)
		publisher.SetLogsConsumer(sink)

		removed := []settingsproto.SettingExtension{
			settingsproto.OtelComponentMapping{
				Id:         "comp1",
				Identifier: "urn:otel-component-mapping:comp1",
				Name:       "Removed Mapping",
			},
		}

		publisher.PublishTombstones(removed)

		allLogs := sink.AllLogs()
		require.Len(t, allLogs, 1)
		records := extractMetadataRecords(t, allLogs[0])
		require.Len(t, records, 1)

		lr := records[0]
		// Tombstone: body should be empty bytes
		assert.Equal(t, 0, len(lr.Body().Bytes().AsRaw()))

		// Key should still be present with correct data source
		keyAttr, ok := lr.Attributes().Get(stskafkaexporter.KafkaMessageKey)
		require.True(t, ok)
		var key topostreamv1.TopologyStreamMetadataMessageKey
		require.NoError(t, proto.Unmarshal(keyAttr.Bytes().AsRaw(), &key))
		assert.Equal(t, topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL, key.GetOwner())
		assert.Equal(t, "urn:otel-component-mapping:comp1", key.GetDataSource())

		// Topic attribute should be set
		topicAttr, ok := lr.Attributes().Get(stskafkaexporter.KafkaMessageTopic)
		require.True(t, ok)
		assert.Equal(t, "sts_topology_stream_metadata", topicAttr.Str())
	})

	t.Run("skips tombstones when no mappings removed", func(t *testing.T) {
		logger := zaptest.NewLogger(t)
		sink := &consumertest.LogsSink{}
		publisher := NewMetadataPublisher(logger)
		publisher.SetLogsConsumer(sink)

		publisher.PublishTombstones(nil)
		assert.Empty(t, sink.AllLogs())
	})
}
