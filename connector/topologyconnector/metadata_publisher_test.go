//nolint:testpackage
package topologyconnector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
)

func TestMetadataPublisher_SerializesPendingRecords(t *testing.T) {
	for _, finalState := range []string{"deleted", "recreated"} {
		t.Run(finalState, func(t *testing.T) {
			received := make(chan plog.Logs, 32)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			var calls atomic.Int32
			sink, err := consumer.NewLogs(func(_ context.Context, logs plog.Logs) error {
				call := calls.Add(1)
				received <- logs
				if call == 1 {
					<-release
					return errors.New("first export failed")
				}
				return nil
			})
			require.NoError(t, err)
			publisher := NewMetadataPublisher(zaptest.NewLogger(t))
			publisher.SetLogsConsumer(sink)
			mapping := settingsproto.OtelComponentMapping{Identifier: "urn:test:mapping", Name: "original"}
			publish := func(mapping settingsproto.OtelComponentMapping) {
				publisher.Update(nil, map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping{
					settingsproto.TRACES: {mapping},
				}, nil)
			}
			receive := func() plog.Logs {
				t.Helper()
				select {
				case logs := <-received:
					return logs
				case <-time.After(5 * time.Second):
					t.Fatal("metadata export did not arrive")
					return plog.Logs{}
				}
			}
			firstDone := make(chan struct{})
			go func() {
				publish(mapping)
				close(firstDone)
			}()
			first := extractMetadataRecords(t, receive())
			require.Len(t, first, 1)
			assertMetadataLogRecord(t, first[0], "urn:test:mapping", "original")

			queued := make(chan struct{})
			go func() {
				for i := range 20 {
					mapping.Name = fmt.Sprintf("update-%d", i)
					publish(mapping)
				}
				publisher.PublishTombstones([]settingsproto.SettingExtension{mapping})
				if finalState == "recreated" {
					mapping.Name = "recreated"
					publish(mapping)
				}
				close(queued)
			}()
			t.Cleanup(func() {
				unblock()
				<-firstDone
				<-queued
				require.Eventually(t, func() bool {
					publisher.pendingMu.Lock()
					defer publisher.pendingMu.Unlock()
					return !publisher.publishing
				}, 5*time.Second, time.Millisecond)
			})
			select {
			case <-queued:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("pending metadata blocked on the active export")
			}
			select {
			case <-received:
				t.Fatal("a newer export overtook the blocked export")
			case <-time.After(100 * time.Millisecond):
			}
			unblock()
			latest := extractMetadataRecords(t, receive())
			require.Len(t, latest, 1, "pending updates for one mapping must coalesce")
			if finalState == "deleted" {
				assert.Empty(t, latest[0].Body().Bytes().AsRaw())
			} else {
				assertMetadataLogRecord(t, latest[0], "urn:test:mapping", "recreated")
			}
			assert.Equal(t, int32(2), calls.Load())
		})
	}
}

func TestSnapshotManager_MetadataBackpressure(t *testing.T) {
	for _, operation := range []string{"read", "stop"} {
		t.Run(operation, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			exported := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			sink, err := consumer.NewLogs(func(context.Context, plog.Logs) error {
				close(entered)
				<-release
				close(exported)
				return nil
			})
			require.NoError(t, err)
			publisher := NewMetadataPublisher(zaptest.NewLogger(t))
			publisher.SetLogsConsumer(sink)
			manager := NewSnapshotManager(zaptest.NewLogger(t), []settingsproto.OtelInputSignal{settingsproto.TRACES}, publisher)
			onRemovals := func(context.Context, []settingsproto.OtelComponentMapping, []settingsproto.OtelRelationMapping) {}
			require.NoError(t, manager.Start(t.Context(), NewMockStsSettingsProvider(nil, nil), onRemovals))
			updated := make(chan struct{})
			go func() {
				manager.Update(t.Context(), []settingsproto.OtelComponentMapping{
					{Identifier: "urn:test:mapping", Input: settingsproto.OtelInput{Signal: []settingsproto.OtelInputSignal{settingsproto.TRACES}}},
				}, nil, onRemovals)
				close(updated)
			}()
			t.Cleanup(func() {
				unblock()
				<-updated
				<-exported
				manager.Stop()
			})
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("metadata export did not start")
			}
			done := make(chan struct{})
			go func() {
				if operation == "read" {
					manager.Current(settingsproto.TRACES)
				} else {
					manager.Stop()
				}
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
				t.Errorf("%s blocked on metadata export", operation)
			}
			unblock()
			<-done
		})
	}
}

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

func awaitMetadataLogs(t *testing.T, sink *consumertest.LogsSink, count int) []plog.Logs {
	t.Helper()
	require.Eventually(t, func() bool { return len(sink.AllLogs()) == count }, 5*time.Second, time.Millisecond)
	return sink.AllLogs()
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

		allLogs := awaitMetadataLogs(t, sink, 1)
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

		allLogs := awaitMetadataLogs(t, sink, 1)
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
		awaitMetadataLogs(t, sink, 1)
		publisher.Update([]settingsproto.OtelInputSignal{settingsproto.TRACES}, mappings, nil)
		awaitMetadataLogs(t, sink, 2)
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

		records := extractMetadataRecords(t, awaitMetadataLogs(t, sink, 1)[0])
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

		allLogs := awaitMetadataLogs(t, sink, 1)
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
