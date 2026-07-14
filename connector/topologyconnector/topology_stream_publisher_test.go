package topologyconnector

import (
	"context"
	"testing"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func dataMessage(dataSource, shardID string) internal.MessageWithKey {
	return internal.MessageWithKey{
		Key: &topostreamv1.TopologyStreamMessageKey{
			Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: dataSource,
			ShardId:    shardID,
		},
		Message: &topostreamv1.TopologyStreamMessage{
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{},
			},
		},
	}
}

func removeMessage(dataSource, shardID string) internal.MessageWithKey {
	return internal.MessageWithKey{
		Key: &topostreamv1.TopologyStreamMessageKey{
			Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: dataSource,
			ShardId:    shardID,
		},
		Message: &topostreamv1.TopologyStreamMessage{
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRemove{
				TopologyStreamRemove: &topostreamv1.TopologyStreamRemove{RemovalCause: "gone"},
			},
		},
	}
}

func newTestPublisher(t *testing.T) (*TopologyStreamPublisher, *observer.ObservedLogs, *consumertest.LogsSink) {
	t.Helper()
	core, logs := observer.New(zap.InfoLevel)
	sink := &consumertest.LogsSink{}
	p := NewTopologyStreamPublisher(zap.New(core))
	p.SetLogsConsumer(sink)
	return p, logs, sink
}

func createdCount(logs *observer.ObservedLogs) int {
	return logs.FilterMessage("Topology stream created").Len()
}

func removedCount(logs *observer.ObservedLogs) int {
	return logs.FilterMessage("Topology stream removed").Len()
}

func TestTopologyStreamPublisher_TracksCreationOncePerKey(t *testing.T) {
	p, logs, sink := newTestPublisher(t)

	// Two distinct shard streams for the same dataSource.
	p.Publish(context.Background(), []internal.MessageWithKey{
		dataMessage("urn:test:a", "0"),
		dataMessage("urn:test:a", "1"),
	})

	assert.Equal(t, 2, createdCount(logs), "one 'created' log per new shard stream")
	assert.Equal(t, 2, sink.LogRecordCount())

	// Re-publishing the same keys must not log creation again.
	p.Publish(context.Background(), []internal.MessageWithKey{
		dataMessage("urn:test:a", "0"),
		dataMessage("urn:test:a", "1"),
	})
	assert.Equal(t, 2, createdCount(logs), "no new 'created' logs when nothing new is seen")
}

func TestTopologyStreamPublisher_IgnoresRemoveAndUnknownShard(t *testing.T) {
	p, logs, _ := newTestPublisher(t)

	p.Publish(context.Background(), []internal.MessageWithKey{
		removeMessage("urn:test:a", "0"),                   // not a data payload
		dataMessage("urn:test:a", internal.UnknownShardID), // error pseudo-shard
	})

	assert.Equal(t, 0, createdCount(logs), "neither a remove payload nor the unknown shard counts as a stream")
}

func TestTopologyStreamPublisher_OnMappingRemovedEvicts(t *testing.T) {
	p, logs, _ := newTestPublisher(t)

	p.Publish(context.Background(), []internal.MessageWithKey{
		dataMessage("urn:test:a", "0"),
		dataMessage("urn:test:a", "1"),
		dataMessage("urn:test:b", "0"),
	})
	require.Equal(t, 3, createdCount(logs))

	p.OnMappingRemoved([]string{"urn:test:a"})
	assert.Equal(t, 1, removedCount(logs))

	// A later re-creation of an evicted stream is logged again; urn:test:b was untouched.
	p.Publish(context.Background(), []internal.MessageWithKey{
		dataMessage("urn:test:a", "0"), // evicted -> logged again
		dataMessage("urn:test:b", "0"), // still known -> not logged again
	})
	assert.Equal(t, 4, createdCount(logs))
}

func TestTopologyStreamPublisher_NoConsumerDropsSilently(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	p := NewTopologyStreamPublisher(zap.New(core))
	// No SetLogsConsumer call.

	p.Publish(context.Background(), []internal.MessageWithKey{dataMessage("urn:test:a", "0")})

	assert.Equal(t, 0, createdCount(logs), "nothing is published or tracked without a consumer")
}
