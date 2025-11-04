package internal

import (
	"fmt"
	"hash/fnv"
	"time"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
)

// ShardCount is the number of shards to use for the topology stream.
const ShardCount = 4

type MessageWithKey struct {
	Key     *topostreamv1.TopologyStreamMessageKey
	Message *topostreamv1.TopologyStreamMessage
}

func OutputToMessageWithKey(
	output topostreamv1.ComponentOrRelation,
	mapping settings.SettingExtension,
	collectionTimestampMs int64,
	toComponents func() []*topostreamv1.TopologyStreamComponent,
	toRelations func() []*topostreamv1.TopologyStreamRelation,
) *MessageWithKey {
	return &MessageWithKey{
		Key: &topostreamv1.TopologyStreamMessageKey{
			Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: mapping.GetIdentifier(),
			ShardId:    stableShardID(output.GetExternalId(), ShardCount),
		},
		Message: &topostreamv1.TopologyStreamMessage{
			CollectionTimestamp: collectionTimestampMs,
			SubmittedTimestamp:  time.Now().UnixMilli(),
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
					ExpiryIntervalMs: mapping.GetExpireAfterMs(),
					Components:       toComponents(),
					Relations:        toRelations(),
				},
			},
		},
	}
}

func ErrorsToMessageWithKey(
	errs *[]error,
	mapping settings.SettingExtension,
	collectionTimestampMs int64,
) *MessageWithKey {
	streamErrors := make([]*topostreamv1.TopoStreamError, len(*errs))
	for i, err := range *errs {
		streamErrors[i] = &topostreamv1.TopoStreamError{
			Message: err.Error(),
		}
	}
	return &MessageWithKey{
		Key: &topostreamv1.TopologyStreamMessageKey{
			Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: mapping.GetIdentifier(),
			ShardId:    "unknown",
		},
		Message: &topostreamv1.TopologyStreamMessage{
			CollectionTimestamp: collectionTimestampMs,
			SubmittedTimestamp:  time.Now().UnixMilli(),
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
					ExpiryIntervalMs: mapping.GetExpireAfterMs(),
					Errors:           streamErrors,
				},
			},
		},
	}
}

func RemovalToMessageWithKey(
	mapping settings.SettingExtension,
) []MessageWithKey {
	messages := make([]MessageWithKey, 0)
	now := time.Now().UnixMilli()
	for shard := 0; shard < ShardCount; shard++ {
		message := &MessageWithKey{
			Key: &topostreamv1.TopologyStreamMessageKey{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: mapping.GetIdentifier(),
				ShardId:    fmt.Sprintf("%d", shard),
			},
			Message: &topostreamv1.TopologyStreamMessage{
				CollectionTimestamp: now,
				SubmittedTimestamp:  now,
				Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRemove{
					TopologyStreamRemove: &topostreamv1.TopologyStreamRemove{
						RemovalCause: fmt.Sprintf("Setting with identifier '%s' was removed'", mapping.GetIdentifier()),
					},
				},
			},
		}
		messages = append(messages, *message)
	}
	return messages
}

func stableShardID(shardKey string, shardCount uint32) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(shardKey))
	return fmt.Sprintf("%d", h.Sum32()%shardCount)
}
