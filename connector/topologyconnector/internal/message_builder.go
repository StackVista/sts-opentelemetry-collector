package internal

import (
	"fmt"
	"hash/fnv"
	"time"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"google.golang.org/protobuf/proto"
)

// ShardCount is the number of shards to use for the topology stream.
const (
	ShardCount = 4
	// UnknownShardID is the shard placeholder used for messages that are not tied to a concrete
	// element shard (e.g. mapping-level error envelopes). It is not a real topology stream shard.
	UnknownShardID = "unknown"
)

type MessageWithKey struct {
	Key     *topostreamv1.TopologyStreamMessageKey
	Message *topostreamv1.TopologyStreamMessage
}

func OutputToMessageWithKey(
	output topostreamv1.ComponentOrRelation,
	mapping settingsproto.SettingExtension,
	collectionTimestampMs int64,
	toComponents func() []*topostreamv1.TopologyStreamComponent,
	toRelations func() []*topostreamv1.TopologyStreamRelation,
) *MessageWithKey {
	return &MessageWithKey{
		Key: &topostreamv1.TopologyStreamMessageKey{
			Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: mapping.GetIdentifier(),
			ShardId:    stableShardID(output.GetExternalId()),
		},
		Message: &topostreamv1.TopologyStreamMessage{
			CollectionTimestamp: collectionTimestampMs,
			SubmittedTimestamp:  time.Now().UnixMilli(),
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
					ExpiryIntervalMs: mapping.GetExpireAfterMs(),
					Specificity:      mapping.GetSpecificity(),
					Components:       toComponents(),
					Relations:        toRelations(),
				},
			},
		},
	}
}

func ErrorsToMessageWithKey(
	errs *[]error,
	mapping settingsproto.SettingExtension,
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
			ShardId:    UnknownShardID,
		},
		Message: &topostreamv1.TopologyStreamMessage{
			CollectionTimestamp: collectionTimestampMs,
			SubmittedTimestamp:  time.Now().UnixMilli(),
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
					ExpiryIntervalMs: mapping.GetExpireAfterMs(),
					Specificity:      mapping.GetSpecificity(),
					Errors:           streamErrors,
				},
			},
		},
	}
}

func RemovalToMessageWithKey(
	mapping settingsproto.SettingExtension,
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

func ComponentDeleteToMessageWithKey(
	externalID string,
	mapping settingsproto.SettingExtension,
	collectionTimestampMs int64,
) *MessageWithKey {
	return &MessageWithKey{
		Key: &topostreamv1.TopologyStreamMessageKey{
			Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: mapping.GetIdentifier(),
			ShardId:    stableShardID(externalID),
		},
		Message: &topostreamv1.TopologyStreamMessage{
			CollectionTimestamp: collectionTimestampMs,
			SubmittedTimestamp:  time.Now().UnixMilli(),
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
					ExpiryIntervalMs:           mapping.GetExpireAfterMs(),
					Specificity:                mapping.GetSpecificity(),
					DeleteComponentExternalIds: []string{externalID},
				},
			},
		},
	}
}

func RelationDeleteToMessageWithKey(
	externalID string,
	mapping settingsproto.SettingExtension,
	collectionTimestampMs int64,
) *MessageWithKey {
	return &MessageWithKey{
		Key: &topostreamv1.TopologyStreamMessageKey{
			Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: mapping.GetIdentifier(),
			ShardId:    stableShardID(externalID),
		},
		Message: &topostreamv1.TopologyStreamMessage{
			CollectionTimestamp: collectionTimestampMs,
			SubmittedTimestamp:  time.Now().UnixMilli(),
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
					ExpiryIntervalMs:          mapping.GetExpireAfterMs(),
					Specificity:               mapping.GetSpecificity(),
					DeleteRelationExternalIds: []string{externalID},
				},
			},
		},
	}
}

// maxGroupedMessageBytes caps the marshaled size of a single grouped envelope. Each envelope becomes one Kafka
// record, which is bounded by the broker's message.max.bytes (typically ~1 MiB), so an unbounded group could
// produce a record that the producer rejects (and, with acks=none, silently drops). We bound on bytes rather than
// element count because element sizes vary enormously (fat resource_definition/status_data payloads), and such fat
// elements are likely to be collected together and so land in the same group — an element count would be a poor
// proxy for the wire size that actually matters. When adding an element would exceed this budget a fresh envelope
// is started for the same key+timestamp. Splitting is safe: the platform gate is keyed per externalId and treats
// equal timestamps as non-stale, so distinct elements spread across envelopes are all applied. The value is
// intentionally conservative relative to the 1 MiB limit to leave headroom for the key and Kafka framing.
const maxGroupedMessageBytes = 768 * 1024

// repeatPayloadOf returns the RepeatElements payload of a message, or nil if the message carries a different
// payload (e.g. a TopologyStreamRemove). Uses a checked type assertion so a non-matching payload is handled
// gracefully rather than panicking.
func repeatPayloadOf(m *topostreamv1.TopologyStreamMessage) *topostreamv1.TopologyStreamRepeatElementsData {
	wrapper, ok := m.GetPayload().(*topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData)
	if !ok {
		return nil
	}
	return wrapper.TopologyStreamRepeatElementsData
}

// mergeRepeatElements appends all of src's elements into dst. ExpiryIntervalMs and Specificity are per-mapping
// (dataSource) and identical within a group, so dst's existing values are kept.
func mergeRepeatElements(dst, src *topostreamv1.TopologyStreamRepeatElementsData) {
	dst.Components = append(dst.Components, src.Components...)
	dst.Relations = append(dst.Relations, src.Relations...)
	dst.DeleteComponentExternalIds = append(dst.DeleteComponentExternalIds, src.DeleteComponentExternalIds...)
	dst.DeleteRelationExternalIds = append(dst.DeleteRelationExternalIds, src.DeleteRelationExternalIds...)
	dst.Errors = append(dst.Errors, src.Errors...)
}

// groupMessagesByKeyAndTimestamp coalesces single-element TopologyStreamRepeatElementsData messages that share
// the same partition key (owner, dataSource, shardId) AND the same collection_timestamp into one message. Because
// each element already carries its own collection time as the message timestamp, grouping by timestamp keeps that
// per-element fidelity while reducing the number of messages on the wire.
//
// Only RepeatElements payloads are merged. TopologyStreamRemove and any non-RepeatElements payloads are passed
// through untouched and never merged. Grouping is never done across shardId or across differing collection_timestamp,
// which would violate the partition-ordering guarantee or the per-element timestamp respectively.
//
// A single grouped envelope is capped at maxGroupedMessageBytes; overflow for the same key+timestamp spills into
// additional envelopes so no single Kafka record grows unbounded.
//
// Output order is deterministic: groups appear in the order their first message was seen in the input.
func groupMessagesByKeyAndTimestamp(messages []MessageWithKey) []MessageWithKey {
	if len(messages) <= 1 {
		return messages
	}

	type groupKey struct {
		owner               topostreamv1.TopologyStreamOwner
		dataSource          string
		shardID             string
		collectionTimestamp int64
	}
	// group tracks the position of the CURRENT (not-yet-full) accumulating message for a key, plus a running
	// estimate of its marshaled size so we don't re-marshal the whole envelope on every merge.
	type group struct {
		pos   int
		bytes int
	}

	result := make([]MessageWithKey, 0, len(messages))
	index := make(map[groupKey]group)

	for _, mwk := range messages {
		incoming := repeatPayloadOf(mwk.Message)
		if incoming == nil {
			// Pass through removes, errors-only-with-other-payloads, etc. without merging.
			result = append(result, mwk)
			continue
		}

		key := groupKey{
			owner:               mwk.Key.GetOwner(),
			dataSource:          mwk.Key.GetDataSource(),
			shardID:             mwk.Key.GetShardId(),
			collectionTimestamp: mwk.Message.GetCollectionTimestamp(),
		}
		incomingBytes := proto.Size(mwk.Message)

		if g, seen := index[key]; seen && g.bytes+incomingBytes <= maxGroupedMessageBytes {
			// Merge this message's elements into the already-accumulating message for this group.
			mergeRepeatElements(repeatPayloadOf(result[g.pos].Message), incoming)
			index[key] = group{pos: g.pos, bytes: g.bytes + incomingBytes}
			continue
		}

		// First message for this key, or the current envelope would overflow: start a fresh envelope.
		index[key] = group{pos: len(result), bytes: incomingBytes}
		result = append(result, mwk)
	}

	return result
}

func stableShardID(shardKey string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(shardKey))
	return fmt.Sprintf("%d", h.Sum32()%ShardCount)
}
