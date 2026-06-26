//nolint:testpackage
package internal

import (
	"testing"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDataSource = "ds"

func repeatMsg(shardID string, ts int64, mutate func(d *topostreamv1.TopologyStreamRepeatElementsData)) MessageWithKey {
	d := &topostreamv1.TopologyStreamRepeatElementsData{ExpiryIntervalMs: 60000}
	mutate(d)
	return MessageWithKey{
		Key: &topostreamv1.TopologyStreamMessageKey{
			Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: testDataSource,
			ShardId:    shardID,
		},
		Message: &topostreamv1.TopologyStreamMessage{
			CollectionTimestamp: ts,
			SubmittedTimestamp:  1,
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: d,
			},
		},
	}
}

func withComponent(id string) func(d *topostreamv1.TopologyStreamRepeatElementsData) {
	return func(d *topostreamv1.TopologyStreamRepeatElementsData) {
		d.Components = append(d.Components, &topostreamv1.TopologyStreamComponent{ExternalId: id})
	}
}

func repeatData(m MessageWithKey) *topostreamv1.TopologyStreamRepeatElementsData {
	return repeatPayloadOf(m.Message)
}

func TestGroupMessagesByKeyAndTimestamp_MergesSameKeyAndTimestamp(t *testing.T) {
	in := []MessageWithKey{
		repeatMsg("0", 100, withComponent("a")),
		repeatMsg("0", 100, withComponent("b")),
	}

	out := groupMessagesByKeyAndTimestamp(in)

	require.Len(t, out, 1)
	comps := repeatData(out[0]).Components
	require.Len(t, comps, 2)
	assert.Equal(t, "a", comps[0].GetExternalId())
	assert.Equal(t, "b", comps[1].GetExternalId())
	assert.Equal(t, int64(100), out[0].Message.GetCollectionTimestamp())
}

func TestGroupMessagesByKeyAndTimestamp_DoesNotMergeAcrossShard(t *testing.T) {
	in := []MessageWithKey{
		repeatMsg("0", 100, withComponent("a")),
		repeatMsg("1", 100, withComponent("b")),
	}

	out := groupMessagesByKeyAndTimestamp(in)

	assert.Len(t, out, 2)
}

func TestGroupMessagesByKeyAndTimestamp_DoesNotMergeAcrossTimestamp(t *testing.T) {
	in := []MessageWithKey{
		repeatMsg("0", 100, withComponent("a")),
		repeatMsg("0", 200, withComponent("b")),
	}

	out := groupMessagesByKeyAndTimestamp(in)

	assert.Len(t, out, 2)
}

func TestGroupMessagesByKeyAndTimestamp_MergesDeletesAndErrors(t *testing.T) {
	in := []MessageWithKey{
		repeatMsg("0", 100, func(d *topostreamv1.TopologyStreamRepeatElementsData) {
			d.DeleteComponentExternalIds = []string{"x"}
		}),
		repeatMsg("0", 100, func(d *topostreamv1.TopologyStreamRepeatElementsData) {
			d.DeleteRelationExternalIds = []string{"y"}
			d.Errors = []*topostreamv1.TopoStreamError{{Message: "boom"}}
		}),
	}

	out := groupMessagesByKeyAndTimestamp(in)

	require.Len(t, out, 1)
	d := repeatData(out[0])
	assert.Equal(t, []string{"x"}, d.DeleteComponentExternalIds)
	assert.Equal(t, []string{"y"}, d.DeleteRelationExternalIds)
	require.Len(t, d.Errors, 1)
	assert.Equal(t, "boom", d.Errors[0].GetMessage())
}

func TestGroupMessagesByKeyAndTimestamp_PassesThroughRemovesUnmerged(t *testing.T) {
	removeMsg := func() MessageWithKey {
		return MessageWithKey{
			Key: &topostreamv1.TopologyStreamMessageKey{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: testDataSource,
				ShardId:    "0",
			},
			Message: &topostreamv1.TopologyStreamMessage{
				CollectionTimestamp: 100,
				Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRemove{
					TopologyStreamRemove: &topostreamv1.TopologyStreamRemove{RemovalCause: "gone"},
				},
			},
		}
	}
	in := []MessageWithKey{removeMsg(), removeMsg()}

	out := groupMessagesByKeyAndTimestamp(in)

	// Removes are never merged.
	assert.Len(t, out, 2)
}

func TestGroupMessagesByKeyAndTimestamp_CapsGroupSize(t *testing.T) {
	const total = maxElementsPerGroupedMessage + 50
	in := make([]MessageWithKey, 0, total)
	for i := 0; i < total; i++ {
		in = append(in, repeatMsg("0", 100, withComponent(string(rune('a'))+string(rune(i)))))
	}

	out := groupMessagesByKeyAndTimestamp(in)

	// Overflow spills into a second envelope for the same key+timestamp.
	require.Len(t, out, 2)
	assert.LessOrEqual(t, repeatElementCount(repeatData(out[0])), maxElementsPerGroupedMessage)
	assert.LessOrEqual(t, repeatElementCount(repeatData(out[1])), maxElementsPerGroupedMessage)
	assert.Equal(t, total, repeatElementCount(repeatData(out[0]))+repeatElementCount(repeatData(out[1])))
	// Both envelopes keep the same key and timestamp.
	assert.Equal(t, "0", out[1].Key.GetShardId())
	assert.Equal(t, int64(100), out[1].Message.GetCollectionTimestamp())
}

func TestGroupMessagesByKeyAndTimestamp_PreservesFirstSeenOrder(t *testing.T) {
	in := []MessageWithKey{
		repeatMsg("0", 200, withComponent("first")),
		repeatMsg("0", 100, withComponent("second")),
		repeatMsg("0", 200, withComponent("third")),
	}

	out := groupMessagesByKeyAndTimestamp(in)

	require.Len(t, out, 2)
	// ts=200 group was seen first.
	assert.Equal(t, int64(200), out[0].Message.GetCollectionTimestamp())
	assert.Equal(t, int64(100), out[1].Message.GetCollectionTimestamp())
	require.Len(t, repeatData(out[0]).Components, 2) // first + third
}
