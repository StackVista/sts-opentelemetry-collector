package stsk8stopologyexporter //nolint:testpackage // Exercises the internal object store.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func key(name string) objectKey {
	return objectKey{kind: "Pod", namespace: "default", name: name}
}

func obj(name string) map[string]any {
	return map[string]any{"metadata": map[string]any{"name": name}}
}

func TestStoreReadyOnlyAfterCompleteSnapshot(t *testing.T) {
	now := time.Now()
	s := newObjectStore()
	s.upsert(key("a"), "v1", obj("a"))
	_, _, ok := s.view(now, time.Hour)
	assert.False(t, ok, "objects before any snapshot are not a complete view")

	s.startSnapshot("1")
	s.upsert(key("a"), "v1", obj("a"))
	assert.False(t, s.endSnapshot("1", false, now), "incomplete snapshot")
	_, _, ok = s.view(now, time.Hour)
	assert.False(t, ok)

	s.startSnapshot("2")
	s.upsert(key("a"), "v1", obj("a"))
	assert.True(t, s.endSnapshot("2", true, now))
	view, _, ok := s.view(now, time.Hour)
	require.True(t, ok)
	assert.Len(t, view, 1)
}

func TestStoreSnapshotDropsObjectsNotReemitted(t *testing.T) {
	now := time.Now()
	s := newObjectStore()
	s.startSnapshot("1")
	s.upsert(key("a"), "v1", obj("a"))
	s.upsert(key("b"), "v1", obj("b"))
	require.True(t, s.endSnapshot("1", true, now))

	s.startSnapshot("2")
	s.upsert(key("a"), "v1", obj("a"))
	require.True(t, s.endSnapshot("2", true, now))

	view, _, ok := s.view(now, time.Hour)
	require.True(t, ok)
	assert.Contains(t, view, key("a"))
	assert.NotContains(t, view, key("b"))
}

func TestStoreAppliesIncrementsBetweenSnapshots(t *testing.T) {
	now := time.Now()
	s := newObjectStore()
	s.startSnapshot("1")
	s.upsert(key("a"), "v1", obj("a"))
	require.True(t, s.endSnapshot("1", true, now))

	s.upsert(key("b"), "v1", obj("b"))
	s.remove(key("a"))
	view, _, ok := s.view(now, time.Hour)
	require.True(t, ok)
	assert.Equal(t, []objectKey{key("b")}, keys(view))
}

func TestStoreIgnoresEndWithoutMatchingStart(t *testing.T) {
	now := time.Now()
	s := newObjectStore()
	s.upsert(key("a"), "v1", obj("a"))
	assert.False(t, s.endSnapshot("1", true, now))

	s.startSnapshot("2")
	assert.False(t, s.endSnapshot("3", true, now))
	_, _, ok := s.view(now, time.Hour)
	assert.False(t, ok)
}

func TestStoreResetAndStaleness(t *testing.T) {
	now := time.Now()
	s := newObjectStore()
	s.startSnapshot("1")
	s.upsert(key("a"), "v1", obj("a"))
	require.True(t, s.endSnapshot("1", true, now))

	_, _, ok := s.view(now.Add(2*time.Minute), time.Minute)
	assert.False(t, ok, "a snapshot older than the maximum age is not sent")

	s.reset()
	_, _, ok = s.view(now, time.Hour)
	assert.False(t, ok, "reset pauses sending until the next complete snapshot")
}

func keys(view map[objectKey]storedObject) []objectKey {
	out := make([]objectKey, 0, len(view))
	for k := range view {
		out = append(out, k)
	}
	return out
}
