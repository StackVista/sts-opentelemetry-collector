// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"encoding/hex"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const clientService = "client"

func TestStoreUpsertEdge(t *testing.T) {
	key := NewKey(pcommon.TraceID([16]byte{1, 2, 3}), pcommon.SpanID([8]byte{1, 2, 3}))

	var onCompletedCount int
	var onExpireCount int
	var onRescheduleCount int

	s := NewStore(time.Hour, 1, countingCompleteCallback(&onCompletedCount), countingCallback(&onExpireCount), countingCallback(&onRescheduleCount))
	assert.Equal(t, 0, s.len())

	// Insert first half of an edge
	isNew, err := s.UpsertEdge(key, func(e *Edge) {
		e.ClientService = clientService
	})
	require.NoError(t, err)
	require.Equal(t, true, isNew)
	assert.Equal(t, 1, s.len())

	// Nothing should be evicted as TTL is set to 1h
	assert.False(t, s.tryEvictHead(time.Now()))
	assert.Equal(t, 0, onCompletedCount)
	assert.Equal(t, 0, onExpireCount)

	// Insert the second half of an edge
	isNew, err = s.UpsertEdge(key, func(e *Edge) {
		assert.Equal(t, clientService, e.ClientService)
		e.ServerService = "server"
	})
	require.NoError(t, err)
	require.Equal(t, false, isNew)
	// Edge is complete and should have been removed
	assert.Equal(t, 0, s.len())

	assert.Equal(t, 1, onCompletedCount)
	assert.Equal(t, 0, onExpireCount)

	// Insert an edge that will immediately expire
	isNew, err = s.UpsertEdge(key, func(e *Edge) {
		e.ClientService = clientService
		e.expiration = time.UnixMicro(0)
	})
	require.NoError(t, err)
	require.Equal(t, true, isNew)
	assert.Equal(t, 1, s.len())
	assert.Equal(t, 1, onCompletedCount)
	assert.Equal(t, 0, onExpireCount)

	assert.True(t, s.tryEvictHead(time.Now()))
	assert.Equal(t, 0, s.len())
	assert.Equal(t, 1, onCompletedCount)
	assert.Equal(t, 1, onExpireCount)
}

func TestStoreUpsertEdge_errTooManyItems(t *testing.T) {
	key1 := NewKey(pcommon.TraceID([16]byte{1, 2, 3}), pcommon.SpanID([8]byte{1, 2, 3}))
	key2 := NewKey(pcommon.TraceID([16]byte{4, 5, 6}), pcommon.SpanID([8]byte{1, 2, 3}))
	var onCallbackCounter int

	s := NewStore(time.Hour, 1, countingCompleteCallback(&onCallbackCounter), countingCallback(&onCallbackCounter), countingCallback(&onCallbackCounter))
	assert.Equal(t, 0, s.len())

	isNew, err := s.UpsertEdge(key1, func(e *Edge) {
		e.ClientService = clientService
	})
	require.NoError(t, err)
	require.Equal(t, true, isNew)
	assert.Equal(t, 1, s.len())

	_, err = s.UpsertEdge(key2, func(e *Edge) {
		e.ClientService = clientService
	})
	require.ErrorIs(t, err, ErrTooManyItems)
	assert.Equal(t, 1, s.len())

	isNew, err = s.UpsertEdge(key1, func(e *Edge) {
		e.ClientService = clientService
	})
	require.NoError(t, err)
	require.Equal(t, false, isNew)
	assert.Equal(t, 1, s.len())

	assert.Equal(t, 0, onCallbackCounter)
}

func TestStoreExpire(t *testing.T) {
	const testSize = 1000

	keys := map[Key]struct{}{}
	for i := 0; i < testSize; i++ {
		keys[NewKey(pcommon.TraceID([16]byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}), pcommon.SpanID([8]byte{1, 2, 3}))] = struct{}{}
	}

	var onCompletedCount int
	var onExpireCount int
	var onRescheduleCount int

	onComplete := func(e *Edge, logp float64) {
		onCompletedCount++
		assert.Contains(t, keys, e.Key)
		assert.Equal(t, 0.0, logp)
	}
	s := NewStore(time.Millisecond, testSize, onComplete, countingCallback(&onExpireCount), countingCallback(&onRescheduleCount))

	for key := range keys {
		isNew, err := s.UpsertEdge(key, noopCallback)
		require.NoError(t, err)
		require.Equal(t, true, isNew)
	}

	s.Expire(time.Now().Add(2 * time.Millisecond))
	assert.Equal(t, 0, onCompletedCount)
	assert.Equal(t, testSize, onExpireCount+onRescheduleCount)
	assert.Equal(t, onRescheduleCount, s.len())

	// expected number of rescheduled items is 0.368 * testSize (for large testSize)
	assert.Less(t, testSize/3, onRescheduleCount)
	assert.Greater(t, 2*testSize/5, onRescheduleCount)
}

func TestStoreConcurrency(t *testing.T) {
	s := NewStore(10*time.Millisecond, 100000, noopCompleteCallback, noopCallback, noopCallback)

	end := make(chan struct{})

	accessor := func(f func()) {
		for {
			select {
			case <-end:
				return
			default:
				f()
			}
		}
	}

	go accessor(func() {
		key := NewKey(pcommon.TraceID([16]byte{byte(rand.Intn(32))}), pcommon.SpanID([8]byte{1, 2, 3}))

		_, err := s.UpsertEdge(key, func(e *Edge) {
			e.ClientService = hex.EncodeToString(key.tid[:])
		})
		assert.NoError(t, err)
	})

	go accessor(func() {
		s.Expire(time.Now())
	})

	time.Sleep(100 * time.Millisecond)
	close(end)
}

func noopCallback(_ *Edge)                    {}
func noopCompleteCallback(_ *Edge, _ float64) {}

func countingCallback(counter *int) func(*Edge) {
	return func(_ *Edge) {
		*counter++
	}
}

func countingCompleteCallback(counter *int) func(*Edge, float64) {
	return func(_ *Edge, _ float64) {
		*counter++
	}
}
