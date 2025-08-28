// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"crypto/rand"
	"encoding/hex"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/connector/stsservicegraphconnector/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const clientService = "client"

func TestStoreUpsertEdge(t *testing.T) {
	key := store.NewKey(pcommon.TraceID([16]byte{1, 2, 3}), pcommon.SpanID([8]byte{1, 2, 3}))

	var onCompletedCount int
	var onExpireCount int
	var onRescheduleCount int

	s := store.NewStore(
		time.Hour,
		1,
		countingCompleteCallback(&onCompletedCount),
		countingCallback(&onExpireCount),
		countingCallback(&onRescheduleCount),
	)
	assert.Equal(t, 0, s.Len())

	// Insert first half of an edge
	isNew, err := s.UpsertEdge(key, func(e *store.Edge) {
		e.ClientService = clientService
	})
	require.NoError(t, err)
	require.Equal(t, true, isNew)
	assert.Equal(t, 1, s.Len())

	// Nothing should be evicted as TTL is set to 1h
	assert.False(t, s.TryEvictHead(time.Now()))
	assert.Equal(t, 0, onCompletedCount)
	assert.Equal(t, 0, onExpireCount)

	// Insert the second half of an edge
	isNew, err = s.UpsertEdge(key, func(e *store.Edge) {
		assert.Equal(t, clientService, e.ClientService)
		e.ServerService = "server"
	})
	require.NoError(t, err)
	require.Equal(t, false, isNew)
	// Edge is complete and should have been removed
	assert.Equal(t, 0, s.Len())

	assert.Equal(t, 1, onCompletedCount)
	assert.Equal(t, 0, onExpireCount)

	// Insert an edge that will immediately expire
	isNew, err = s.UpsertEdge(key, func(e *store.Edge) {
		e.ClientService = clientService
		e.SetExpiration(time.UnixMicro(0))
	})
	require.NoError(t, err)
	require.Equal(t, true, isNew)
	assert.Equal(t, 1, s.Len())
	assert.Equal(t, 1, onCompletedCount)
	assert.Equal(t, 0, onExpireCount)

	assert.True(t, s.TryEvictHead(time.Now()))
	assert.Equal(t, 0, s.Len())
	assert.Equal(t, 1, onCompletedCount)
	assert.Equal(t, 1, onExpireCount)
}

func TestStoreUpsertEdge_errTooManyItems(t *testing.T) {
	key1 := store.NewKey(pcommon.TraceID([16]byte{1, 2, 3}), pcommon.SpanID([8]byte{1, 2, 3}))
	key2 := store.NewKey(pcommon.TraceID([16]byte{4, 5, 6}), pcommon.SpanID([8]byte{1, 2, 3}))
	var onCallbackCounter int

	s := store.NewStore(
		time.Hour,
		1,
		countingCompleteCallback(&onCallbackCounter),
		countingCallback(&onCallbackCounter),
		countingCallback(&onCallbackCounter),
	)
	assert.Equal(t, 0, s.Len())

	isNew, err := s.UpsertEdge(key1, func(e *store.Edge) {
		e.ClientService = clientService
	})
	require.NoError(t, err)
	require.Equal(t, true, isNew)
	assert.Equal(t, 1, s.Len())

	_, err = s.UpsertEdge(key2, func(e *store.Edge) {
		e.ClientService = clientService
	})
	require.ErrorIs(t, err, store.ErrTooManyItems)
	assert.Equal(t, 1, s.Len())

	isNew, err = s.UpsertEdge(key1, func(e *store.Edge) {
		e.ClientService = clientService
	})
	require.NoError(t, err)
	require.Equal(t, false, isNew)
	assert.Equal(t, 1, s.Len())

	assert.Equal(t, 0, onCallbackCounter)
}

func TestStoreExpire(t *testing.T) {
	const testSize = 1000

	keys := map[store.Key]struct{}{}
	for i := 0; i < testSize; i++ {
		keys[store.NewKey(
			pcommon.TraceID([16]byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}),
			pcommon.SpanID([8]byte{1, 2, 3}),
		)] = struct{}{}
	}

	var onCompletedCount int
	var onExpireCount int
	var onRescheduleCount int

	onComplete := func(e *store.Edge, logp float64) {
		onCompletedCount++
		assert.Contains(t, keys, e.Key)
		assert.Equal(t, 0.0, logp)
	}
	s := store.NewStore(
		time.Millisecond,
		testSize,
		onComplete,
		countingCallback(&onExpireCount),
		countingCallback(&onRescheduleCount),
	)

	for key := range keys {
		isNew, err := s.UpsertEdge(key, noopCallback)
		require.NoError(t, err)
		require.Equal(t, true, isNew)
	}

	s.Expire(time.Now().Add(2 * time.Millisecond))
	assert.Equal(t, 0, onCompletedCount)
	assert.Equal(t, testSize, onExpireCount+onRescheduleCount)
	assert.Equal(t, onRescheduleCount, s.Len())

	// expected number of rescheduled items is 0.368 * testSize (for large testSize)
	assert.Less(t, testSize/3, onRescheduleCount)
	assert.Greater(t, 2*testSize/5, onRescheduleCount)
}

func TestStoreConcurrency(t *testing.T) {
	s := store.NewStore(10*time.Millisecond, 100000, noopCompleteCallback, noopCallback, noopCallback)

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
		randInt, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt32))
		assert.NoError(t, err)
		randInt64 := randInt.Int64()
		var randInt32 int32

		if randInt64 > math.MaxInt32 && randInt64 < math.MinInt32 {
			randInt32 = int32(randInt64)
		}

		key := store.NewKey(pcommon.TraceID([16]byte{byte(randInt32)}), pcommon.SpanID([8]byte{1, 2, 3}))

		_, err = s.UpsertEdge(key, func(e *store.Edge) {
			tid := key.GetTraceID()
			e.ClientService = hex.EncodeToString(tid[:])
		})
		assert.NoError(t, err)
	})

	go accessor(func() {
		s.Expire(time.Now())
	})

	time.Sleep(100 * time.Millisecond)
	close(end)
}

func noopCallback(_ *store.Edge)                    {}
func noopCompleteCallback(_ *store.Edge, _ float64) {}

func countingCallback(counter *int) func(*store.Edge) {
	return func(_ *store.Edge) {
		*counter++
	}
}

func countingCompleteCallback(counter *int) func(*store.Edge, float64) {
	return func(_ *store.Edge, _ float64) {
		*counter++
	}
}
