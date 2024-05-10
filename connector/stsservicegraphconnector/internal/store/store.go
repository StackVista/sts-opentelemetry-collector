// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package store // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/servicegraphconnector/internal/store"

import (
	"container/list"
	"errors"
	"hash/fnv"
	"math"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

var (
	ErrTooManyItems = errors.New("too many items")
)

type CompleteCallback func(e *Edge, logp float64)
type UpdateCallback func(e *Edge)
type ExpireCallback func(e *Edge)

type Key struct {
	tid pcommon.TraceID
	sid pcommon.SpanID
}

func (k *Key) SpanIDIsEmpty() bool {
	return k.sid.IsEmpty()
}

func NewKey(tid pcommon.TraceID, sid pcommon.SpanID) Key {
	return Key{tid: tid, sid: sid}
}

type Store struct {
	l   *list.List
	mtx sync.Mutex
	m   map[Key]*list.Element

	onComplete CompleteCallback
	onExpire   ExpireCallback

	ttl      time.Duration
	maxItems int
}

// NewStore creates a Store to build service graphs. The store caches edges, each representing a
// request between two services. Once an edge is complete its metrics can be collected. Edges that
// have not found their pair are deleted after ttl time.
func NewStore(ttl time.Duration, maxItems int, onComplete CompleteCallback, onExpire ExpireCallback) *Store {
	s := &Store{
		l: list.New(),
		m: make(map[Key]*list.Element),

		onComplete: onComplete,
		onExpire:   onExpire,

		ttl:      ttl,
		maxItems: maxItems,
	}

	return s
}

// len is only used for testing.
func (s *Store) len() int {
	return s.l.Len()
}

// UpsertEdge fetches an Edge from the store and updates it using the given callback. If the Edge
// doesn't exist yet, it creates a new one with the default TTL.
// If the Edge is complete after applying the callback, it's completed and removed.
func (s *Store) UpsertEdge(key Key, update UpdateCallback) (isNew bool, err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if storedEdge, ok := s.m[key]; ok {
		edge := storedEdge.Value.(*Edge)
		update(edge)

		if edge.isComplete() {
			s.onComplete(edge, edge.logp)
			delete(s.m, key)
			s.l.Remove(storedEdge)
		}

		return false, nil
	}

	edge := newEdge(key, s.ttl)
	update(edge)

	if edge.isComplete() {
		s.onComplete(edge, 0.0)
		return true, nil
	}

	// Check we can add new edges
	if s.l.Len() >= s.maxItems {
		// TODO: try to evict expired items
		return false, ErrTooManyItems
	}

	ele := s.l.PushBack(edge)
	s.m[key] = ele

	return true, nil
}

// Expire evicts all expired items in the store.
func (s *Store) Expire() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// Iterates until no more items can be evicted
	for s.tryEvictHead() { // nolint
	}
}

// tryEvictHead checks if the oldest item (head of list) can be evicted and will delete it if so.
// Returns true if the head was evicted.
//
// Must be called holding lock.
func (s *Store) tryEvictHead() bool {
	head := s.l.Front()
	if head == nil {
		return false // list is empty
	}

	headEdge := head.Value.(*Edge)
	if !headEdge.isExpired() {
		return false
	}

	// expire edges when there is memory pressure, but retain them when there is no pressure.
	// In the extreme case (Len == maxItems), no expired edges are re-added.
	// When the list is nearly empty, all edges are retained.  When the list is half-filled,
	// about half of the expired edges are added again.
	h := fnv.New32a()
	h.Write(headEdge.TraceID[:])
	h.Write([]byte(strconv.Itoa(headEdge.generation)))
	hash := h.Sum32()
	if hash%uint32(s.maxItems) < uint32(s.l.Len()) {
		s.onExpire(headEdge)
		delete(s.m, headEdge.Key)
		s.l.Remove(head)
	} else {
		headEdge.expiration = time.Now().Add(s.ttl)
		headEdge.generation++
		// update weight of edge to compensate for expiration
		// this keeps the metrics (in expectation) correct
		headEdge.logp += math.Log(1.0 - float64(s.l.Len()-1)/float64(s.maxItems))
		s.l.MoveToBack(head)
	}

	return true
}
