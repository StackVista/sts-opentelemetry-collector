package stsk8stopologyexporter

import (
	"sync"
	"time"
)

type objectKey struct {
	group, kind, namespace, name string
}

type storedObject struct {
	version string
	object  map[string]any
}

// objectStore mirrors the Cluster Observer's object stream. It becomes ready
// only after a complete, bracketed snapshot, because the platform deletes every
// element missing from a topology snapshot.
type objectStore struct {
	mu           sync.Mutex
	objects      map[objectKey]storedObject
	snapshotID   string
	seen         map[objectKey]struct{}
	ready        bool
	lastComplete time.Time
}

func newObjectStore() *objectStore {
	return &objectStore{objects: map[objectKey]storedObject{}}
}

func (s *objectStore) upsert(key objectKey, version string, object map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = storedObject{version: version, object: object}
	if s.seen != nil {
		s.seen[key] = struct{}{}
	}
}

func (s *objectStore) remove(key objectKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	if s.seen != nil {
		delete(s.seen, key)
	}
}

func (s *objectStore) startSnapshot(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshotID = id
	s.seen = map[objectKey]struct{}{}
}

// endSnapshot drops objects not re-emitted during the snapshot and reports
// whether the store is ready. An end without its matching start is ignored.
func (s *objectStore) endSnapshot(id string, complete bool, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil || id != s.snapshotID {
		return s.ready
	}
	for key := range s.objects {
		if _, ok := s.seen[key]; !ok {
			delete(s.objects, key)
		}
	}
	s.seen = nil
	s.snapshotID = ""
	s.ready = complete
	if complete {
		s.lastComplete = now
	}
	return s.ready
}

func (s *objectStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects = map[objectKey]storedObject{}
	s.seen = nil
	s.snapshotID = ""
	s.ready = false
}

// view returns a copy of the objects when the store holds a complete snapshot
// no older than maxAge.
func (s *objectStore) view(now time.Time, maxAge time.Duration) (map[objectKey]storedObject, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready || now.Sub(s.lastComplete) > maxAge {
		return nil, false
	}
	out := make(map[objectKey]storedObject, len(s.objects))
	for key, value := range s.objects {
		out[key] = value
	}
	return out, true
}
