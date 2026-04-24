package k8scrdreceiver

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// PeerSyncSnapshot is the protocol payload exchanged between replicas.
type PeerSyncSnapshot struct {
	// Cache is the last-emitted resource state.
	Cache *resourceCache `json:"cache"`

	// LastSnapshotTime the time at which the previous leader emitted
	// its most recent full snapshot. A new leader uses this to schedule its next
	// snapshot relative to the cluster's history rather than restarting the timer
	// from "now" — preventing a snapshot gap that exceeds the configured interval.
	LastSnapshotTime time.Time `json:"last_snapshot_time"`
}

// streamFrameType discriminates the frames in a peerSyncStream snapshot transfer.
type streamFrameType string

const (
	streamFrameMeta streamFrameType = "meta"
	streamFrameCRD  streamFrameType = "crd"
	streamFrameCR   streamFrameType = "cr"
)

// peerSyncStreamFrame is one record in the newline-delimited JSON snapshot format.
// The leader emits one frame per entry; the client reads frames one at a time and
// applies them to a fresh cache. This avoids ever holding the entire serialized or
// deserialized snapshot in memory at once.
type peerSyncStreamFrame struct {
	Type streamFrameType `json:"type"`

	// Meta frame fields.
	LastSnapshotTime time.Time `json:"last_snapshot_time,omitempty"`

	// CRD/CR frame fields.
	Key string                       `json:"key,omitempty"`
	Obj *unstructured.Unstructured   `json:"obj,omitempty"`
	GVR *schema.GroupVersionResource `json:"gvr,omitempty"`
}

// PeerSyncDelta is the wire format for a leader→peer push of per-cycle changes.
type PeerSyncDelta struct {
	// AppliedAt is the leader's wall-clock time when this delta was generated.
	// Secondaries use it to discard buffered deltas that predate their snapshot.
	AppliedAt time.Time `json:"applied_at"`

	// LastSnapshotTime carries snapshot-timing continuity across leader changes.
	LastSnapshotTime time.Time `json:"last_snapshot_time"`

	// Changes is the list of resource changes (adds/mods/deletes) for this cycle.
	Changes []ResourceChange `json:"changes"`
}

func marshalPeerSyncDelta(delta *PeerSyncDelta) ([]byte, error) {
	if delta == nil {
		return nil, fmt.Errorf("marshalPeerSyncDelta: delta is nil")
	}
	return json.Marshal(delta)
}

func unmarshalPeerSyncDelta(data []byte) (*PeerSyncDelta, error) {
	var delta PeerSyncDelta
	if err := json.Unmarshal(data, &delta); err != nil {
		return nil, fmt.Errorf("unmarshal peer sync delta: %w", err)
	}
	return &delta, nil
}

// PeerStore owns the synchronised resource cache and the peer-sync transport.
type PeerStore interface {
	// Lifecycle.
	Start(ctx context.Context) error
	Stop()

	// Bootstrap pulls a snapshot from the leader (if any) and applies it to the
	// local cache. Idempotent; subsequent calls are no-ops once ready.
	Bootstrap(ctx context.Context) error

	// SetLeader controls whether this node serves snapshot pulls.
	SetLeader(leader bool)

	// ComputeChanges returns the delta between the cache and the given current
	// informer state. Acquires a read lock internally.
	ComputeChanges(
		currentCRDs []*unstructured.Unstructured,
		currentCRs map[schema.GroupVersionResource][]*unstructured.Unstructured,
	) []ResourceChange

	// ApplyDelta atomically applies the changes to the cache (under write lock)
	// and broadcasts the delta to peers. Updates LastSnapshotTime if set on the
	// delta. The leader calls this after computing changes; the HTTP handler
	// uses it internally for received pushes.
	ApplyDelta(ctx context.Context, delta *PeerSyncDelta) error

	// IsEmpty returns true if the cache contains no CRDs or CRs.
	IsEmpty() bool

	// LastSnapshotTime returns the leader's wall-clock at the most recently applied
	// snapshot. Used by the leader's snapshot scheduler for continuity across failovers.
	LastSnapshotTime() time.Time
}

// noopPeerStore is the no-broadcast implementation: no HTTP server, no peer push,
// no bootstrap pull. It still owns a real resourceCache so the collector can
// compute changes and apply deltas locally exactly as it would with peer sync
// enabled — only the cross-replica side is suppressed.
type noopPeerStore struct {
	mu               sync.RWMutex
	cache            *resourceCache
	lastSnapshotTime time.Time
}

var _ PeerStore = (*noopPeerStore)(nil)

func (n *noopPeerStore) Start(_ context.Context) error     { return nil }
func (n *noopPeerStore) Stop()                             {}
func (n *noopPeerStore) Bootstrap(_ context.Context) error { return nil }
func (n *noopPeerStore) SetLeader(_ bool)                  {}

func (n *noopPeerStore) cacheRef() *resourceCache {
	if n.cache == nil {
		n.cache = newResourceCache()
	}
	return n.cache
}

func (n *noopPeerStore) ComputeChanges(
	currentCRDs []*unstructured.Unstructured,
	currentCRs map[schema.GroupVersionResource][]*unstructured.Unstructured,
) []ResourceChange {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.cacheRef().computeChanges(currentCRDs, currentCRs)
}

func (n *noopPeerStore) ApplyDelta(_ context.Context, delta *PeerSyncDelta) error {
	if delta == nil {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cacheRef().applyDelta(delta.Changes)
	if !delta.LastSnapshotTime.IsZero() {
		n.lastSnapshotTime = delta.LastSnapshotTime
	}
	return nil
}

func (n *noopPeerStore) IsEmpty() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.cacheRef().isEmpty()
}

func (n *noopPeerStore) LastSnapshotTime() time.Time {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastSnapshotTime
}
