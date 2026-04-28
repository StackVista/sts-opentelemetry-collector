package k8scrdreceiver

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// PeerSyncSnapshot is the assembled snapshot a secondary builds up from the
// streamed frames returned by /sync/snapshot.
type PeerSyncSnapshot struct {
	// Cache is the last-emitted resource state.
	Cache *resourceCache

	// LastSnapshotTime is the time at which the previous leader emitted its most
	// recent full snapshot. A new leader uses this to schedule its next snapshot
	// relative to the cluster's history rather than restarting the timer from
	// "now" — preventing a snapshot gap that exceeds the configured interval.
	LastSnapshotTime time.Time
}

// streamFrameType discriminates the frames in a peer-sync stream. Both snapshot
// pulls and delta pushes use the same frame schema and codec — the difference is
// only which fields are populated on each frame type.
type streamFrameType string

const (
	streamFrameMeta streamFrameType = "meta"
	streamFrameCRD  streamFrameType = "crd"
	streamFrameCR   streamFrameType = "cr"
)

// peerSyncStreamFrame is one record in the gzipped NDJSON wire format used by
// both /sync/snapshot (server-streamed) and /sync/increments (encode-once-buffered).
// All optional fields are tagged so unused fields don't appear on the wire.
type peerSyncStreamFrame struct {
	Type streamFrameType `json:"type"`

	// Meta frame fields. AppliedAt is delta-only; LastSnapshotTime appears on both.
	AppliedAt        time.Time `json:"applied_at,omitzero"`
	LastSnapshotTime time.Time `json:"last_snapshot_time,omitzero"`

	// Resource frame fields. Key indexes the cache (snapshot path); Obj/GVR carry
	// the resource itself. EventType is delta-only — empty on snapshot frames where
	// every entry is implicitly Added.
	Key       string                       `json:"key,omitempty"`
	Obj       *unstructured.Unstructured   `json:"obj,omitempty"`
	GVR       *schema.GroupVersionResource `json:"gvr,omitempty"`
	EventType watch.EventType              `json:"event_type,omitempty"`
}

// PeerSyncDelta is the wire format for a leader→peer push of per-cycle changes.
type PeerSyncDelta struct {
	// AppliedAt is the leader's wall-clock time when this delta was generated.
	// Secondaries use it to discard buffered deltas that predate their snapshot.
	AppliedAt time.Time

	// LastSnapshotTime carries snapshot-timing continuity across leader changes.
	LastSnapshotTime time.Time

	// Changes is the list of resource changes (adds/mods/deletes) for this cycle.
	Changes []ResourceChange
}

// encodeDeltaStream writes the delta as gzipped NDJSON frames into w. One meta
// frame followed by one frame per change. Closes the gzip writer (flushing the
// trailer) before returning.
func encodeDeltaStream(w io.Writer, delta *PeerSyncDelta) error {
	gz := gzip.NewWriter(w)
	enc := json.NewEncoder(gz)

	if err := enc.Encode(peerSyncStreamFrame{
		Type:             streamFrameMeta,
		AppliedAt:        delta.AppliedAt,
		LastSnapshotTime: delta.LastSnapshotTime,
	}); err != nil {
		_ = gz.Close()
		return fmt.Errorf("encode meta frame: %w", err)
	}

	for _, ch := range delta.Changes {
		frame := peerSyncStreamFrame{
			Obj:       ch.Obj,
			EventType: ch.EventType,
		}
		if ch.IsCRD {
			frame.Type = streamFrameCRD
		} else {
			frame.Type = streamFrameCR
			gvr := ch.GVR
			frame.GVR = &gvr
		}
		if err := enc.Encode(frame); err != nil {
			_ = gz.Close()
			return fmt.Errorf("encode change frame: %w", err)
		}
	}

	return gz.Close()
}

// decodeDeltaStream reads gzipped NDJSON frames from r and assembles a PeerSyncDelta.
// Returns an error if the meta frame is missing or any frame is malformed.
func decodeDeltaStream(r io.Reader) (*PeerSyncDelta, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	defer func() { _ = gr.Close() }()

	dec := json.NewDecoder(gr)
	delta := &PeerSyncDelta{}
	metaSeen := false

	for {
		var frame peerSyncStreamFrame
		if err := dec.Decode(&frame); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode frame: %w", err)
		}
		switch frame.Type {
		case streamFrameMeta:
			delta.AppliedAt = frame.AppliedAt
			delta.LastSnapshotTime = frame.LastSnapshotTime
			metaSeen = true
		case streamFrameCRD:
			if frame.Obj == nil {
				return nil, fmt.Errorf("CRD frame missing obj")
			}
			delta.Changes = append(delta.Changes, ResourceChange{
				Obj:       frame.Obj,
				EventType: frame.EventType,
				IsCRD:     true,
			})
		case streamFrameCR:
			if frame.Obj == nil {
				return nil, fmt.Errorf("CR frame missing obj")
			}
			if frame.GVR == nil {
				return nil, fmt.Errorf("CR frame missing gvr")
			}
			delta.Changes = append(delta.Changes, ResourceChange{
				Obj:       frame.Obj,
				EventType: frame.EventType,
				GVR:       *frame.GVR,
			})
		}
	}

	if !metaSeen {
		return nil, fmt.Errorf("missing meta frame")
	}
	return delta, nil
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
