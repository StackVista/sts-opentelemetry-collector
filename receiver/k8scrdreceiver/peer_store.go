package k8scrdreceiver

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// PeerSyncMessage is the protocol payload exchanged between replicas.
type PeerSyncMessage struct {
	// Cache is the last-emitted resource state.
	Cache *resourceCache `json:"cache"`

	// LastSnapshotTime the time at which the previous leader emitted
	// its most recent full snapshot. A new leader uses this to schedule its next
	// snapshot relative to the cluster's history rather than restarting the timer
	// from "now" — preventing a snapshot gap that exceeds the configured interval.
	LastSnapshotTime time.Time `json:"last_snapshot_time"`
}

// PeerStore provides resource cache sharing between replicas for fast leader failover.
type PeerStore interface {
	Load(ctx context.Context) (*PeerSyncMessage, error)
	Save(ctx context.Context, state *PeerSyncMessage) error
}

// noopPeerStore is a no-op implementation.
type noopPeerStore struct{}

var _ PeerStore = (*noopPeerStore)(nil)

func (n *noopPeerStore) Load(_ context.Context) (*PeerSyncMessage, error) {
	return &PeerSyncMessage{Cache: newResourceCache()}, nil
}

func (n *noopPeerStore) Save(_ context.Context, _ *PeerSyncMessage) error {
	return nil
}

func marshalPeerSyncMessage(state *PeerSyncMessage) ([]byte, error) {
	if state == nil || state.Cache == nil {
		return nil, fmt.Errorf("marshalPeerSyncMessage: state or cache is nil")
	}
	return json.Marshal(state)
}

func unmarshalPeerSyncMessage(data []byte) (*PeerSyncMessage, error) {
	var state PeerSyncMessage
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("unmarshal peer sync message: %w", err)
	}
	if state.Cache == nil {
		state.Cache = newResourceCache()
	}
	if state.Cache.CRDs == nil {
		state.Cache.CRDs = make(map[string]*unstructured.Unstructured)
	}
	if state.Cache.CRs == nil {
		state.Cache.CRs = make(map[string]*cachedCR)
	}
	return &state, nil
}
