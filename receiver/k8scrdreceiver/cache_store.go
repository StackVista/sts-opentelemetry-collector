package k8scrdreceiver

import (
	"context"
	"encoding/json"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// CacheStore provides persistence for the resource cache across restarts and leader failovers.
type CacheStore interface {
	Load(ctx context.Context) (*resourceCache, error)
	Save(ctx context.Context, cache *resourceCache) error
}

// noopCacheStore is a no-op implementation used when no external cache is configured.
// Load returns an empty cache, Save is a no-op.
type noopCacheStore struct{}

var _ CacheStore = (*noopCacheStore)(nil)

func (n *noopCacheStore) Load(_ context.Context) (*resourceCache, error) {
	return newResourceCache(), nil
}

func (n *noopCacheStore) Save(_ context.Context, _ *resourceCache) error {
	return nil
}

// --- Serialization ---

// serializedCache is the JSON-serializable representation of a resourceCache.
type serializedCache struct {
	CRDs map[string]*unstructured.Unstructured `json:"crds"`
	CRs  map[string]*serializedCachedCR        `json:"crs"`
}

// serializedCachedCR is the JSON-serializable representation of a cachedCR.
// The GVR is stored as separate fields since schema.GroupVersionResource has no JSON tags.
type serializedCachedCR struct {
	Object   *unstructured.Unstructured `json:"object"`
	Group    string                     `json:"group"`
	Version  string                     `json:"version"`
	Resource string                     `json:"resource"`
}

func marshalResourceCache(rc *resourceCache) ([]byte, error) {
	sc := serializedCache{
		CRDs: rc.crds,
		CRs:  make(map[string]*serializedCachedCR, len(rc.crs)),
	}
	for key, cached := range rc.crs {
		sc.CRs[key] = &serializedCachedCR{
			Object:   cached.obj,
			Group:    cached.gvr.Group,
			Version:  cached.gvr.Version,
			Resource: cached.gvr.Resource,
		}
	}
	return json.Marshal(sc)
}

func unmarshalResourceCache(data []byte) (*resourceCache, error) {
	var sc serializedCache
	if err := json.Unmarshal(data, &sc); err != nil {
		return nil, err
	}

	rc := &resourceCache{
		crds: sc.CRDs,
		crs:  make(map[string]*cachedCR, len(sc.CRs)),
	}
	if rc.crds == nil {
		rc.crds = make(map[string]*unstructured.Unstructured)
	}
	for key, scr := range sc.CRs {
		rc.crs[key] = &cachedCR{
			obj: scr.Object,
			gvr: schema.GroupVersionResource{
				Group:    scr.Group,
				Version:  scr.Version,
				Resource: scr.Resource,
			},
		}
	}
	return rc, nil
}
