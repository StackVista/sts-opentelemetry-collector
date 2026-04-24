package k8scrdreceiver

import (
	"fmt"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/emit"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// ResourceChange represents a detected change between the resource cache and current
// informer state. It is also the wire format for delta pushes between replicas, so
// fields are exported and JSON-tagged.
type ResourceChange struct {
	Obj       *unstructured.Unstructured  `json:"obj"`
	EventType watch.EventType             `json:"event_type"`
	IsCRD     bool                        `json:"is_crd"`
	GVR       schema.GroupVersionResource `json:"gvr,omitempty"` // set for CR changes
}

// cachedCR stores a CR alongside its GVR for delta computation. Fields are exported
// because the cache is shipped over the peer sync protocol via encoding/json.
type cachedCR struct {
	Obj *unstructured.Unstructured  `json:"object"`
	GVR schema.GroupVersionResource `json:"gvr"`
}

// resourceCache tracks the last state emitted downstream.
// Fields are exported so the cache can be JSON-marshaled directly for peer sync.
type resourceCache struct {
	CRDs map[string]*unstructured.Unstructured `json:"crds"` // key: CRD name
	CRs  map[string]*cachedCR                  `json:"crs"`  // key: crResourceKey(gvr, namespace, name)
}

func newResourceCache() *resourceCache {
	return &resourceCache{
		CRDs: make(map[string]*unstructured.Unstructured),
		CRs:  make(map[string]*cachedCR),
	}
}

func (rc *resourceCache) isEmpty() bool {
	return len(rc.CRDs) == 0 && len(rc.CRs) == 0
}

// computeChanges compares the current informer state against the resource cache and returns the delta.
func (rc *resourceCache) computeChanges(
	currentCRDs []*unstructured.Unstructured,
	currentCRs map[schema.GroupVersionResource][]*unstructured.Unstructured,
) []ResourceChange {
	var changes []ResourceChange

	// --- CRD changes ---
	currentCRDMap := make(map[string]*unstructured.Unstructured, len(currentCRDs))
	for _, crd := range currentCRDs {
		name := crd.GetName()
		currentCRDMap[name] = crd

		prev, exists := rc.CRDs[name]
		if !exists {
			changes = append(changes, ResourceChange{
				Obj:       crd,
				EventType: watch.Added,
				IsCRD:     true,
			})
		} else if prev.GetResourceVersion() != crd.GetResourceVersion() {
			changes = append(changes, ResourceChange{
				Obj:       crd,
				EventType: watch.Modified,
				IsCRD:     true,
			})
		}
	}

	// Detect deleted CRDs
	for name, prev := range rc.CRDs {
		if _, exists := currentCRDMap[name]; !exists {
			changes = append(changes, ResourceChange{
				Obj:       prev,
				EventType: watch.Deleted,
				IsCRD:     true,
			})
		}
	}

	// --- CR changes ---
	currentCRMap := make(map[string]*unstructured.Unstructured)
	for gvr, crs := range currentCRs {
		for _, cr := range crs {
			key := crResourceKey(gvr, cr.GetNamespace(), cr.GetName())
			currentCRMap[key] = cr

			prev, exists := rc.CRs[key]
			if !exists {
				changes = append(changes, ResourceChange{
					Obj:       cr,
					EventType: watch.Added,
					GVR:       gvr,
				})
			} else if prev.Obj.GetResourceVersion() != cr.GetResourceVersion() {
				changes = append(changes, ResourceChange{
					Obj:       cr,
					EventType: watch.Modified,
					GVR:       gvr,
				})
			}
		}
	}

	// Detect deleted CRs
	for key, prev := range rc.CRs {
		if _, exists := currentCRMap[key]; !exists {
			changes = append(changes, ResourceChange{
				Obj:       prev.Obj,
				EventType: watch.Deleted,
				GVR:       prev.GVR,
			})
		}
	}

	return changes
}

// applyAdditions applies ADDED and MODIFIED changes to the cache.
// Objects are stored by pointer. Per k8s client-go's ThreadSafeStore contract
// (tools/cache/thread_safe_store.go), List/Get returns must be treated as
// read-only; we extend that contract through this cache.
func (rc *resourceCache) applyAdditions(changes []ResourceChange) {
	for _, ch := range changes {
		if ch.EventType == watch.Deleted {
			continue
		}
		if ch.IsCRD {
			rc.CRDs[ch.Obj.GetName()] = ch.Obj
		} else {
			key := crResourceKey(ch.GVR, ch.Obj.GetNamespace(), ch.Obj.GetName())
			rc.CRs[key] = &cachedCR{Obj: ch.Obj, GVR: ch.GVR}
		}
	}
}

// applyDeletions removes DELETED entries from the cache. Used in the second phase of a
// two-phase increment so the cache retains the soon-to-be-deleted resources until after
// the platform has been notified.
func (rc *resourceCache) applyDeletions(changes []ResourceChange) {
	for _, ch := range changes {
		if ch.EventType != watch.Deleted {
			continue
		}
		if ch.IsCRD {
			delete(rc.CRDs, ch.Obj.GetName())
		} else {
			key := crResourceKey(ch.GVR, ch.Obj.GetNamespace(), ch.Obj.GetName())
			delete(rc.CRs, key)
		}
	}
}

// applyDelta splits and applies all changes in a delta — adds/mods first, then deletes.
// Used by secondaries when receiving a delta from the leader. The same ordering as the
// leader's two-phase emit (state-after-add-but-before-delete is briefly visible) is not
// material here since secondaries don't emit, but we use the same primitives.
func (rc *resourceCache) applyDelta(changes []ResourceChange) {
	rc.applyAdditions(changes)
	rc.applyDeletions(changes)
}

// update replaces the resource cache with the current state. Objects are stored
// by pointer (read-only per the informer convention).
func (rc *resourceCache) update(
	currentCRDs []*unstructured.Unstructured,
	currentCRs map[schema.GroupVersionResource][]*unstructured.Unstructured,
) {
	newCRDs := make(map[string]*unstructured.Unstructured, len(currentCRDs))
	for _, crd := range currentCRDs {
		newCRDs[crd.GetName()] = crd
	}
	rc.CRDs = newCRDs

	newCRs := make(map[string]*cachedCR)
	for gvr, crs := range currentCRs {
		for _, cr := range crs {
			key := crResourceKey(gvr, cr.GetNamespace(), cr.GetName())
			newCRs[key] = &cachedCR{
				Obj: cr,
				GVR: gvr,
			}
		}
	}
	rc.CRs = newCRs
}

// crResourceKey returns a unique key for a CR within the resource cache.
func crResourceKey(gvr schema.GroupVersionResource, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", emit.FormatGVRKey(gvr), namespace, name)
}
