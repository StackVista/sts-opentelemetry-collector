package k8scrdreceiver

import (
	"fmt"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/emit"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// ResourceChange represents a detected change between the resource cache and
// current informer state.
type ResourceChange struct {
	Obj       *unstructured.Unstructured
	EventType watch.EventType
	IsCRD     bool
	GVR       schema.GroupVersionResource // set for CR changes
}

// cachedCR stores a CR alongside its GVR for delta computation.
type cachedCR struct {
	Obj *unstructured.Unstructured
	GVR schema.GroupVersionResource
}

// resourceCache tracks the last state emitted downstream.
type resourceCache struct {
	CRDs map[string]*unstructured.Unstructured // key: CRD name
	CRs  map[string]*cachedCR                  // key: crResourceKey(gvr, namespace, name)
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

// applyDeletions removes DELETED entries from the cache.
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
func (rc *resourceCache) applyDelta(changes []ResourceChange) {
	rc.applyAdditions(changes)
	rc.applyDeletions(changes)
}

// crResourceKey returns a unique key for a CR within the resource cache.
func crResourceKey(gvr schema.GroupVersionResource, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", emit.FormatGVRKey(gvr), namespace, name)
}
