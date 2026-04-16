package k8scrdreceiver

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// resourceChange represents a detected change between the resource cache and current informer state.
type resourceChange struct {
	obj       *unstructured.Unstructured
	eventType watch.EventType
	isCRD     bool
	gvr       schema.GroupVersionResource // set for CR changes
}

// cachedCR stores a CR alongside its GVR for delta computation.
type cachedCR struct {
	obj *unstructured.Unstructured
	gvr schema.GroupVersionResource
}

// resourceCache tracks the last state emitted downstream.
// It is used to compute deltas between successive increment loop iterations.
// The cache is not thread-safe — it is only accessed from the increment loop goroutine.
type resourceCache struct {
	crds map[string]*unstructured.Unstructured // key: CRD name
	crs  map[string]*cachedCR                  // key: crResourceKey(gvr, namespace, name)
}

func newResourceCache() *resourceCache {
	return &resourceCache{
		crds: make(map[string]*unstructured.Unstructured),
		crs:  make(map[string]*cachedCR),
	}
}

func (rc *resourceCache) isEmpty() bool {
	return len(rc.crds) == 0 && len(rc.crs) == 0
}

// computeChanges compares the current informer state against the resource cache and returns the delta.
func (rc *resourceCache) computeChanges(
	currentCRDs []*unstructured.Unstructured,
	currentCRs map[schema.GroupVersionResource][]*unstructured.Unstructured,
) []resourceChange {
	var changes []resourceChange

	// --- CRD changes ---
	currentCRDMap := make(map[string]*unstructured.Unstructured, len(currentCRDs))
	for _, crd := range currentCRDs {
		name := crd.GetName()
		currentCRDMap[name] = crd

		prev, exists := rc.crds[name]
		if !exists {
			changes = append(changes, resourceChange{
				obj:       crd,
				eventType: watch.Added,
				isCRD:     true,
			})
		} else if prev.GetResourceVersion() != crd.GetResourceVersion() {
			changes = append(changes, resourceChange{
				obj:       crd,
				eventType: watch.Modified,
				isCRD:     true,
			})
		}
	}

	// Detect deleted CRDs
	for name, prev := range rc.crds {
		if _, exists := currentCRDMap[name]; !exists {
			changes = append(changes, resourceChange{
				obj:       prev,
				eventType: watch.Deleted,
				isCRD:     true,
			})
		}
	}

	// --- CR changes ---
	currentCRMap := make(map[string]*unstructured.Unstructured)
	for gvr, crs := range currentCRs {
		for _, cr := range crs {
			key := crResourceKey(gvr, cr.GetNamespace(), cr.GetName())
			currentCRMap[key] = cr

			prev, exists := rc.crs[key]
			if !exists {
				changes = append(changes, resourceChange{
					obj:       cr,
					eventType: watch.Added,
					gvr:       gvr,
				})
			} else if prev.obj.GetResourceVersion() != cr.GetResourceVersion() {
				changes = append(changes, resourceChange{
					obj:       cr,
					eventType: watch.Modified,
					gvr:       gvr,
				})
			}
		}
	}

	// Detect deleted CRs
	for key, prev := range rc.crs {
		if _, exists := currentCRMap[key]; !exists {
			changes = append(changes, resourceChange{
				obj:       prev.obj,
				eventType: watch.Deleted,
				gvr:       prev.gvr,
			})
		}
	}

	return changes
}

// update replaces the resource cache with the current state.
// Objects are deep-copied to prevent the cache from being mutated by informer updates.
func (rc *resourceCache) update(
	currentCRDs []*unstructured.Unstructured,
	currentCRs map[schema.GroupVersionResource][]*unstructured.Unstructured,
) {
	newCRDs := make(map[string]*unstructured.Unstructured, len(currentCRDs))
	for _, crd := range currentCRDs {
		newCRDs[crd.GetName()] = crd.DeepCopy()
	}
	rc.crds = newCRDs

	newCRs := make(map[string]*cachedCR)
	for gvr, crs := range currentCRs {
		for _, cr := range crs {
			key := crResourceKey(gvr, cr.GetNamespace(), cr.GetName())
			newCRs[key] = &cachedCR{
				obj: cr.DeepCopy(),
				gvr: gvr,
			}
		}
	}
	rc.crs = newCRs
}

// crResourceKey returns a unique key for a CR within the resource cache.
func crResourceKey(gvr schema.GroupVersionResource, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", formatGVRKey(gvr), namespace, name)
}
