package k8scrdreceiver

import (
	"fmt"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/emit"
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

// cachedCR stores a CR alongside its GVR for delta computation. Fields are exported
// because the cache is shipped over the peer sync protocol via encoding/json.
type cachedCR struct {
	Obj *unstructured.Unstructured  `json:"object"`
	GVR schema.GroupVersionResource `json:"gvr"`
}

// resourceCache tracks the last state emitted downstream.
// It is used to compute deltas between successive increment loop iterations.
// The cache is not thread-safe — it is only accessed from the increment loop goroutine.
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
) []resourceChange {
	var changes []resourceChange

	// --- CRD changes ---
	currentCRDMap := make(map[string]*unstructured.Unstructured, len(currentCRDs))
	for _, crd := range currentCRDs {
		name := crd.GetName()
		currentCRDMap[name] = crd

		prev, exists := rc.CRDs[name]
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
	for name, prev := range rc.CRDs {
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

			prev, exists := rc.CRs[key]
			if !exists {
				changes = append(changes, resourceChange{
					obj:       cr,
					eventType: watch.Added,
					gvr:       gvr,
				})
			} else if prev.Obj.GetResourceVersion() != cr.GetResourceVersion() {
				changes = append(changes, resourceChange{
					obj:       cr,
					eventType: watch.Modified,
					gvr:       gvr,
				})
			}
		}
	}

	// Detect deleted CRs
	for key, prev := range rc.CRs {
		if _, exists := currentCRMap[key]; !exists {
			changes = append(changes, resourceChange{
				obj:       prev.Obj,
				eventType: watch.Deleted,
				gvr:       prev.GVR,
			})
		}
	}

	return changes
}

// applyAdditions applies ADDED and MODIFIED changes to the cache. Used in the first
// phase of a two-phase increment so peers can be updated before the platform is notified.
func (rc *resourceCache) applyAdditions(changes []resourceChange) {
	for _, ch := range changes {
		if ch.eventType == watch.Deleted {
			continue
		}
		if ch.isCRD {
			rc.CRDs[ch.obj.GetName()] = ch.obj.DeepCopy()
		} else {
			key := crResourceKey(ch.gvr, ch.obj.GetNamespace(), ch.obj.GetName())
			rc.CRs[key] = &cachedCR{Obj: ch.obj.DeepCopy(), GVR: ch.gvr}
		}
	}
}

// applyDeletions removes DELETED entries from the cache. Used in the second phase of a
// two-phase increment so the cache retains the soon-to-be-deleted resources until after
// the platform has been notified.
func (rc *resourceCache) applyDeletions(changes []resourceChange) {
	for _, ch := range changes {
		if ch.eventType != watch.Deleted {
			continue
		}
		if ch.isCRD {
			delete(rc.CRDs, ch.obj.GetName())
		} else {
			key := crResourceKey(ch.gvr, ch.obj.GetNamespace(), ch.obj.GetName())
			delete(rc.CRs, key)
		}
	}
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
	rc.CRDs = newCRDs

	newCRs := make(map[string]*cachedCR)
	for gvr, crs := range currentCRs {
		for _, cr := range crs {
			key := crResourceKey(gvr, cr.GetNamespace(), cr.GetName())
			newCRs[key] = &cachedCR{
				Obj: cr.DeepCopy(),
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
