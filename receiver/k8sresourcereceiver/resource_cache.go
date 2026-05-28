package k8sresourcereceiver

import (
	"fmt"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/emit"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// ObjectSource identifies which informer flavour an object came from.
type ObjectSource string

const (
	ObjectSourceCR     ObjectSource = "cr"
	ObjectSourceStatic ObjectSource = "static"
)

// eventNameForSource maps an ObjectSource to the emit event name that controls
// the downstream log shape. CR-sourced objects (CRD-discovered or CRD-backed
// statics) keep the CR shape; plain static objects use the neutral Object shape.
func eventNameForSource(s ObjectSource) string {
	if s == ObjectSourceCR {
		return emit.EventNameCR
	}
	return emit.EventNameObject
}

// ObjectGroup pairs a list of objects with the source they came from. Returned
// per-GVR by Informers.ReadObjects.
type ObjectGroup struct {
	Source  ObjectSource
	Objects []*unstructured.Unstructured
}

// ResourceChange represents a detected change between the resource cache and
// current informer state. Source is empty for CRD changes (IsCRD=true).
type ResourceChange struct {
	Obj       *unstructured.Unstructured
	EventType watch.EventType
	IsCRD     bool
	GVR       schema.GroupVersionResource // set for object changes
	Source    ObjectSource                // set for object changes
}

type cachedObject struct {
	Obj    *unstructured.Unstructured
	GVR    schema.GroupVersionResource
	Source ObjectSource
}

// resourceCache tracks the last state emitted downstream.
type resourceCache struct {
	CRDs    map[string]*unstructured.Unstructured // key: CRD name
	Objects map[string]*cachedObject              // key: objectResourceKey(gvr, namespace, name)
}

func newResourceCache() *resourceCache {
	return &resourceCache{
		CRDs:    make(map[string]*unstructured.Unstructured),
		Objects: make(map[string]*cachedObject),
	}
}

func (rc *resourceCache) isEmpty() bool {
	return len(rc.CRDs) == 0 && len(rc.Objects) == 0
}

// computeChanges compares the current informer state against the resource cache and returns the delta.
func (rc *resourceCache) computeChanges(
	currentCRDs []*unstructured.Unstructured,
	currentObjects map[schema.GroupVersionResource]ObjectGroup,
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

	// --- Object changes ---
	currentObjectMap := make(map[string]*unstructured.Unstructured)
	for gvr, group := range currentObjects {
		for _, obj := range group.Objects {
			key := objectResourceKey(gvr, obj.GetNamespace(), obj.GetName())
			currentObjectMap[key] = obj

			prev, exists := rc.Objects[key]
			if !exists {
				changes = append(changes, ResourceChange{
					Obj:       obj,
					EventType: watch.Added,
					GVR:       gvr,
					Source:    group.Source,
				})
			} else if prev.Obj.GetResourceVersion() != obj.GetResourceVersion() {
				changes = append(changes, ResourceChange{
					Obj:       obj,
					EventType: watch.Modified,
					GVR:       gvr,
					Source:    group.Source,
				})
			}
		}
	}

	// Detect deleted objects
	for key, prev := range rc.Objects {
		if _, exists := currentObjectMap[key]; !exists {
			changes = append(changes, ResourceChange{
				Obj:       prev.Obj,
				EventType: watch.Deleted,
				GVR:       prev.GVR,
				Source:    prev.Source,
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
			key := objectResourceKey(ch.GVR, ch.Obj.GetNamespace(), ch.Obj.GetName())
			rc.Objects[key] = &cachedObject{Obj: ch.Obj, GVR: ch.GVR, Source: ch.Source}
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
			key := objectResourceKey(ch.GVR, ch.Obj.GetNamespace(), ch.Obj.GetName())
			delete(rc.Objects, key)
		}
	}
}

// applyDelta splits and applies all changes in a delta — adds/mods first, then deletes.
func (rc *resourceCache) applyDelta(changes []ResourceChange) {
	rc.applyAdditions(changes)
	rc.applyDeletions(changes)
}

// objectResourceKey returns a unique key for an object within the resource cache.
func objectResourceKey(gvr schema.GroupVersionResource, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", emit.FormatGVRKey(gvr), namespace, name)
}
