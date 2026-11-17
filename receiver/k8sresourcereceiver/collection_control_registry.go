package k8sresourcereceiver

import (
	"sync"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/resourcecollection"
	"k8s.io/client-go/tools/cache"
)

// Reuse the required declaration informer for an identical collection watch.
type collectionControlRegistry struct {
	target, controls *collectionWatchRegistry
	transform        cache.TransformFunc
	mu               sync.Mutex
	declaration      *collectionWatchSpec
}

func (r *collectionControlRegistry) reconcile(specs []collectionWatchSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var declaration *collectionWatchSpec
	targets := make([]collectionWatchSpec, 0, len(specs))
	for _, spec := range specs {
		if spec.Key == (collectionWatchKey{GVR: resourcecollection.GVR()}) {
			copySpec := copyCollectionWatchSpec(spec)
			declaration = &copySpec
		} else {
			targets = append(targets, spec)
		}
	}
	if err := r.target.reconcile(targets); err != nil {
		return err
	}
	r.declaration = declaration
	return nil
}

func (r *collectionControlRegistry) snapshot() []collectionWatchSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := r.target.snapshot()
	if r.declaration == nil {
		return result
	}
	for _, observation := range r.controls.snapshot() {
		if observation.Spec.Key != r.declaration.Key {
			continue
		}
		observation.Spec = copyCollectionWatchSpec(*r.declaration)
		objects := make([]resourceObject, 0, len(observation.Objects))
		for _, object := range observation.Objects {
			decoded, err := decodeObject(object)
			if err == nil {
				var transformed interface{}
				transformed, err = r.transform(decoded.DeepCopy())
				if cached, ok := toResourceObject(transformed); err == nil && ok {
					objects = append(objects, cached)
					continue
				}
			}
			observation.State, observation.Reason = resourcecollection.Pending, "ObservationFailed"
			observation.Message = "A declaration object could not be read from the cache."
		}
		observation.Objects = objects
		result = append(result, observation)
	}
	return result
}
