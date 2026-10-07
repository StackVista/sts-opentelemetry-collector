package k8sresourcereceiver

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/stackvista/sts-opentelemetry-collector/common/k8ssanitize"
)

// sanitizeObject is an informer transform, so raw Secret and ConfigMap contents
// never reach the cache, the peer sync or any consumer. It never errors, because
// a transform error would stall the informer.
func sanitizeObject(configMapMaxDataSize int) func(interface{}) (interface{}, error) {
	return func(obj interface{}) (interface{}, error) {
		if u, ok := obj.(*unstructured.Unstructured); ok {
			k8ssanitize.Object(u.Object, configMapMaxDataSize)
		}
		return obj, nil
	}
}
