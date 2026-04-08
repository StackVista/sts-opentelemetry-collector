package k8scrdreceiver

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

// k8sClient abstracts Kubernetes dynamic client operations for testing
type k8sClient interface {
	Resource(gvr schema.GroupVersionResource) k8sResourceInterface
}

// k8sResourceInterface abstracts resource-specific operations for testing
type k8sResourceInterface interface {
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
	List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error)
}

// dynamicClientWrapper wraps the real dynamic.Interface to implement k8sClient
type dynamicClientWrapper struct {
	client dynamic.Interface
}

// newDynamicClientWrapper creates a k8sClient from a dynamic.Interface
func newDynamicClientWrapper(client dynamic.Interface) k8sClient {
	return &dynamicClientWrapper{client: client}
}

// Resource returns a resource interface for the given GVR
func (w *dynamicClientWrapper) Resource(gvr schema.GroupVersionResource) k8sResourceInterface {
	return &dynamicResourceWrapper{resource: w.client.Resource(gvr)}
}

// dynamicResourceWrapper wraps dynamic.ResourceInterface to implement k8sResourceInterface
type dynamicResourceWrapper struct {
	resource dynamic.NamespaceableResourceInterface
}

// Watch creates a watch for the resource
func (w *dynamicResourceWrapper) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return w.resource.Watch(ctx, opts)
}

// List lists all instances of the resource
func (w *dynamicResourceWrapper) List(
	ctx context.Context, opts metav1.ListOptions,
) (*unstructured.UnstructuredList, error) {
	return w.resource.List(ctx, opts)
}
