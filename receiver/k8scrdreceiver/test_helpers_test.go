//nolint:testpackage // Tests require access to internal functions
package k8scrdreceiver

import (
	"context"
	"errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// fakeClient is a configurable fake k8sClient for testing both pull and watch modes.
type fakeClient struct {
	// resources maps GVR keys to their configured behavior
	resources map[string]*fakeResource
}

type fakeResource struct {
	listResult *unstructured.UnstructuredList
	listError  error
	watcher    watch.Interface
	watchError error
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		resources: make(map[string]*fakeResource),
	}
}

func (f *fakeClient) withList(gvr schema.GroupVersionResource, list *unstructured.UnstructuredList) *fakeClient {
	key := formatGVRKey(gvr)
	r := f.getOrCreate(key)
	r.listResult = list
	return f
}

func (f *fakeClient) withListError(gvr schema.GroupVersionResource, err error) *fakeClient {
	key := formatGVRKey(gvr)
	r := f.getOrCreate(key)
	r.listError = err
	return f
}

func (f *fakeClient) withWatcher(gvr schema.GroupVersionResource, w watch.Interface) *fakeClient {
	key := formatGVRKey(gvr)
	r := f.getOrCreate(key)
	r.watcher = w
	return f
}

func (f *fakeClient) withWatchError(gvr schema.GroupVersionResource, err error) *fakeClient {
	key := formatGVRKey(gvr)
	r := f.getOrCreate(key)
	r.watchError = err
	return f
}

func (f *fakeClient) getOrCreate(key string) *fakeResource {
	if _, ok := f.resources[key]; !ok {
		f.resources[key] = &fakeResource{}
	}
	return f.resources[key]
}

func (f *fakeClient) Resource(gvr schema.GroupVersionResource) k8sResourceInterface {
	key := formatGVRKey(gvr)
	if r, ok := f.resources[key]; ok {
		return &fakeResourceClient{res: r}
	}
	return &fakeResourceClient{res: &fakeResource{}}
}

type fakeResourceClient struct {
	res *fakeResource
}

func (f *fakeResourceClient) List(_ context.Context, _ metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	if f.res.listError != nil {
		return nil, f.res.listError
	}
	if f.res.listResult != nil {
		return f.res.listResult, nil
	}
	return &unstructured.UnstructuredList{}, nil
}

func (f *fakeResourceClient) Watch(_ context.Context, _ metav1.ListOptions) (watch.Interface, error) {
	if f.res.watchError != nil {
		return nil, f.res.watchError
	}
	if f.res.watcher != nil {
		return f.res.watcher, nil
	}
	return nil, errors.New("watch not configured in fake")
}

// fakeWatcher implements watch.Interface for testing.
type fakeWatcher struct {
	ch     chan watch.Event
	stopCh chan struct{}
}

func newFakeWatcher() *fakeWatcher {
	return &fakeWatcher{
		ch:     make(chan watch.Event, 10),
		stopCh: make(chan struct{}),
	}
}

func (w *fakeWatcher) ResultChan() <-chan watch.Event {
	return w.ch
}

func (w *fakeWatcher) Stop() {
	select {
	case <-w.stopCh:
	default:
		close(w.stopCh)
	}
}

// send sends an event to the watcher channel. Returns false if the watcher was stopped.
func (w *fakeWatcher) send(event watch.Event) bool {
	select {
	case w.ch <- event:
		return true
	case <-w.stopCh:
		return false
	}
}

// CRD GVR used across tests
var crdGVR = schema.GroupVersionResource{
	Group:    "apiextensions.k8s.io",
	Version:  "v1",
	Resource: "customresourcedefinitions",
}

// makeTestCRD creates a test CRD unstructured object.
func makeTestCRD(name, group, kind, plural string) *unstructured.Unstructured {
	return makeTestCRDWithVersions(name, group, kind, plural, []map[string]interface{}{
		{"name": "v1", "storage": true},
	})
}

// makeTestCRDWithVersions creates a test CRD with multiple versions.
func makeTestCRDWithVersions(name, group, kind, plural string, versions []map[string]interface{}) *unstructured.Unstructured {
	versionList := make([]interface{}, len(versions))
	for i, v := range versions {
		versionList[i] = v
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apiextensions.k8s.io/v1",
			"kind":       "CustomResourceDefinition",
			"metadata": map[string]interface{}{
				"name": name,
			},
			"spec": map[string]interface{}{
				"group": group,
				"names": map[string]interface{}{
					"kind":   kind,
					"plural": plural,
				},
				"versions": versionList,
			},
		},
	}
}

// makeTestCR creates a test CR unstructured object.
func makeTestCR(name, group, kind, version string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": group + "/" + version,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"name": name,
			},
			"spec": map[string]interface{}{},
		},
	}
}
