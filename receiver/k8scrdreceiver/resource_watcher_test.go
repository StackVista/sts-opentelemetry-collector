//nolint:testpackage
package k8scrdreceiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

const testAllowedGroup = "allowed-group"

func TestResourceWatcher_ListInitialState_RespectsFilter(t *testing.T) {
	tests := []struct {
		name             string
		objects          []*unstructured.Unstructured
		filterFunc       func(*unstructured.Unstructured) bool
		wantEmittedCount int
		wantHandledCount int
		wantSkippedCount int
	}{
		{
			name: "all objects pass filter",
			objects: []*unstructured.Unstructured{
				makeTestObject("obj1", testAllowedGroup),
				makeTestObject("obj2", testAllowedGroup),
			},
			filterFunc: func(obj *unstructured.Unstructured) bool {
				group, _, _ := unstructured.NestedString(obj.Object, "spec", "group")
				return group == testAllowedGroup
			},
			wantEmittedCount: 2,
			wantHandledCount: 2,
			wantSkippedCount: 0,
		},
		{
			name: "some objects filtered out",
			objects: []*unstructured.Unstructured{
				makeTestObject("obj1", testAllowedGroup),
				makeTestObject("obj2", "blocked-group"),
				makeTestObject("obj3", testAllowedGroup),
			},
			filterFunc: func(obj *unstructured.Unstructured) bool {
				group, _, _ := unstructured.NestedString(obj.Object, "spec", "group")
				return group == testAllowedGroup
			},
			wantEmittedCount: 2,
			wantHandledCount: 3, // All handled, but not all emitted
			wantSkippedCount: 1,
		},
		{
			name: "all objects filtered out",
			objects: []*unstructured.Unstructured{
				makeTestObject("obj1", "blocked-group"),
				makeTestObject("obj2", "blocked-group"),
			},
			filterFunc: func(obj *unstructured.Unstructured) bool {
				group, _, _ := unstructured.NestedString(obj.Object, "spec", "group")
				return group == testAllowedGroup
			},
			wantEmittedCount: 0,
			wantHandledCount: 2,
			wantSkippedCount: 2,
		},
		{
			name: "no filter - all emitted",
			objects: []*unstructured.Unstructured{
				makeTestObject("obj1", "any-group"),
				makeTestObject("obj2", "other-group"),
			},
			filterFunc:       nil, // No filter - all should be emitted
			wantEmittedCount: 2,
			wantHandledCount: 0, // Handler not called when nil
			wantSkippedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup fake client
			fakeClient := &fakeK8sClient{
				listResult: &unstructured.UnstructuredList{
					Items: make([]unstructured.Unstructured, len(tt.objects)),
				},
			}
			for i, obj := range tt.objects {
				fakeClient.listResult.Items[i] = *obj
			}

			// Track what gets emitted and handled
			emittedLogs := []string{}
			handledObjects := []string{}
			skippedObjects := []string{}

			// Create watcher
			gvr := schema.GroupVersionResource{Group: "test.io", Version: "v1", Resource: "tests"}
			w := &resourceWatcher{
				gvr:    gvr,
				client: fakeClient,
				ctx:    context.Background(),
				logger: zap.NewNop(),
				config: watcherConfig{
					listInitialState: true,
					resourceType:     "Test",
				},
				handlers: watcherHandlers{
					buildLog: func(obj *unstructured.Unstructured, _ watch.EventType, _ time.Time) (plog.Logs, error) {
						return plog.NewLogs(), nil
					},
					consumeLog: func(_ context.Context, _ plog.Logs) error {
						// Track what got emitted
						// Note: In real test we'd inspect the log, here we just count
						emittedLogs = append(emittedLogs, "emitted")
						return nil
					},
				},
			}

			// Add filter handler if provided
			if tt.filterFunc != nil {
				w.handlers.handleEvent = func(obj *unstructured.Unstructured, _ watch.EventType) (bool, error) {
					handledObjects = append(handledObjects, obj.GetName())
					shouldEmit := tt.filterFunc(obj)
					if !shouldEmit {
						skippedObjects = append(skippedObjects, obj.GetName())
					}
					return shouldEmit, nil
				}
			}

			// Execute
			err := w.listInitialState()
			require.NoError(t, err)

			// Verify
			assert.Len(t, emittedLogs, tt.wantEmittedCount, "emitted log count mismatch")
			assert.Len(t, handledObjects, tt.wantHandledCount, "handled object count mismatch")
			assert.Len(t, skippedObjects, tt.wantSkippedCount, "skipped object count mismatch")
		})
	}
}

func makeTestObject(name, apiGroup string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apiextensions.k8s.io/v1",
			"kind":       "CustomResourceDefinition",
			"metadata": map[string]interface{}{
				"name": name,
			},
			"spec": map[string]interface{}{
				"group": apiGroup,
			},
		},
	}
}

// fakeK8sClient is a minimal fake client for testing
type fakeK8sClient struct {
	listResult *unstructured.UnstructuredList
	listError  error
}

func (f *fakeK8sClient) Resource(_ schema.GroupVersionResource) k8sResourceInterface {
	return &fakeResourceClient{
		listResult: f.listResult,
		listError:  f.listError,
	}
}

type fakeResourceClient struct {
	listResult *unstructured.UnstructuredList
	listError  error
}

func (f *fakeResourceClient) List(_ context.Context, _ metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	if f.listError != nil {
		return nil, f.listError
	}
	return f.listResult, nil
}

func (f *fakeResourceClient) Watch(_ context.Context, _ metav1.ListOptions) (watch.Interface, error) {
	return nil, errors.New("watch not implemented in test")
}
