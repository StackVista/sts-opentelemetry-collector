package emit_test

import (
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/emit"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

func TestPodContainerSummaryAttributes(t *testing.T) {
	tests := []struct {
		name       string
		apiVersion string
		kind       string
		status     map[string]interface{}
		want       int64
		total      int64
		ready      int64
		present    bool
	}{
		{
			name: "sum regular containers only", apiVersion: "v1", kind: "Pod",
			status: map[string]interface{}{
				"containerStatuses": []interface{}{
					map[string]interface{}{"restartCount": int64(3), "ready": true},
					map[string]interface{}{"restartCount": int64(7)},
					map[string]interface{}{},
				},
				"initContainerStatuses": []interface{}{
					map[string]interface{}{"restartCount": int64(100)},
				},
			},
			want: 10, total: 3, ready: 1, present: true,
		},
		{
			name: "empty statuses", apiVersion: "v1", kind: "Pod",
			status:  map[string]interface{}{"containerStatuses": []interface{}{}},
			present: true,
		},
		{name: "missing status", apiVersion: "v1", kind: "Pod"},
		{name: "missing container statuses", apiVersion: "v1", kind: "Pod", status: map[string]interface{}{"phase": "Pending"}},
		{
			name: "invalid count preserves event without summary", apiVersion: "v1", kind: "Pod",
			status: map[string]interface{}{"containerStatuses": []interface{}{map[string]interface{}{"restartCount": "invalid"}}},
		},
		{
			name: "negative count", apiVersion: "v1", kind: "Pod",
			status: map[string]interface{}{"containerStatuses": []interface{}{map[string]interface{}{"restartCount": int64(-1)}}},
		},
		{
			name: "large restart sum", apiVersion: "v1", kind: "Pod",
			status: map[string]interface{}{"containerStatuses": []interface{}{
				map[string]interface{}{"restartCount": int64(2147483647), "ready": true},
				map[string]interface{}{"restartCount": int64(2147483647), "ready": true},
			}},
			want: 4294967294, total: 2, ready: 2, present: true,
		},
		{
			name: "invalid ready value", apiVersion: "v1", kind: "Pod",
			status: map[string]interface{}{"containerStatuses": []interface{}{map[string]interface{}{"ready": "invalid"}}},
		},
		{
			name: "invalid container status", apiVersion: "v1", kind: "Pod",
			status: map[string]interface{}{"containerStatuses": []interface{}{"invalid"}},
		},
		{
			name: "custom resource named Pod", apiVersion: "example.com/v1", kind: "Pod",
			status: map[string]interface{}{"containerStatuses": []interface{}{map[string]interface{}{"restartCount": int64(3)}}},
		},
		{
			name: "other resource kind", apiVersion: "apps/v1", kind: "Deployment",
			status: map[string]interface{}{"containerStatuses": []interface{}{map[string]interface{}{"restartCount": int64(3)}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			object := map[string]interface{}{
				"apiVersion": tt.apiVersion, "kind": tt.kind,
				"metadata": map[string]interface{}{"name": "example", "namespace": "default"},
				"spec":     map[string]interface{}{"unchanged": true},
			}
			if tt.status != nil {
				object["status"] = tt.status
			}
			obj := &unstructured.Unstructured{Object: object}
			before := obj.DeepCopy()
			logs, err := emit.BuildObjectLogRecord(obj, watch.Modified, time.Now(), "cluster", emit.EventNameObject, nil)
			require.NoError(t, err)
			record := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
			value, present := record.Attributes().Get(emit.AttrK8sPodRestartCount)
			require.Equal(t, tt.present, present)
			if present {
				require.Equal(t, tt.want, value.Int())
			}
			for key, want := range map[string]int64{
				emit.AttrK8sPodContainerCount:      tt.total,
				emit.AttrK8sPodReadyContainerCount: tt.ready,
			} {
				value, present := record.Attributes().Get(key)
				require.Equal(t, tt.present, present)
				if present {
					require.Equal(t, want, value.Int())
				}
			}
			require.Equal(t, before, obj)
			body, found := record.Body().Map().Get("object")
			require.True(t, found)
			require.Equal(t, before.Object, body.Map().AsRaw())
		})
	}
}
