package emit_test

import (
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/emit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

func TestBuildCRLogRecord(t *testing.T) {
	timestamp := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)
	clusterName := "test-cluster"

	tests := []struct {
		name      string
		cr        *unstructured.Unstructured
		eventType watch.EventType
		wantErr   bool
	}{
		{
			name: "valid CR with namespace",
			cr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "policies.kubewarden.io/v1",
					"kind":       "PolicyServer",
					"metadata": map[string]interface{}{
						"name":      "default",
						"namespace": "kubewarden",
						"uid":       "abc-123",
					},
					"spec": map[string]interface{}{
						"image": "ghcr.io/kubewarden/policy-server:v1.0.0",
					},
				},
			},
			eventType: watch.Added,
			wantErr:   false,
		},
		{
			name: "cluster-scoped CR without namespace",
			cr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "policies.kubewarden.io/v1",
					"kind":       "ClusterAdmissionPolicy",
					"metadata": map[string]interface{}{
						"name": "pod-privileged",
						"uid":  "def-456",
					},
				},
			},
			eventType: watch.Modified,
			wantErr:   false,
		},
		{
			name: "deleted event",
			cr: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "example.com/v1",
					"kind":       "MyResource",
					"metadata": map[string]interface{}{
						"name": "test-resource",
					},
				},
			},
			eventType: watch.Deleted,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs, err := emit.BuildCRLogRecord(tt.cr, tt.eventType, timestamp, clusterName)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Verify log structure
			assert.Equal(t, 1, logs.ResourceLogs().Len())
			resourceLogs := logs.ResourceLogs().At(0)

			// Verify resource attributes
			resourceAttrs := resourceLogs.Resource().Attributes()
			cluster, ok := resourceAttrs.Get(emit.AttrK8sClusterName)
			require.True(t, ok, "resource should have cluster name attribute")
			assert.Equal(t, clusterName, cluster.Str())

			if tt.cr.GetNamespace() != "" {
				ns, ok := resourceAttrs.Get(emit.AttrK8sNamespaceName)
				require.True(t, ok, "resource should have namespace attribute for namespaced resources")
				assert.Equal(t, tt.cr.GetNamespace(), ns.Str())
			}

			assert.Equal(t, 1, resourceLogs.ScopeLogs().Len())

			scopeLogs := resourceLogs.ScopeLogs().At(0)
			assert.Equal(t, emit.ScopeName, scopeLogs.Scope().Name())
			assert.Equal(t, 1, scopeLogs.LogRecords().Len())

			logRecord := scopeLogs.LogRecords().At(0)

			// Verify timestamp
			assert.NotZero(t, logRecord.ObservedTimestamp())

			// Verify body structure
			bodyMap := logRecord.Body().Map()
			assert.True(t, bodyMap.Len() >= 2, "body should have at least 'object' and 'type'")

			// Verify event type
			eventTypeVal, ok := bodyMap.Get("type")
			require.True(t, ok, "body should have 'type' field")
			assert.Equal(t, string(tt.eventType), eventTypeVal.Str())

			// Verify object exists
			objectVal, ok := bodyMap.Get("object")
			require.True(t, ok, "body should have 'object' field")
			objectMap := objectVal.Map()
			assert.True(t, objectMap.Len() > 0, "object should not be empty")

			// Verify attributes
			attrs := logRecord.Attributes()
			kind, ok := attrs.Get(emit.AttrK8sResourceName)
			require.True(t, ok)
			assert.Equal(t, tt.cr.GetKind(), kind.Str())

			group, ok := attrs.Get(emit.AttrK8sResourceGroup)
			require.True(t, ok)
			assert.Equal(t, tt.cr.GroupVersionKind().Group, group.Str())

			domain, ok := attrs.Get(emit.AttrEventDomain)
			require.True(t, ok)
			assert.Equal(t, emit.EventDomainK8s, domain.Str())

			assert.Equal(t, emit.EventNameCR, logRecord.EventName())

			objectName, ok := attrs.Get(emit.AttrK8sObjectName)
			require.True(t, ok)
			assert.Equal(t, tt.cr.GetName(), objectName.Str())

			if tt.cr.GetNamespace() != "" {
				ns, ok := attrs.Get(emit.AttrK8sNamespaceName)
				require.True(t, ok, "log record should have namespace attribute")
				assert.Equal(t, tt.cr.GetNamespace(), ns.Str())
			} else {
				_, ok := attrs.Get(emit.AttrK8sNamespaceName)
				assert.False(t, ok, "namespace attribute should not exist for cluster-scoped resources")
			}
		})
	}
}

func TestBuildCRDLogRecord(t *testing.T) {
	timestamp := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)
	clusterName := "test-cluster"

	tests := []struct {
		name      string
		crd       *unstructured.Unstructured
		eventType watch.EventType
		wantErr   bool
	}{
		{
			name: "CRD added",
			crd: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apiextensions.k8s.io/v1",
					"kind":       "CustomResourceDefinition",
					"metadata": map[string]interface{}{
						"name": "policyservers.policies.kubewarden.io",
					},
					"spec": map[string]interface{}{
						"group": "policies.kubewarden.io",
						"names": map[string]interface{}{
							"kind":   "PolicyServer",
							"plural": "policyservers",
						},
						"scope": "Namespaced",
						"versions": []interface{}{
							map[string]interface{}{
								"name":    "v1",
								"storage": true,
								"served":  true,
							},
						},
					},
				},
			},
			eventType: watch.Added,
			wantErr:   false,
		},
		{
			name: "CRD deleted",
			crd: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apiextensions.k8s.io/v1",
					"kind":       "CustomResourceDefinition",
					"metadata": map[string]interface{}{
						"name": "volumes.longhorn.io",
					},
					"spec": map[string]interface{}{
						"group": "longhorn.io",
						"names": map[string]interface{}{
							"kind":   "Volume",
							"plural": "volumes",
						},
						"scope": "Namespaced",
					},
				},
			},
			eventType: watch.Deleted,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs, err := emit.BuildCRDLogRecord(tt.crd, tt.eventType, timestamp, clusterName)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			assert.Equal(t, 1, logs.ResourceLogs().Len())
			resourceLogs := logs.ResourceLogs().At(0)

			// CRDs are cluster-scoped — only cluster name in resource attributes
			resourceAttrs := resourceLogs.Resource().Attributes()
			cluster, ok := resourceAttrs.Get(emit.AttrK8sClusterName)
			require.True(t, ok, "CRD should have cluster name attribute")
			assert.Equal(t, clusterName, cluster.Str())
			assert.Equal(t, 1, resourceAttrs.Len(), "CRD should only have cluster name attribute (cluster-scoped)")

			assert.Equal(t, 1, resourceLogs.ScopeLogs().Len())

			scopeLogs := resourceLogs.ScopeLogs().At(0)
			assert.Equal(t, emit.ScopeName, scopeLogs.Scope().Name())
			assert.Equal(t, 1, scopeLogs.LogRecords().Len())

			logRecord := scopeLogs.LogRecords().At(0)
			assert.NotZero(t, logRecord.ObservedTimestamp())

			bodyMap := logRecord.Body().Map()
			assert.True(t, bodyMap.Len() >= 2, "body should have at least 'object' and 'type'")

			eventTypeVal, ok := bodyMap.Get("type")
			require.True(t, ok, "body should have 'type' field")
			assert.Equal(t, string(tt.eventType), eventTypeVal.Str())

			objectVal, ok := bodyMap.Get("object")
			require.True(t, ok, "body should have 'object' field")
			objectMap := objectVal.Map()
			assert.True(t, objectMap.Len() > 0, "object should not be empty")

			attrs := logRecord.Attributes()

			kind, ok := attrs.Get(emit.AttrK8sResourceName)
			require.True(t, ok)
			assert.Equal(t, "CustomResourceDefinition", kind.Str())

			group, ok := attrs.Get(emit.AttrK8sResourceGroup)
			require.True(t, ok)
			assert.Equal(t, "apiextensions.k8s.io", group.Str())

			version, ok := attrs.Get(emit.AttrK8sResourceVersion)
			require.True(t, ok)
			assert.Equal(t, "v1", version.Str())

			domain, ok := attrs.Get(emit.AttrEventDomain)
			require.True(t, ok)
			assert.Equal(t, emit.EventDomainK8s, domain.Str())

			assert.Equal(t, emit.EventNameCRD, logRecord.EventName())

			objectName, ok := attrs.Get(emit.AttrK8sObjectName)
			require.True(t, ok)
			assert.Equal(t, tt.crd.GetName(), objectName.Str())

			_, ok = attrs.Get(emit.AttrK8sNamespaceName)
			assert.False(t, ok, "CRD should not have namespace attribute (cluster-scoped)")
		})
	}
}

func TestFormatGVRKey(t *testing.T) {
	tests := []struct {
		name string
		gvr  schema.GroupVersionResource
		want string
	}{
		{
			name: "standard GVR",
			gvr: schema.GroupVersionResource{
				Group:    "policies.kubewarden.io",
				Version:  "v1",
				Resource: "policyservers",
			},
			want: "policies.kubewarden.io/v1/policyservers",
		},
		{
			name: "core group (empty string)",
			gvr: schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods",
			},
			want: "/v1/pods",
		},
		{
			name: "beta version",
			gvr: schema.GroupVersionResource{
				Group:    "example.com",
				Version:  "v1beta1",
				Resource: "myresources",
			},
			want: "example.com/v1beta1/myresources",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := emit.FormatGVRKey(tt.gvr)
			assert.Equal(t, tt.want, got)
		})
	}
}
