package emit_test

import (
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/emit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	testAPIVersionKey = "apiVersion"
	testKindKey       = "kind"
	testMetadataKey   = "metadata"
	testNameKey       = "name"
	testSpecKey       = "spec"
)

func TestBuildObjectLogRecord(t *testing.T) {
	timestamp := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)
	clusterName := "test-cluster"

	tests := []struct {
		name      string
		obj       *unstructured.Unstructured
		eventType watch.EventType
		eventName string
	}{
		{
			name: "CR-shape: namespaced CR with EventNameCR",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					testAPIVersionKey: "policies.kubewarden.io/v1",
					testKindKey:       "PolicyServer",
					testMetadataKey: map[string]interface{}{
						testNameKey: "default",
						"namespace": "kubewarden",
						"uid":       "abc-123",
					},
					testSpecKey: map[string]interface{}{
						"image": "ghcr.io/kubewarden/policy-server:v1.0.0",
					},
				},
			},
			eventType: watch.Added,
			eventName: emit.EventNameCR,
		},
		{
			name: "CR-shape: cluster-scoped CR without namespace",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					testAPIVersionKey: "policies.kubewarden.io/v1",
					testKindKey:       "ClusterAdmissionPolicy",
					testMetadataKey: map[string]interface{}{
						testNameKey: "pod-privileged",
						"uid":       "def-456",
					},
				},
			},
			eventType: watch.Modified,
			eventName: emit.EventNameCR,
		},
		{
			name: "Object-shape: static pod with EventNameObject",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					testAPIVersionKey: "v1",
					testKindKey:       "Pod",
					testMetadataKey: map[string]interface{}{
						testNameKey: "nginx",
						"namespace": "default",
					},
				},
			},
			eventType: watch.Deleted,
			eventName: emit.EventNameObject,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs, err := emit.BuildObjectLogRecord(tt.obj, tt.eventType, timestamp, clusterName, tt.eventName, nil)
			require.NoError(t, err)

			assert.Equal(t, 1, logs.ResourceLogs().Len())
			resourceLogs := logs.ResourceLogs().At(0)

			resourceAttrs := resourceLogs.Resource().Attributes()
			cluster, ok := resourceAttrs.Get(emit.AttrK8sClusterName)
			require.True(t, ok, "resource should have cluster name attribute")
			assert.Equal(t, clusterName, cluster.Str())

			if tt.obj.GetNamespace() != "" {
				ns, ok := resourceAttrs.Get(emit.AttrK8sNamespaceName)
				require.True(t, ok, "resource should have namespace attribute for namespaced resources")
				assert.Equal(t, tt.obj.GetNamespace(), ns.Str())
			}

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
			kind, ok := attrs.Get(emit.AttrK8sResourceKind)
			require.True(t, ok)
			assert.Equal(t, tt.obj.GetKind(), kind.Str())

			group, ok := attrs.Get(emit.AttrK8sResourceGroup)
			require.True(t, ok)
			assert.Equal(t, tt.obj.GroupVersionKind().Group, group.Str())

			domain, ok := attrs.Get(emit.AttrEventDomain)
			require.True(t, ok)
			assert.Equal(t, emit.EventDomainK8s, domain.Str())

			assert.Equal(t, tt.eventName, logRecord.EventName())

			objectName, ok := attrs.Get(emit.AttrK8sObjectName)
			require.True(t, ok)
			assert.Equal(t, tt.obj.GetName(), objectName.Str())

			if tt.obj.GetNamespace() != "" {
				ns, ok := attrs.Get(emit.AttrK8sNamespaceName)
				require.True(t, ok, "log record should have namespace attribute")
				assert.Equal(t, tt.obj.GetNamespace(), ns.Str())
			} else {
				_, ok := attrs.Get(emit.AttrK8sNamespaceName)
				assert.False(t, ok, "namespace attribute should not exist for cluster-scoped resources")
			}
		})
	}
}

func TestBuildObjectLogRecordAddsResourceAttributes(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name":      "nginx",
			"namespace": "default",
		},
	}}
	logs, err := emit.BuildObjectLogRecord(
		obj,
		watch.Added,
		time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		"test-cluster",
		emit.EventNameObject,
		map[string]string{"rancher.manager.url": "https://rancher.example.com"},
	)

	require.NoError(t, err)
	attrs := logs.ResourceLogs().At(0).Resource().Attributes()
	value, exists := attrs.Get("rancher.manager.url")
	require.True(t, exists)
	assert.Equal(t, "https://rancher.example.com", value.Str())
}

func TestBuildObjectLogRecordKeepsBuiltInResourceAttributes(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name":      "nginx",
			"namespace": "default",
		},
	}}
	logs, err := emit.BuildObjectLogRecord(
		obj,
		watch.Added,
		time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		"test-cluster",
		emit.EventNameObject,
		map[string]string{
			emit.AttrK8sClusterName:   "wrong-cluster",
			emit.AttrK8sNamespaceName: "wrong-namespace",
		},
	)

	require.NoError(t, err)
	attrs := logs.ResourceLogs().At(0).Resource().Attributes()
	cluster, exists := attrs.Get(emit.AttrK8sClusterName)
	require.True(t, exists)
	assert.Equal(t, "test-cluster", cluster.Str())
	namespace, exists := attrs.Get(emit.AttrK8sNamespaceName)
	require.True(t, exists)
	assert.Equal(t, "default", namespace.Str())
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
					testAPIVersionKey: "apiextensions.k8s.io/v1",
					testKindKey:       "CustomResourceDefinition",
					testMetadataKey: map[string]interface{}{
						testNameKey: "policyservers.policies.kubewarden.io",
					},
					testSpecKey: map[string]interface{}{
						"group": "policies.kubewarden.io",
						"names": map[string]interface{}{
							testKindKey: "PolicyServer",
							"plural":    "policyservers",
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
			logs, err := emit.BuildCRDLogRecord(tt.crd, tt.eventType, timestamp, clusterName, true)
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

			// CRD attributes should reflect the defined CR type, not the CRD API type itself.
			crdKind, _, _ := unstructured.NestedString(tt.crd.Object, "spec", "names", "kind")
			crdGroup, _, _ := unstructured.NestedString(tt.crd.Object, "spec", "group")

			kind, ok := attrs.Get(emit.AttrK8sResourceKind)
			require.True(t, ok)
			assert.Equal(t, crdKind, kind.Str())

			group, ok := attrs.Get(emit.AttrK8sResourceGroup)
			require.True(t, ok)
			assert.Equal(t, crdGroup, group.Str())

			version, ok := attrs.Get(emit.AttrK8sResourceVersion)
			require.True(t, ok)
			assert.Equal(t, "v1", version.Str())

			crsWatched, ok := attrs.Get(emit.AttrK8sCRDCRsWatched)
			require.True(t, ok)
			assert.True(t, crsWatched.Bool())

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
