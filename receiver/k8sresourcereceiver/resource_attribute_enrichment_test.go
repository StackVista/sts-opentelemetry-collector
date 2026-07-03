//nolint:testpackage
package k8sresourcereceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestExtractStaticContainerEnv(t *testing.T) {
	deployment := makeDeploymentWithEnv("cattle-cluster-agent", map[string]interface{}{
		"name":  "CATTLE_SERVER",
		"value": "https://rancher.example.com",
	})

	value, found, unsupported := extractStaticContainerEnv(deployment, "cattle-cluster-agent", "CATTLE_SERVER")

	require.False(t, unsupported)
	require.True(t, found)
	assert.Equal(t, "https://rancher.example.com", value)
}

func TestExtractStaticContainerEnvReportsUnsupportedValueFrom(t *testing.T) {
	deployment := makeDeploymentWithEnv("cattle-cluster-agent", map[string]interface{}{
		"name": "CATTLE_SERVER",
		"valueFrom": map[string]interface{}{
			"fieldRef": map[string]interface{}{"fieldPath": "metadata.name"},
		},
	})

	_, found, unsupported := extractStaticContainerEnv(deployment, "cattle-cluster-agent", "CATTLE_SERVER")

	assert.False(t, found)
	assert.True(t, unsupported)
}

func TestExtractStaticContainerEnvReturnsMissingForAbsentContainer(t *testing.T) {
	deployment := makeDeploymentWithEnv("other-container", map[string]interface{}{
		"name":  "CATTLE_SERVER",
		"value": "https://rancher.example.com",
	})

	_, found, unsupported := extractStaticContainerEnv(deployment, "cattle-cluster-agent", "CATTLE_SERVER")

	assert.False(t, found)
	assert.False(t, unsupported)
}

func TestExtractStaticContainerEnvReturnsMissingForAbsentEnv(t *testing.T) {
	deployment := makeDeploymentWithEnv("cattle-cluster-agent", map[string]interface{}{
		"name":  "OTHER_ENV",
		"value": "https://rancher.example.com",
	})

	_, found, unsupported := extractStaticContainerEnv(deployment, "cattle-cluster-agent", "CATTLE_SERVER")

	assert.False(t, found)
	assert.False(t, unsupported)
}

func TestExtractStaticContainerEnvReturnsMissingForUnexpectedObjectShape(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"name":      "cattle-cluster-agent",
			"namespace": "cattle-system",
		},
	}}

	_, found, unsupported := extractStaticContainerEnv(obj, "cattle-cluster-agent", "CATTLE_SERVER")

	assert.False(t, found)
	assert.False(t, unsupported)
}

func TestResourceAttributeApplyToMatchesGroupsAndResources(t *testing.T) {
	applyTo := ResourceAttributeApplyTo{APIGroups: []string{"kubevirt.io"}, Resources: []string{"virtualmachines"}}

	assert.True(t, applyTo.matches(schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachines"}))
	assert.False(t, applyTo.matches(schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "pods"}))
	assert.False(t, applyTo.matches(schema.GroupVersionResource{Group: "harvesterhci.io", Version: "v1", Resource: "virtualmachines"}))
}

func makeDeploymentWithEnv(containerName string, env map[string]interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]interface{}{
			"name":      "cattle-cluster-agent",
			"namespace": "cattle-system",
		},
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"spec": map[string]interface{}{
					"containers": []interface{}{
						map[string]interface{}{
							"name": containerName,
							"env":  []interface{}{env},
						},
					},
				},
			},
		},
	}}
}
