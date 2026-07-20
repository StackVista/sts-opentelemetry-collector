//nolint:testpackage
package k8sresourcereceiver

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestExtractManagerUrl(t *testing.T) {
	deployment := makeDeploymentWithEnv("cluster-register", map[string]interface{}{
		"name":  "CATTLE_SERVER",
		"value": "https://rancher.example.com",
	})

	enrichment := rancherManagerURLEnrichment()
	value, found, unsupported := enrichment.Extract(deployment)

	require.False(t, unsupported)
	require.True(t, found)
	assert.Equal(t, "https://rancher.example.com", value)
}

func TestExtractManagerUrlReportsUnsupportedValueFrom(t *testing.T) {
	deployment := makeDeploymentWithEnv("cluster-register", map[string]interface{}{
		"name": "CATTLE_SERVER",
		"valueFrom": map[string]interface{}{
			"fieldRef": map[string]interface{}{"fieldPath": "metadata.name"},
		},
	})

	enrichment := rancherManagerURLEnrichment()
	_, found, unsupported := enrichment.Extract(deployment)

	assert.False(t, found)
	assert.True(t, unsupported)
}

func TestExtractManagerUrlReturnsMissingForAbsentContainer(t *testing.T) {
	deployment := makeDeploymentWithEnv("other-container", map[string]interface{}{
		"name":  "CATTLE_SERVER",
		"value": "https://rancher.example.com",
	})

	enrichment := rancherManagerURLEnrichment()
	_, found, unsupported := enrichment.Extract(deployment)

	assert.False(t, found)
	assert.False(t, unsupported)
}

func TestExtractManagerUrlReturnsMissingForAbsentEnv(t *testing.T) {
	deployment := makeDeploymentWithEnv("cluster-register", map[string]interface{}{
		"name":  "OTHER_ENV",
		"value": "https://rancher.example.com",
	})

	enrichment := rancherManagerURLEnrichment()
	_, found, unsupported := enrichment.Extract(deployment)

	assert.False(t, found)
	assert.False(t, unsupported)
}

func TestExtractManagerUrlReturnsMissingForUnexpectedObjectShape(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"name":      "cattle-cluster-agent",
			"namespace": "cattle-system",
		},
	}}

	enrichment := rancherManagerURLEnrichment()
	_, found, unsupported := enrichment.Extract(obj)

	assert.False(t, found)
	assert.False(t, unsupported)
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

func TestExtractClusterName(t *testing.T) {
	deployment := makeSecret("namespace", "ranch-cluster")

	enrichment := rancherClusterIDEnrichment()
	value, found, unsupported := enrichment.Extract(deployment)

	require.False(t, unsupported)
	require.True(t, found)
	assert.Equal(t, "ranch-cluster", value)
}

func TestExtractClusterNameReportsMissingForAbsentKey(t *testing.T) {
	deployment := makeSecret("other", "clusterid")

	enrichment := rancherClusterIDEnrichment()
	_, found, unsupported := enrichment.Extract(deployment)

	require.False(t, unsupported)
	require.False(t, found)
}

func makeSecret(key, value string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]interface{}{
			"name":      "cattle-credentials-9b91ebd69c",
			"namespace": "cattle-system",
		},
		"type": "Opaque",
		"data": map[string]interface{}{
			key: base64.StdEncoding.EncodeToString([]byte(value)), // "cmFuY2gtY2x1c3Rlcgo=",
		},
	}}

}
