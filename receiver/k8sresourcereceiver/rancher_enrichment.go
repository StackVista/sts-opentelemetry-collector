package k8sresourcereceiver

import (
	"encoding/base64"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// This file contains all Rancher Manager URL and Harvester Cluster ID
// enrichment logic.  When enabled, the receiver reads CATTLE_SERVER from the
// cattle-cluster-agent Deployment in cattle-system and attaches the URL as a
// resource attribute on all emitted log records, enabling Rancher Manager links
// in the platform.
//
// Everything Rancher-specific is isolated here for easy identification and
// removal if the design changes in future.

// rancherEnrichmentConfig is the Rancher enrichment sub-config embedded in Config.
type rancherEnrichmentConfig struct {
	// Enabled activates Rancher Manager URL enrichment. Defaults to true in the
	// helm chart: on non-Rancher clusters cattle-cluster-agent is absent so the
	// attribute is simply not set — no harm done.
	Enabled bool `mapstructure:"enabled"`
}

//nolint:gosec
const (
	rancherManagerURLKey = "rancher.manager.url"
	rancherClusterIDKey  = "rancher.cluster.id"
	agentDeploymentName  = "cattle-cluster-agent"
	agentContainerName   = "cluster-register"
	agentEnvVar          = "CATTLE_SERVER"
	agentResource        = "deployments"
	secretName           = "cattle-credentials-.*"
	secretResource       = "secrets"
	rancherNamespace     = "cattle-system"
	rancherAPIGroup      = ""
)

type RancherManagerURLEnrichment struct{}

func (r RancherManagerURLEnrichment) Key() string {
	return rancherManagerURLKey
}

func (r RancherManagerURLEnrichment) ValueFrom() ResourceAttributeValueFrom {
	return ResourceAttributeValueFrom{
		K8sObjectSource{
			Resource:  agentResource,
			Name:      agentDeploymentName,
			Group:     rancherAPIGroup,
			Namespace: rancherNamespace,
		},
	}
}

func (r RancherManagerURLEnrichment) ApplyTo() ResourceAttributeApplyTo {
	return ResourceAttributeApplyTo{
		APIGroups: []string{"*"},
	}
}

// searches spec.template.spec.containers then
// spec.template.spec.initContainers for a container named containerName, and
// returns the static value of the env var named envName within that container.
// Returns (value, found, unsupported): unsupported=true when the env entry
// uses valueFrom (dynamic reference) rather than a static literal value.
// Once the named container is found in a slice, the search does not continue
// into subsequent slices even if the env var is absent there.
func (r RancherManagerURLEnrichment) Extract(obj *unstructured.Unstructured) (string, bool, bool) {
	for _, path := range [][]string{
		{specKey, templateKey, specKey, specContainersKey},
		{specKey, templateKey, specKey, "initContainers"},
	} {
		containers, found, _ := unstructured.NestedSlice(obj.Object, path...)
		if !found {
			continue
		}
		for _, rawContainer := range containers {
			container, ok := rawContainer.(map[string]interface{})
			if !ok {
				continue
			}
			name, _, _ := unstructured.NestedString(container, "name")
			if name != agentContainerName {
				continue
			}
			// Container matched — search for the env var here; do not fall through to initContainers.
			envs, found, _ := unstructured.NestedSlice(container, "env")
			if !found {
				return "", false, false
			}
			for _, rawEnv := range envs {
				env, ok := rawEnv.(map[string]interface{})
				if !ok {
					continue
				}
				name, _, _ := unstructured.NestedString(env, "name")
				if name != agentEnvVar {
					continue
				}
				if _, found, _ := unstructured.NestedMap(env, "valueFrom"); found {
					return "", false, true
				}
				value, found, _ := unstructured.NestedString(env, "value")
				return value, found, false
			}
			return "", false, false
		}
	}
	return "", false, false
}

func rancherManagerURLEnrichment() ResourceAttributeEnrichment {
	return RancherManagerURLEnrichment{}
}

type RancherClusterIDEnrichment struct{}

func (r RancherClusterIDEnrichment) Key() string {
	return rancherClusterIDKey
}

func (r RancherClusterIDEnrichment) ValueFrom() ResourceAttributeValueFrom {
	return ResourceAttributeValueFrom{
		K8sObjectSource{
			Resource:  secretResource,
			Name:      secretName,
			Group:     rancherAPIGroup,
			Namespace: rancherNamespace,
		},
	}
}

func (r RancherClusterIDEnrichment) ApplyTo() ResourceAttributeApplyTo {
	return ResourceAttributeApplyTo{
		APIGroups: []string{"*"},
	}
}

func (r RancherClusterIDEnrichment) Extract(obj *unstructured.Unstructured) (string, bool, bool) {
	b64Cluster, found, err := unstructured.NestedString(obj.Object, "data", "namespace")
	if err != nil {
		return "", false, true
	}
	cluster, err := base64.StdEncoding.DecodeString(b64Cluster)
	return string(cluster), found, err != nil
}

func rancherClusterIDEnrichment() ResourceAttributeEnrichment {
	return RancherClusterIDEnrichment{}
}
