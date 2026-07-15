package k8sresourcereceiver

// This file contains all Rancher Manager URL enrichment logic. When enabled,
// the receiver reads CATTLE_SERVER from the cattle-cluster-agent Deployment in
// cattle-system and attaches the URL as a resource attribute on all emitted
// log records, enabling Rancher Manager links in the platform.
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

const (
	rancherAttributeKey   = "rancher.manager.url"
	rancherDeploymentName = "cattle-cluster-agent"
	rancherContainerName  = "cluster-register"
	rancherEnvVar         = "CATTLE_SERVER"
	rancherNamespace      = "cattle-system"
	rancherAPIGroup       = ""
	rancherResource       = "deployments"
)

func rancherResourceAttributeEnrichment() ResourceAttributeEnrichment {
	return ResourceAttributeEnrichment{
		Key: rancherAttributeKey,
		ValueFrom: ResourceAttributeValueFrom{
			K8sContainerEnv: &K8sContainerEnvSource{
				Object: K8sObjectSource{
					Name:      rancherDeploymentName,
					Group:     rancherAPIGroup,
					Resource:  rancherResource,
					Namespace: rancherNamespace,
				},
				Container: rancherContainerName,
				Env:       rancherEnvVar,
			},
		},
		// Empty Resources matches all resource types; wildcard APIGroup matches every group.
		ApplyTo: ResourceAttributeApplyTo{
			APIGroups: []string{"*"},
		},
	}
}
