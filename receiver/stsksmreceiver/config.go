package stsksmreceiver

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
)

const (
	authTypeServiceAccount = "serviceAccount"
	authTypeKubeConfig     = "kubeConfig"
)

// Config configures the cluster-agent-compatible Kubernetes state metrics receiver.
type Config struct {
	// AuthType is how the receiver authenticates to the Kubernetes API:
	// serviceAccount or kubeConfig.
	AuthType string `mapstructure:"auth_type"`
	// ClusterName is the name the cluster agent is configured with. It is
	// lowercased, as the agent does.
	ClusterName string `mapstructure:"cluster_name"`
	// CollectionInterval is the interval between collections, the check interval.
	CollectionInterval time.Duration `mapstructure:"collection_interval"`
	// ResyncPeriod is the informer resync period, in whole seconds.
	ResyncPeriod time.Duration `mapstructure:"resync_period"`
	// Collectors are the kube-state-metrics resources to collect.
	Collectors []string `mapstructure:"collectors"`
	// LabelsAsTags and AnnotationsAsTags map, per resource kind, a label or
	// annotation to the tag it becomes, as the check's instance options do.
	LabelsAsTags      map[string]map[string]string `mapstructure:"labels_as_tags"`
	AnnotationsAsTags map[string]map[string]string `mapstructure:"annotations_as_tags"`
	// K8sLeaderElector is the ID of a k8sleaderelector extension. When set, only
	// the leader collects.
	K8sLeaderElector *component.ID `mapstructure:"k8s_leader_elector"`
}

// defaultCollectors is the cluster agent chart's collector list.
func defaultCollectors() []string {
	return []string{
		"nodes", "pods", "services", "persistentvolumeclaims", "persistentvolumes", "namespaces", "endpoints",
		"daemonsets", "deployments", "replicasets", "statefulsets", "cronjobs", "jobs", "ingresses", "secrets",
		"resourcequotas", "replicationcontrollers", "limitranges", "horizontalpodautoscalers",
		"poddisruptionbudgets", "storageclasses", "volumeattachments",
	}
}

// namespaceKind is the labels_as_tags key for namespaces.
const namespaceKind = "namespace"

func (cfg *Config) Validate() error {
	switch cfg.AuthType {
	case authTypeServiceAccount, authTypeKubeConfig:
	default:
		return fmt.Errorf("auth_type must be %s or %s", authTypeServiceAccount, authTypeKubeConfig)
	}
	if strings.TrimSpace(cfg.ClusterName) == "" {
		return errors.New("cluster_name is required")
	}
	if cfg.CollectionInterval <= 0 {
		return errors.New("collection_interval must be positive")
	}
	if cfg.ResyncPeriod < time.Second {
		return errors.New("resync_period must be at least 1s")
	}
	if len(cfg.Collectors) == 0 {
		return errors.New("collectors must not be empty")
	}
	if slices.Contains(cfg.Collectors, "") {
		return errors.New("collectors must not contain an empty name")
	}
	for name, asTags := range map[string]map[string]map[string]string{
		"labels_as_tags": cfg.LabelsAsTags, "annotations_as_tags": cfg.AnnotationsAsTags,
	} {
		if _, ok := asTags[namespaceKind]["*"]; ok {
			// The KSM label join matches namespace metrics on "namespace" twice, so a wildcard panics on
			// any namespace without labels; the cluster agent crashes the same way.
			return fmt.Errorf("%s: a \"*\" wildcard for namespace is not supported, list the namespace labels instead", name)
		}
	}
	return nil
}

func (cfg *Config) clusterName() string {
	return strings.ToLower(cfg.ClusterName)
}
