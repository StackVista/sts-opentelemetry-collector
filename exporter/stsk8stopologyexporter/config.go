package stsk8stopologyexporter

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	intakeSuffix          = "/stsAgent/intake"
	clusterTypeKubernetes = "kubernetes"
	clusterTypeOpenShift  = "openshift"
)

// Config configures the cluster-agent-compatible topology exporter.
type Config struct {
	// Endpoint is the Receiver intake URL, ending in /stsAgent/intake.
	Endpoint string              `mapstructure:"endpoint"`
	APIKey   configopaque.String `mapstructure:"api_key"`
	ProxyURL configopaque.String `mapstructure:"proxy_url"`
	TLS      TLSConfig           `mapstructure:"tls"`
	// TimeoutSettings bounds each intake request.
	TimeoutSettings exporterhelper.TimeoutConfig `mapstructure:",squash"`

	// ClusterName is the topology instance URL; it must match the cluster name
	// configured in the Kubernetes StackPack.
	ClusterName string `mapstructure:"cluster_name"`
	// ClusterType is kubernetes or openshift; it sets the cluster-type tags.
	ClusterType string `mapstructure:"cluster_type"`
	// InternalHostname identifies the producer to the topology sync. The sync
	// switches to an unseen producer immediately and ignores data from
	// producers that are no longer active, so the default, the pod hostname,
	// keeps a former leader's late requests out of its successor's snapshot.
	InternalHostname string `mapstructure:"internal_hostname"`

	// Interval between full topology snapshots.
	Interval time.Duration `mapstructure:"interval"`
	// SnapshotMaxAge stops sending when no complete Cluster Observer snapshot
	// has been received for this long, e.g. after losing leadership silently.
	SnapshotMaxAge time.Duration `mapstructure:"snapshot_max_age"`
	// HandoverDelay postpones the first snapshot after the observer becomes
	// ready, so requests a former leader still has in flight are processed first.
	HandoverDelay time.Duration `mapstructure:"handover_delay"`
	// CollectTimeout bounds one topology build.
	CollectTimeout time.Duration `mapstructure:"collect_timeout"`
	// MaxElementsPerRequest splits a snapshot into several ordered requests.
	MaxElementsPerRequest int `mapstructure:"max_elements_per_request"`
	// MaxAttempts is the number of attempts per request before the snapshot is abandoned.
	MaxAttempts int `mapstructure:"max_attempts"`

	// DiscoveryEnabled follows the platform's legacy-kubernetes-topology
	// capability: legacy topology is not sent while it is false.
	DiscoveryEnabled bool `mapstructure:"discovery_enabled"`

	Resources            ResourcesConfig `mapstructure:"resources"`
	ConfigMapMaxDataSize int             `mapstructure:"configmap_max_datasize"`
	CSIPVMapperEnabled   bool            `mapstructure:"csi_pv_mapper_enabled"`
}

// TLSConfig controls server certificate verification.
type TLSConfig struct {
	InsecureSkipVerify bool `mapstructure:"insecure_skip_verify"`
}

// ResourcesConfig mirrors the cluster-agent kubernetes_api_topology resources.
type ResourcesConfig struct {
	PersistentVolumes      bool `mapstructure:"persistentvolumes"`
	PersistentVolumeClaims bool `mapstructure:"persistentvolumeclaims"`
	Namespaces             bool `mapstructure:"namespaces"`
	ConfigMaps             bool `mapstructure:"configmaps"`
	Secrets                bool `mapstructure:"secrets"`
	DaemonSets             bool `mapstructure:"daemonsets"`
	Deployments            bool `mapstructure:"deployments"`
	ReplicaSets            bool `mapstructure:"replicasets"`
	StatefulSets           bool `mapstructure:"statefulsets"`
	Ingresses              bool `mapstructure:"ingresses"`
	Jobs                   bool `mapstructure:"jobs"`
	CronJobs               bool `mapstructure:"cronjobs"`
}

func (cfg *Config) Validate() error {
	if !strings.HasSuffix(cfg.Endpoint, intakeSuffix) {
		return fmt.Errorf("endpoint must end in %s", intakeSuffix)
	}
	if strings.TrimSpace(string(cfg.APIKey)) == "" ||
		strings.ContainsFunc(string(cfg.APIKey), func(r rune) bool { return r < 32 || r == 127 }) {
		return errors.New("api_key must contain a valid credential")
	}
	if strings.TrimSpace(cfg.ClusterName) == "" {
		return errors.New("cluster_name is required")
	}
	if cfg.ClusterType != clusterTypeKubernetes && cfg.ClusterType != clusterTypeOpenShift {
		return errors.New("cluster_type must be kubernetes or openshift")
	}
	if cfg.TimeoutSettings.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if cfg.Interval <= 0 || cfg.CollectTimeout <= 0 {
		return errors.New("interval and collect_timeout must be positive")
	}
	if cfg.SnapshotMaxAge < cfg.Interval {
		return errors.New("snapshot_max_age must not be shorter than interval")
	}
	if cfg.HandoverDelay < 0 {
		return errors.New("handover_delay must not be negative")
	}
	if cfg.MaxElementsPerRequest <= 0 || cfg.MaxAttempts <= 0 {
		return errors.New("max_elements_per_request and max_attempts must be positive")
	}
	if cfg.ConfigMapMaxDataSize <= 0 {
		return errors.New("configmap_max_datasize must be positive")
	}
	return nil
}
