package stsk8stopologyexporter

import (
	"sort"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8stopologyexporter/internal/apiserver"
	"go.uber.org/zap"
	appsV1 "k8s.io/api/apps/v1"
	batchV1 "k8s.io/api/batch/v1"
	batchV1B1 "k8s.io/api/batch/v1beta1"
	coreV1 "k8s.io/api/core/v1"
	extensionsV1B "k8s.io/api/extensions/v1beta1"
	netV1 "k8s.io/api/networking/v1"
	storageV1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/version"
)

// cacheClient serves the vendored collectors from a store view instead of the
// Kubernetes API.
type cacheClient struct {
	objects map[objectKey]storedObject
	logger  *zap.Logger
}

var _ apiserver.APICollectorClient = (*cacheClient)(nil)

// listTyped converts the matching objects in a stable order. version is
// ignored when empty. Unconvertible objects are skipped.
func listTyped[T any](c *cacheClient, group, kind, version string) []T {
	keys := make([]objectKey, 0)
	for key, stored := range c.objects {
		if key.group == group && key.kind == kind && (version == "" || stored.version == version) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].namespace != keys[j].namespace {
			return keys[i].namespace < keys[j].namespace
		}
		return keys[i].name < keys[j].name
	})
	out := make([]T, 0, len(keys))
	for _, key := range keys {
		var typed T
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(c.objects[key].object, &typed); err != nil {
			c.logger.Warn("Skipping Kubernetes object that cannot be converted",
				zap.String("kind", kind), zap.String("namespace", key.namespace),
				zap.String("name", key.name), zap.Error(err))
			continue
		}
		out = append(out, typed)
	}
	return out
}

func (c *cacheClient) GetDaemonSets() ([]appsV1.DaemonSet, error) {
	return listTyped[appsV1.DaemonSet](c, "apps", "DaemonSet", ""), nil
}

func (c *cacheClient) GetReplicaSets() ([]appsV1.ReplicaSet, error) {
	return listTyped[appsV1.ReplicaSet](c, "apps", "ReplicaSet", ""), nil
}

func (c *cacheClient) GetDeployments() ([]appsV1.Deployment, error) {
	return listTyped[appsV1.Deployment](c, "apps", "Deployment", ""), nil
}

func (c *cacheClient) GetStatefulSets() ([]appsV1.StatefulSet, error) {
	return listTyped[appsV1.StatefulSet](c, "apps", "StatefulSet", ""), nil
}

func (c *cacheClient) GetJobs() ([]batchV1.Job, error) {
	return listTyped[batchV1.Job](c, "batch", "Job", ""), nil
}

func (c *cacheClient) GetCronJobsV1B1() ([]batchV1B1.CronJob, error) {
	return listTyped[batchV1B1.CronJob](c, "batch", "CronJob", "v1beta1"), nil
}

func (c *cacheClient) GetCronJobsV1() ([]batchV1.CronJob, error) {
	return listTyped[batchV1.CronJob](c, "batch", "CronJob", "v1"), nil
}

//nolint:staticcheck // required by the vendored APICollectorClient interface
func (c *cacheClient) GetEndpoints() ([]coreV1.Endpoints, error) {
	return listTyped[coreV1.Endpoints](c, "", "Endpoints", ""), nil //nolint:staticcheck // see above
}

func (c *cacheClient) GetNodes() ([]coreV1.Node, error) {
	return listTyped[coreV1.Node](c, "", "Node", ""), nil
}

func (c *cacheClient) GetPods() ([]coreV1.Pod, error) {
	return listTyped[coreV1.Pod](c, "", "Pod", ""), nil
}

func (c *cacheClient) GetServices() ([]coreV1.Service, error) {
	return listTyped[coreV1.Service](c, "", "Service", ""), nil
}

func (c *cacheClient) GetIngressesExtV1B1() ([]extensionsV1B.Ingress, error) {
	return listTyped[extensionsV1B.Ingress](c, "extensions", "Ingress", ""), nil
}

func (c *cacheClient) GetIngressesNetV1() ([]netV1.Ingress, error) {
	return listTyped[netV1.Ingress](c, "networking.k8s.io", "Ingress", ""), nil
}

func (c *cacheClient) GetConfigMaps() ([]coreV1.ConfigMap, error) {
	return listTyped[coreV1.ConfigMap](c, "", "ConfigMap", ""), nil
}

func (c *cacheClient) GetSecrets() ([]coreV1.Secret, error) {
	return listTyped[coreV1.Secret](c, "", "Secret", ""), nil
}

func (c *cacheClient) GetNamespaces() ([]coreV1.Namespace, error) {
	return listTyped[coreV1.Namespace](c, "", "Namespace", ""), nil
}

func (c *cacheClient) GetPersistentVolumes() ([]coreV1.PersistentVolume, error) {
	return listTyped[coreV1.PersistentVolume](c, "", "PersistentVolume", ""), nil
}

func (c *cacheClient) GetPersistentVolumeClaims() ([]coreV1.PersistentVolumeClaim, error) {
	return listTyped[coreV1.PersistentVolumeClaim](c, "", "PersistentVolumeClaim", ""), nil
}

func (c *cacheClient) GetVolumeAttachments() ([]storageV1.VolumeAttachment, error) {
	return listTyped[storageV1.VolumeAttachment](c, "storage.k8s.io", "VolumeAttachment", ""), nil
}

// GetVersion selects the batch/v1 CronJob and networking.k8s.io/v1 Ingress
// collectors, matching the API versions the Cluster Observer watches.
func (c *cacheClient) GetVersion() (*version.Info, error) {
	return &version.Info{Major: "1", Minor: "21"}, nil
}
