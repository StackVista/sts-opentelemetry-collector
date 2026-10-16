package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAgentKSMCompat(t *testing.T) {
	tests := []struct {
		family   string
		labels   map[string]string
		keep     bool
		expected map[string]string
	}{
		{"kube_ingress_path", map[string]string{"path": "/", "path_type": "Prefix"}, true, map[string]string{"path": "/"}},
		{"kube_service_info", map[string]string{"cluster_ip": "10.0.0.1", "external_traffic_policy": "Cluster"}, true, map[string]string{"cluster_ip": "10.0.0.1"}},
		{"kube_pod_spec_volumes_persistentvolumeclaims_info", map[string]string{"volume": "data", "ephemeral": "false"}, true, map[string]string{"volume": "data"}},
		{"kube_pod_spec_volumes_persistentvolumeclaims_readonly", map[string]string{"volume": "scratch", "ephemeral": "true"}, false, nil},
		{"kube_deployment_status_condition", map[string]string{"condition": "Available", "reason": "MinimumReplicasAvailable"}, true, map[string]string{"condition": "Available"}},
		{"kube_horizontalpodautoscaler_spec_target_metric", map[string]string{"metric_name": "cpu", "container": "app"}, true, map[string]string{"metric_name": "cpu"}},
		{"kube_horizontalpodautoscaler_status_target_metric", map[string]string{"metric_name": "cpu", "container": "app"}, true, map[string]string{"metric_name": "cpu"}},
		{"kube_pod_info", map[string]string{"path_type": "kept", "reason": "kept"}, true, map[string]string{"path_type": "kept", "reason": "kept"}},
	}
	for _, tt := range tests {
		t.Run(tt.family, func(t *testing.T) {
			keep := agentKSMCompat(tt.family, tt.labels)
			assert.Equal(t, tt.keep, keep)
			if keep {
				assert.Equal(t, tt.expected, tt.labels)
			}
		})
	}
}
