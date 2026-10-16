package pmetricsender_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/ddcompat/servicecheck"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/pmetricsender"
)

const shopNamespace = "kube_namespace:shop"

func TestGauge(t *testing.T) {
	s := pmetricsender.New(pcommon.Timestamp(42))
	s.Gauge("kubernetes_state.container.restarts", 3, "node-1-prod", []string{
		shopNamespace, "pod_name:web-0", "uid:pod-uid", "kube_container_name:app",
		"container_id:containerd://abc123", "kube_cluster_name:prod", "kube_namespace:other",
	})
	s.Gauge("kubernetes_state.container.restarts", 1, "", []string{shopNamespace, "pod_name:web-1"})
	s.Gauge("kubernetes_state.node.count", 2, "", []string{"uid:not-a-pod"})

	md := s.Metrics()
	require.Equal(t, 1, md.ResourceMetrics().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).Resource().Attributes().Len())
	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	assert.Empty(t, sm.Scope().Name())
	require.Equal(t, 2, sm.Metrics().Len())

	restarts := sm.Metrics().At(0)
	assert.Equal(t, "kubernetes_state.container.restarts", restarts.Name())
	require.Equal(t, 2, restarts.Gauge().DataPoints().Len())
	dp := restarts.Gauge().DataPoints().At(0)
	assert.Equal(t, pcommon.Timestamp(42), dp.Timestamp())
	assert.Equal(t, 3.0, dp.DoubleValue())
	assert.Equal(t, map[string]any{
		"kube_namespace":      "shop",
		"k8s.namespace.name":  "shop",
		"pod_name":            "web-0",
		"k8s.pod.name":        "web-0",
		"uid":                 "pod-uid",
		"k8s.pod.uid":         "pod-uid",
		"kube_container_name": "app",
		"k8s.container.name":  "app",
		"container_id":        "containerd://abc123",
		"container.id":        "abc123",
		"kube_cluster_name":   "prod",
		"sts_host":            "node-1-prod",
		"interval":            "0",
	}, dp.Attributes().AsRaw())

	hostless := restarts.Gauge().DataPoints().At(1).Attributes().AsRaw()
	assert.NotContains(t, hostless, pmetricsender.HostAttribute)
	assert.Equal(t, "0", hostless[pmetricsender.IntervalAttribute])

	node := sm.Metrics().At(1).Gauge().DataPoints().At(0).Attributes().AsRaw()
	assert.NotContains(t, node, "k8s.pod.uid")
}

func TestServiceCheckIsDropped(t *testing.T) {
	s := pmetricsender.New(pcommon.Timestamp(0))
	s.ServiceCheck("kubernetes_state.node.ready", servicecheck.ServiceCheckOK, "node-1", []string{"node:node-1"}, "")
	assert.Equal(t, 0, s.Metrics().MetricCount())
}

func TestTagWithoutValue(t *testing.T) {
	s := pmetricsender.New(pcommon.Timestamp(1))
	s.Gauge("kubernetes_state.pod.count", 1, "", []string{"orphaned", shopNamespace})

	attrs := s.Metrics().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).Attributes()
	v, ok := attrs.Get("label_orphaned")
	require.True(t, ok)
	assert.Equal(t, "true", v.Str())
	_, ok = attrs.Get("orphaned")
	assert.False(t, ok)
}
