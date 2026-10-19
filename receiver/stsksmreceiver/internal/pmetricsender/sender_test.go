package pmetricsender_test

import (
	"strings"
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

func TestRepeatedSeriesKeepLastValue(t *testing.T) {
	const name, op, pod = "kubernetes_state.pod.tolerations", "operator:Exists", "pod_name:a"
	s := pmetricsender.New(pcommon.Timestamp(0))
	s.Gauge(name, 1, "", []string{op, pod})
	s.Gauge(name, 2, "", []string{pod, op, pod})
	s.Gauge(name, 3, "node-1", []string{pod, op})

	dps := s.Metrics().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints()
	require.Equal(t, 2, dps.Len())
	assert.Equal(t, 2.0, dps.At(0).DoubleValue())
	assert.Equal(t, 3.0, dps.At(1).DoubleValue())
}

// Remote write turns attribute names into Prometheus label names, joining the
// values of names that collide, so an added attribute must not sanitise to a
// Datadog tag key.
func TestSemconvAttributesDoNotCollideWithTags(t *testing.T) {
	sanitise := func(name string) string {
		return strings.Map(func(r rune) rune {
			if r == '_' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || ('0' <= r && r <= '9') {
				return r
			}
			return '_'
		}, name)
	}
	for tag, attr := range pmetricsender.SemconvAttributes() {
		assert.NotEqual(t, tag, sanitise(attr), "%s and %s become the same label", tag, attr)
		for other := range pmetricsender.SemconvAttributes() {
			assert.NotEqual(t, other, sanitise(attr), "%s and tag %s become the same label", attr, other)
		}
	}
	for _, attr := range []string{"k8s.pod.uid", pmetricsender.HostAttribute, pmetricsender.IntervalAttribute} {
		_, isTag := pmetricsender.SemconvAttributes()[sanitise(attr)]
		assert.False(t, isTag, "%s becomes the label of a mapped tag", attr)
	}
}
