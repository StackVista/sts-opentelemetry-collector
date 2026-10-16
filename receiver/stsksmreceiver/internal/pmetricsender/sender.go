// Package pmetricsender turns the series produced by the KSM mapping layer
// into OTLP gauges.
package pmetricsender

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/ddcompat/servicecheck"
)

const (
	HostAttribute = "sts_host"
	// IntervalAttribute is the label the Receiver adds to agent check series.
	IntervalAttribute = "interval"

	podNameTag = "pod_name"
)

// semconv maps Datadog tag keys to the k8sclusterreceiver attribute emitted
// alongside them.
var semconv = map[string]string{ //nolint:gochecknoglobals
	"kube_namespace":              "k8s.namespace.name",
	podNameTag:                    "k8s.pod.name",
	"kube_container_name":         "k8s.container.name",
	"node":                        "k8s.node.name",
	"kube_deployment":             "k8s.deployment.name",
	"kube_replica_set":            "k8s.replicaset.name",
	"kube_stateful_set":           "k8s.statefulset.name",
	"kube_daemon_set":             "k8s.daemonset.name",
	"kube_job":                    "k8s.job.name",
	"kube_cronjob":                "k8s.cronjob.name",
	"kube_replication_controller": "k8s.replicationcontroller.name",
	"kube_service":                "k8s.service.name",
	"persistentvolume":            "k8s.persistentvolume.name",
	"persistentvolumeclaim":       "k8s.persistentvolumeclaim.name",
	"storageclass":                "k8s.storageclass.name",
	"resourcequota":               "k8s.resourcequota.name",
	"horizontalpodautoscaler":     "k8s.hpa.name",
	"kubelet_version":             "k8s.kubelet.version",
}

// Sender accumulates one collection run into a pmetric.Metrics.
type Sender struct {
	metrics pmetric.Metrics
	scope   pmetric.MetricSlice
	byName  map[string]pmetric.NumberDataPointSlice
	ts      pcommon.Timestamp
}

func New(ts pcommon.Timestamp) *Sender {
	md := pmetric.NewMetrics()
	scope := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	return &Sender{metrics: md, scope: scope, byName: map[string]pmetric.NumberDataPointSlice{}, ts: ts}
}

func (s *Sender) Metrics() pmetric.Metrics { return s.metrics }

func (s *Sender) Gauge(metric string, value float64, hostname string, tags []string) {
	dps, ok := s.byName[metric]
	if !ok {
		m := s.scope.AppendEmpty()
		m.SetName(metric)
		dps = m.SetEmptyGauge().DataPoints()
		s.byName[metric] = dps
	}
	dp := dps.AppendEmpty()
	dp.SetTimestamp(s.ts)
	dp.SetDoubleValue(value)
	putAttributes(dp.Attributes(), hostname, tags)
}

// ServiceCheck drops the check: the cluster agent's serializer never forwards them.
func (s *Sender) ServiceCheck(string, servicecheck.ServiceCheckStatus, string, []string, string) {}

func putAttributes(attrs pcommon.Map, hostname string, tags []string) {
	attrs.EnsureCapacity(2*len(tags) + 2)
	podName := false
	uid := ""
	for _, tag := range tags {
		k, v, _ := strings.Cut(tag, ":")
		// Datadog permits repeated keys; OTLP attributes do not. The last value wins.
		attrs.PutStr(k, v)
		if sc, ok := semconv[k]; ok {
			attrs.PutStr(sc, v)
		}
		switch k {
		case podNameTag:
			podName = true
		case "uid":
			uid = v
		case "container_id":
			// KSM reports <runtime>://<id>; container.id is the bare id.
			if _, id, found := strings.Cut(v, "://"); found {
				attrs.PutStr("container.id", id)
			}
		}
	}
	// KSM only sets uid alongside pod on pod and container series.
	if podName && uid != "" {
		attrs.PutStr("k8s.pod.uid", uid)
	}
	if hostname != "" {
		attrs.PutStr(HostAttribute, hostname)
	}
	attrs.PutStr(IntervalAttribute, "0")
}
