package ksm

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/ddcompat/sender"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/pmetricsender"
)

func TestPipelineToPmetric(t *testing.T) {
	replicas := int32(2)
	ctrl := true
	check := New(&KSMConfig{Collectors: chartCollectors}, Options{ClusterName: "prod_eu", ClusterID: "cid"})
	stores := startFakeKSM(t, check,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "shop", UID: "ns-uid"}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-1", UID: "node-uid", Labels: map[string]string{"topology.kubernetes.io/zone": "eu-1a"}},
			Status: corev1.NodeStatus{
				Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
				Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi")},
			},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "shop", UID: "dep-uid", Labels: map[string]string{"app.kubernetes.io/name": "web"}},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
			Status:     appsv1.DeploymentStatus{Replicas: 2, AvailableReplicas: 2},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "web-7c9f8d6b5-abcde", Namespace: "shop", UID: "pod-uid",
				OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-7c9f8d6b5", Controller: &ctrl}},
			},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
				Containers: []corev1.Container{{
					Name: "app", Image: "web:1",
					Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")}},
				}},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name: "app", Ready: true, RestartCount: 3,
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
				}},
			},
		},
	)

	var s *pmetricsender.Sender
	newSender := func() sender.Sender {
		s = pmetricsender.New(pcommon.NewTimestampFromTime(time.Now()))
		return s
	}
	collectWhen(t, check, stores, newSender, func() bool {
		md := s.Metrics()
		return find(md, "kubernetes_state.container.restarts") != nil &&
			find(md, "kubernetes_state.node.cpu_allocatable") != nil &&
			find(md, "kubernetes_state.deployment.replicas_desired") != nil
	})
	md := s.Metrics()

	restarts := find(md, "kubernetes_state.container.restarts")
	assert.Equal(t, 3.0, restarts.DoubleValue())
	attrs := restarts.Attributes().AsRaw()
	assert.Equal(t, "node-1-prod-eu", attrs[pmetricsender.HostAttribute])
	assert.Equal(t, "0", attrs[pmetricsender.IntervalAttribute])
	assert.Equal(t, "shop", attrs["kube_namespace"])
	assert.Equal(t, "shop", attrs["k8s.namespace.name"])
	assert.Equal(t, "web-7c9f8d6b5-abcde", attrs["pod_name"])
	assert.Equal(t, "web-7c9f8d6b5-abcde", attrs["k8s.pod.name"])
	assert.Equal(t, "pod-uid", attrs["k8s.pod.uid"])
	assert.Equal(t, "app", attrs["kube_container_name"])
	assert.Equal(t, "web-7c9f8d6b5", attrs["kube_replica_set"])
	assert.Equal(t, "web", attrs["kube_deployment"])
	assert.Equal(t, "web", attrs["k8s.deployment.name"])
	assert.Equal(t, "prod_eu", attrs["kube_cluster_name"])
	assert.Equal(t, "cid", attrs["orch_cluster_id"])
	assert.Equal(t, "Running", attrs["pod_phase"])

	waiting := find(md, "kubernetes_state.container.status_report.count.waiting")
	require.NotNil(t, waiting)
	assert.Equal(t, "CrashLoopBackOff", waiting.Attributes().AsRaw()["reason"])

	cpu := find(md, "kubernetes_state.node.cpu_allocatable")
	assert.Equal(t, 4.0, cpu.DoubleValue())
	assert.Equal(t, "node-1-prod-eu", cpu.Attributes().AsRaw()[pmetricsender.HostAttribute])
	assert.Equal(t, "eu-1a", cpu.Attributes().AsRaw()["kube_zone"])
	assert.Equal(t, "node-1", cpu.Attributes().AsRaw()["k8s.node.name"])

	dep := find(md, "kubernetes_state.deployment.replicas_desired")
	assert.Equal(t, 2.0, dep.DoubleValue())
	assert.Equal(t, "web", dep.Attributes().AsRaw()["kube_app_name"])

	assert.Nil(t, find(md, "kubernetes_state.node.ready"), "service checks are dropped")
	assert.NotNil(t, find(md, "kubernetes_state.node.by_condition"))

	// The agent aggregator keeps one gauge per context.
	seen := map[string]bool{}
	ms := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	for i := 0; i < ms.Len(); i++ {
		dps := ms.At(i).Gauge().DataPoints()
		for j := 0; j < dps.Len(); j++ {
			key := fmt.Sprint(ms.At(i).Name(), dps.At(j).Attributes().AsRaw())
			assert.False(t, seen[key], "duplicate series %s", key)
			seen[key] = true
		}
	}
}

func find(md pmetric.Metrics, name string) *pmetric.NumberDataPoint {
	ms := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	for i := 0; i < ms.Len(); i++ {
		if ms.At(i).Name() == name && ms.At(i).Gauge().DataPoints().Len() > 0 {
			dp := ms.At(i).Gauge().DataPoints().At(0)
			return &dp
		}
	}
	return nil
}
