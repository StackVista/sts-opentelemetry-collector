package ksm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDeploymentConditionAndHPATargetParity(t *testing.T) {
	replicas := int32(1)
	utilization := int32(80)
	s := collectParity(t, []string{"kubernetes_state.deployment.condition", "kubernetes_state.hpa.status_target_metric"},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "shop", UID: "dep-1"},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
			Status: appsv1.DeploymentStatus{Conditions: []appsv1.DeploymentCondition{
				{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue, Reason: "MinimumReplicasAvailable"},
			}},
		},
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "shop", UID: "hpa-1"},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				MaxReplicas: 3,
				Metrics: []autoscalingv2.MetricSpec{{
					Type: autoscalingv2.ContainerResourceMetricSourceType,
					ContainerResource: &autoscalingv2.ContainerResourceMetricSource{
						Name: corev1.ResourceCPU, Container: "app",
						Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: &utilization},
					},
				}},
			},
			Status: autoscalingv2.HorizontalPodAutoscalerStatus{
				CurrentMetrics: []autoscalingv2.MetricStatus{{
					Type: autoscalingv2.ContainerResourceMetricSourceType,
					ContainerResource: &autoscalingv2.ContainerResourceMetricStatus{
						Name: corev1.ResourceCPU, Container: "app",
						Current: autoscalingv2.MetricValueStatus{AverageValue: resource.NewMilliQuantity(250, resource.DecimalSI)},
					},
				}},
			},
		},
	)

	conditions := gaugeTags(s, "kubernetes_state.deployment.condition", "kube_deployment:web")
	assert.Len(t, conditions, 3, "one series per condition status")
	for _, tags := range conditions {
		assert.False(t, tagWithPrefix(tags, "reason:"), "%v", tags)
	}
	s.AssertMetric(t, "Gauge", "kubernetes_state.deployment.condition", 1, "",
		[]string{"condition:Available", "status:true", "kube_deployment:web"})

	for _, name := range []string{"kubernetes_state.hpa.spec_target_metric", "kubernetes_state.hpa.status_target_metric"} {
		series := gaugeTags(s, name, "horizontalpodautoscaler:web", "metric_name:cpu")
		require.Len(t, series, 1, name)
		assert.False(t, tagWithPrefix(series[0], "kube_container_name:"), "%s: %v", name, series[0])
		assert.False(t, tagWithPrefix(series[0], "container:"), "%s: %v", name, series[0])
	}
	s.AssertCalled(t, "Gauge", "kubernetes_state.hpa.status_target_metric", 0.25, "", mock.Anything)
}
