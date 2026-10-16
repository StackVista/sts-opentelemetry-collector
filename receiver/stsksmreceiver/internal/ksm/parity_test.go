package ksm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/ddcompat/mocksender"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/ddcompat/sender"
)

var chartCollectors = []string{
	"nodes", "pods", "services", "persistentvolumeclaims", "persistentvolumes", "namespaces", "endpoints",
	"daemonsets", "deployments", "replicasets", "statefulsets", "cronjobs", "jobs", "ingresses", "secrets",
	"resourcequotas", "replicationcontrollers", "limitranges", "horizontalpodautoscalers",
	"poddisruptionbudgets", "storageclasses", "volumeattachments",
}

func startFakeKSM(t *testing.T, check *KSMCheck, objects ...runtime.Object) [][]cache.Store {
	t.Helper()
	// The fake clientset does not send the bookmark that ends a watch-list stream.
	t.Setenv("KUBE_FEATURE_WatchListClient", "false")

	client := fake.NewClientset(objects...)
	client.Discovery().(*fakediscovery.FakeDiscovery).Resources = []*metav1.APIResourceList{
		{GroupVersion: "v1", APIResources: []metav1.APIResource{
			{Name: "nodes", Kind: "Node"}, {Name: "pods", Kind: "Pod"}, {Name: "namespaces", Kind: "Namespace"},
			{Name: "services", Kind: "Service"}, {Name: "endpoints", Kind: "Endpoints"},
		}},
		{GroupVersion: "apps/v1", APIResources: []metav1.APIResource{
			{Name: "deployments", Kind: "Deployment"}, {Name: "replicasets", Kind: "ReplicaSet"},
		}},
		{GroupVersion: "networking.k8s.io/v1", APIResources: []metav1.APIResource{
			{Name: "ingresses", Kind: "Ingress"},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stores, err := check.BuildStores(ctx, client)
	require.NoError(t, err)
	return stores
}

// collectWhen collects until ready holds. The join pass reads the stores before
// the series pass, so a run can race the informers' initial list; collecting
// once more after ready gives every join source time to land.
func collectWhen(t *testing.T, check *KSMCheck, stores [][]cache.Store, newSender func() sender.Sender, ready func() bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		check.Collect(newSender(), stores, time.Now())
		return ready()
	}, 10*time.Second, 50*time.Millisecond)
	check.Collect(newSender(), stores, time.Now())
}

func TestAgentKSMParity(t *testing.T) {
	prefix := networkingv1.PathTypePrefix
	check := New(&KSMConfig{Collectors: chartCollectors}, Options{ClusterName: "prod"})
	stores := startFakeKSM(t, check,
		&corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "shop", UID: "ep-uid"},
			Subsets: []corev1.EndpointSubset{{
				Addresses:         []corev1.EndpointAddress{{IP: "10.0.0.1"}, {IP: "10.0.0.2"}},
				NotReadyAddresses: []corev1.EndpointAddress{{IP: "10.0.0.3"}},
				Ports:             []corev1.EndpointPort{{Port: 80}, {Port: 443}},
			}},
		},
		&networkingv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "shop", UID: "ing-uid"},
			Spec: networkingv1.IngressSpec{Rules: []networkingv1.IngressRule{{
				Host: "shop.example",
				IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
					Paths: []networkingv1.HTTPIngressPath{{
						Path: "/", PathType: &prefix,
						Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
							Name: "web", Port: networkingv1.ServiceBackendPort{Number: 80},
						}},
					}},
				}},
			}}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "db-0", Namespace: "shop", UID: "pod-uid"},
			Spec: corev1.PodSpec{Volumes: []corev1.Volume{
				{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-db-0"}}},
				{Name: "scratch", VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{}}},
			}},
		},
	)

	var s *mocksender.MockSender
	newSender := func() sender.Sender {
		s = mocksender.NewMockSender("ksm")
		s.SetupAcceptAll()
		return s
	}
	called := func(name string) bool {
		for _, c := range s.Calls {
			if c.Arguments.String(0) == name {
				return true
			}
		}
		return false
	}
	collectWhen(t, check, stores, newSender, func() bool {
		return called("kubernetes_state.endpoint.address_available") &&
			called("kubernetes_state.ingress.path") &&
			called("kubernetes_state.pod.volumes.persistentvolumeclaims_readonly")
	})

	endpointTags := []string{"kube_namespace:shop", "kube_endpoint:web", "kube_cluster_name:prod"}
	s.AssertMetric(t, "Gauge", "kubernetes_state.endpoint.address_available", 4, "", endpointTags)
	s.AssertMetric(t, "Gauge", "kubernetes_state.endpoint.address_not_ready", 2, "", endpointTags)

	s.AssertMetric(t, "Gauge", "kubernetes_state.ingress.path", 1, "",
		[]string{"kube_ingress_host:shop.example", "kube_ingress_path:/", "kube_service:web", "kube_service_port:80"})
	s.AssertMetricNotTaggedWith(t, "Gauge", "kubernetes_state.ingress.path", []string{"path_type:Prefix"})

	s.AssertMetric(t, "Gauge", "kubernetes_state.pod.volumes.persistentvolumeclaims_readonly", 0, "",
		[]string{"volume:data", "persistentvolumeclaim:data-db-0"})
	s.AssertNotCalled(t, "Gauge", "kubernetes_state.pod.volumes.persistentvolumeclaims_readonly",
		mock.Anything, mock.Anything, mocksender.MatchTagsContains([]string{"volume:scratch"}))
	s.AssertMetricNotTaggedWith(t, "Gauge", "kubernetes_state.pod.volumes.persistentvolumeclaims_readonly", []string{"ephemeral:false"})
}
