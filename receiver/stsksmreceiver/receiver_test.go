package stsksmreceiver //nolint:testpackage // Injects the Kubernetes client and leader elector.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/k8sleaderelector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

const eventually = 10 * time.Second

func fakeClient(objects ...runtime.Object) kubernetes.Interface {
	client := fake.NewClientset(objects...)
	discovery, _ := client.Discovery().(*fakediscovery.FakeDiscovery)
	discovery.Resources = []*metav1.APIResourceList{
		{GroupVersion: "v1", APIResources: []metav1.APIResource{
			{Name: "nodes", Kind: "Node"}, {Name: "pods", Kind: "Pod"}, {Name: "namespaces", Kind: "Namespace"},
		}},
	}
	return client
}

func clusterObjects() []runtime.Object {
	return []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: "cluster-uid"}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-1", UID: "node-uid"},
			Status: corev1.NodeStatus{
				Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
			},
		},
	}
}

func newTestReceiver(
	t *testing.T, client kubernetes.Interface, sink *consumertest.MetricsSink, mutate func(*Config),
) (*ksmReceiver, *observer.ObservedLogs) {
	t.Helper()
	// The fake clientset does not send the bookmark that ends a watch-list stream.
	t.Setenv("KUBE_FEATURE_WatchListClient", "false")

	cfg := validConfig()
	cfg.ClusterName = "Prod_EU"
	cfg.CollectionInterval = 20 * time.Millisecond
	if mutate != nil {
		mutate(cfg)
	}
	core, logs := observer.New(zap.InfoLevel)
	set := receivertest.NewNopSettings(component.MustNewType(typeStr))
	set.Logger = zap.New(core)
	rcv, err := newReceiver(set, cfg, sink)
	require.NoError(t, err)
	rcv.newClient = func() (kubernetes.Interface, error) { return client, nil }
	rcv.clusterIDTimeout = 50 * time.Millisecond
	return rcv, logs
}

func waitForMetric(t *testing.T, sink *consumertest.MetricsSink, name string) pmetric.Metrics {
	t.Helper()
	var found pmetric.Metrics
	require.Eventually(t, func() bool {
		for _, md := range sink.AllMetrics() {
			if dataPoint(md, name) != nil {
				found = md
				return true
			}
		}
		return false
	}, eventually, 10*time.Millisecond)
	return found
}

func dataPoint(md pmetric.Metrics, name string) *pmetric.NumberDataPoint {
	ms := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	for i := 0; i < ms.Len(); i++ {
		if ms.At(i).Name() == name && ms.At(i).Gauge().DataPoints().Len() > 0 {
			dp := ms.At(i).Gauge().DataPoints().At(0)
			return &dp
		}
	}
	return nil
}

func TestCollectionPayloadShape(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	rcv, _ := newTestReceiver(t, fakeClient(clusterObjects()...), sink, nil)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, rcv.Shutdown(context.Background())) })

	md := waitForMetric(t, sink, "kubernetes_state.node.cpu_allocatable")
	require.Equal(t, 1, md.ResourceMetrics().Len())
	rm := md.ResourceMetrics().At(0)
	assert.Equal(t, map[string]any{"service.namespace": "", "k8s.cluster.name": "prod-eu"}, rm.Resource().Attributes().AsRaw())
	require.Equal(t, 1, rm.ScopeMetrics().Len())
	scope := rm.ScopeMetrics().At(0).Scope()
	assert.Empty(t, scope.Name())
	assert.Empty(t, scope.Version())
	assert.Equal(t, 0, scope.Attributes().Len())

	attrs := dataPoint(md, "kubernetes_state.node.cpu_allocatable").Attributes().AsRaw()
	assert.Equal(t, "node-1-prod-eu", attrs["sts_host"])
	assert.Equal(t, "prod-eu", attrs["kube_cluster_name"])
	assert.Equal(t, "cluster-uid", attrs["orch_cluster_id"])
	assert.Equal(t, "0", attrs["interval"])
}

func TestClusterIDFallback(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	rcv, logs := newTestReceiver(t, fakeClient(clusterObjects()[1:]...), sink, nil)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, rcv.Shutdown(context.Background())) })

	assert.Equal(t, 1, logs.FilterMessageSnippet("Cannot read the cluster ID").Len())
	md := waitForMetric(t, sink, "kubernetes_state.node.count")
	attrs := dataPoint(md, "kubernetes_state.node.count").Attributes().AsRaw()
	assert.NotContains(t, attrs, "orch_cluster_id")
	assert.Equal(t, "prod-eu", attrs["kube_cluster_name"])
}

type fakeElector struct {
	component.StartFunc
	component.ShutdownFunc

	mu    sync.Mutex
	start k8sleaderelector.StartCallback
	stop  k8sleaderelector.StopCallback
}

func (e *fakeElector) SetCallBackFuncs(start k8sleaderelector.StartCallback, stop k8sleaderelector.StopCallback) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.start, e.stop = start, stop
}

func (e *fakeElector) startLeading() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.start(context.Background())
}

func (e *fakeElector) stopLeading() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stop()
}

type hostWithExtensions struct {
	component.Host
	extensions map[component.ID]component.Component
}

func (h hostWithExtensions) GetExtensions() map[component.ID]component.Component {
	return h.extensions
}

func (r *ksmReceiver) collecting() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cancel != nil
}

func TestLeaderElectionStartsAndStopsCollection(t *testing.T) {
	electorID := component.MustNewID("k8s_leader_elector")
	elector := &fakeElector{}
	host := hostWithExtensions{
		Host:       componenttest.NewNopHost(),
		extensions: map[component.ID]component.Component{electorID: elector},
	}
	sink := new(consumertest.MetricsSink)
	rcv, _ := newTestReceiver(t, fakeClient(clusterObjects()...), sink, func(c *Config) { c.K8sLeaderElector = &electorID })
	require.NoError(t, rcv.Start(context.Background(), host))

	time.Sleep(100 * time.Millisecond)
	assert.False(t, rcv.collecting())
	assert.Equal(t, 0, sink.DataPointCount(), "followers do not collect")

	elector.startLeading()
	waitForMetric(t, sink, "kubernetes_state.node.cpu_allocatable")

	elector.stopLeading()
	assert.False(t, rcv.collecting())
	stopped := len(sink.AllMetrics())
	time.Sleep(100 * time.Millisecond)
	assert.Len(t, sink.AllMetrics(), stopped, "no collection after leadership is lost")

	sink.Reset()
	elector.startLeading()
	waitForMetric(t, sink, "kubernetes_state.node.cpu_allocatable")

	require.NoError(t, rcv.Shutdown(context.Background()))
	assert.False(t, rcv.collecting())
	elector.startLeading()
	assert.False(t, rcv.collecting(), "no collection after shutdown")
}

func TestStartRejectsUnknownElector(t *testing.T) {
	electorID := component.MustNewID("k8s_leader_elector")
	rcv, _ := newTestReceiver(t, fakeClient(), new(consumertest.MetricsSink), func(c *Config) { c.K8sLeaderElector = &electorID })
	assert.ErrorContains(t, rcv.Start(context.Background(), componenttest.NewNopHost()), "not found")
}
