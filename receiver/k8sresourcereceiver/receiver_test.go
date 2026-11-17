//nolint:testpackage
package k8sresourcereceiver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/k8sleaderelector"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/resourcecollection"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
)

type receiverTestHost struct {
	extensions map[component.ID]component.Component
}

func (h receiverTestHost) GetExtensions() map[component.ID]component.Component { return h.extensions }

type receiverTestElector struct {
	component.Component
	registered    chan struct{}
	alreadyLeader bool
}

func (e *receiverTestElector) SetCallBackFuncs(start k8sleaderelector.StartCallback, _ k8sleaderelector.StopCallback) {
	close(e.registered)
	if e.alreadyLeader {
		start(context.Background())
	}
}

func TestReceiverColdBootstrapDoesNotBlockStartupOrShutdown(t *testing.T) {
	id := component.MustNewID("k8s_leader_elector")
	elector := &receiverTestElector{registered: make(chan struct{})}
	cfg := testConfig([]string{"*"}, nil)
	cfg.K8sLeaderElector = &id
	cfg.PeerSyncDNS = "127.0.0.1"
	cfg.PeerSyncPort = 0
	r := &k8sresourceReceiver{
		settings: testSettings(t), config: cfg, consumer: consumertest.NewNop(),
		metrics: metrics.NoopRecorder{}, dynamicClient: dynamicfake.NewSimpleDynamicClient(testScheme()),
		discoveryClient: &collectionRuntimeDiscovery{},
	}
	// A short caller deadline must not become the background receiver lifetime.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	require.NoError(t, r.Start(ctx, receiverTestHost{extensions: map[component.ID]component.Component{id: elector}}))
	t.Cleanup(func() { require.NoError(t, r.Shutdown(context.Background())) })
	require.Less(t, time.Since(started), time.Second)
	<-ctx.Done()
	select {
	case <-r.bootstrapDone:
		t.Fatal("bootstrap stopped with the startup context")
	case <-elector.registered:
		t.Fatal("leadership registered before cache bootstrap completed")
	default:
	}
	started = time.Now()
	require.NoError(t, r.Shutdown(context.Background()))
	require.Less(t, time.Since(started), time.Second)
	select {
	case <-elector.registered:
		t.Fatal("shutdown registered leadership callbacks")
	default:
	}
	r.mu.Lock()
	collector := r.collector
	r.mu.Unlock()
	require.Nil(t, collector)
}

func TestReceiverRejectsMissingElectorBeforeStartingPeerServer(t *testing.T) {
	id := component.MustNewID("k8s_leader_elector")
	cfg := testConfig([]string{"*"}, nil)
	cfg.K8sLeaderElector = &id
	r := &k8sresourceReceiver{config: cfg}
	require.ErrorContains(t, r.Start(context.Background(), receiverTestHost{}), "not found")
	require.Nil(t, r.peerStore)
}

func TestReceiverIgnoresLeadershipAfterShutdown(t *testing.T) {
	r := &k8sresourceReceiver{settings: testSettings(t), shuttingDown: true}
	require.NoError(t, r.startCollector(context.Background(), context.Background()))
	require.Nil(t, r.collector)
}

func TestReceiverCollectsBaselineWithoutDeclarations(t *testing.T) {
	for _, available := range []bool{false, true} {
		name := "missing declaration API"
		if available {
			name = "empty declaration API"
		}
		t.Run(name, func(t *testing.T) {
			cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "pods"}}, nil)
			cfg.PeerSyncPort = 0
			client := collectionRuntimeClient(t, makeTestPod("baseline", "payments", nil))
			if !available {
				client.PrependReactor("list", resourcecollection.GVR().Resource,
					func(clienttesting.Action) (bool, runtime.Object, error) {
						return true, nil, apierrors.NewNotFound(resourcecollection.GVR().GroupResource(), "")
					})
			}
			r := &k8sresourceReceiver{
				settings: testSettings(t), config: cfg, consumer: consumertest.NewNop(),
				metrics: metrics.NoopRecorder{}, dynamicClient: client,
				discoveryClient: &collectionRuntimeDiscovery{resources: []*metav1.APIResourceList{
					collectionTestAPI("", "v1", true, "pods"),
				}},
			}
			require.NoError(t, r.Start(context.Background(), receiverTestHost{}))
			t.Cleanup(func() { require.NoError(t, r.Shutdown(context.Background())) })
			r.mu.Lock()
			collector := r.collector
			r.mu.Unlock()
			require.NotNil(t, collector)
			source, ok := collector.informers.(*collectionRuntime)
			require.True(t, ok)
			require.Eventually(t, func() bool {
				view := source.collectionSnapshot()
				return view.Complete && len(view.Objects[podsGVR].Objects) == 1 &&
					view.Objects[podsGVR].Objects[0].GetName() == "baseline"
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}

func TestReceiverShutdownCancelsAlreadyLeaderInformerStartup(t *testing.T) {
	listStarted, listCanceled := make(chan struct{}), make(chan struct{})
	var startedOnce, canceledOnce sync.Once
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		startedOnce.Do(func() { close(listStarted) })
		select {
		case <-req.Context().Done():
			canceledOnce.Do(func() { close(listCanceled) })
		case <-release:
			_, _ = w.Write([]byte(`{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinitionList","items":[]}`))
		}
	}))
	defer server.Close()
	defer close(release)
	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	cfg := testConfig([]string{"*"}, nil)
	id := component.MustNewID("k8s_leader_elector")
	cfg.K8sLeaderElector = &id
	elector := &receiverTestElector{registered: make(chan struct{}), alreadyLeader: true}
	r := &k8sresourceReceiver{
		settings: testSettings(t), config: cfg, consumer: consumertest.NewNop(),
		metrics: metrics.NoopRecorder{}, dynamicClient: client,
		discoveryClient: &collectionRuntimeDiscovery{},
	}
	startDone := make(chan error, 1)
	go func() {
		startDone <- r.Start(context.Background(), receiverTestHost{
			extensions: map[component.ID]component.Component{id: elector},
		})
	}()
	select {
	case <-listStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("initial CRD list did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, r.Shutdown(ctx))
	select {
	case <-listCanceled:
	case <-ctx.Done():
		t.Fatal("initial CRD list was not cancelled")
	}
	require.NoError(t, <-startDone)
	require.Nil(t, r.collector)
}

func TestReceiverShutdownDeadlineDoesNotAbandonCleanup(t *testing.T) {
	r := &k8sresourceReceiver{settings: testSettings(t), bootstrapDone: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, r.Shutdown(ctx), context.DeadlineExceeded)
	close(r.bootstrapDone)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
	defer cleanupCancel()
	require.NoError(t, r.Shutdown(cleanupCtx))
}
