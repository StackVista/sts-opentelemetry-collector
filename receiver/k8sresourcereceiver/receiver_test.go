//nolint:testpackage
package k8sresourcereceiver

import (
	"context"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/k8sleaderelector"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

type receiverTestHost struct {
	extensions map[component.ID]component.Component
}

func (h receiverTestHost) GetExtensions() map[component.ID]component.Component { return h.extensions }

type receiverTestElector struct {
	component.Component
	registered chan struct{}
}

func (e *receiverTestElector) SetCallBackFuncs(_ k8sleaderelector.StartCallback, _ k8sleaderelector.StopCallback) {
	close(e.registered)
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
	require.NoError(t, r.startCollector(context.Background()))
	require.Nil(t, r.collector)
}
