package stsk8stopologyexporter //nolint:testpackage // Exercises the internal reset and handover paths.

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/transactional"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestDeliveryCannotStartFromStateBeingReset(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exp := newTopologyExporter(testConfig(), testLogger(t), testSender(server.URL))
	for _, logs := range snapshotRecords(t, "1", loadFixture(t)) {
		require.NoError(t, exp.consumeLogs(context.Background(), logs))
	}
	entered, release := make(chan struct{}), make(chan struct{})
	exp.testHookDuringReset = func() {
		close(entered)
		<-release
	}
	go exp.resetCollection()
	<-entered

	delivered := make(chan bool, 1)
	go func() {
		sent, _ := exp.sendSnapshot(context.Background())
		delivered <- sent
	}()
	select {
	case <-delivered:
		t.Fatal("a delivery started while the reset was in progress")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	assert.False(t, <-delivered, "the delivery must see the reset state")
	assert.Zero(t, requests.Load())
}

func TestHandoverDelayPostponesTheFirstSnapshot(t *testing.T) {
	capture := &intakeCapture{}
	server := httptest.NewServer(capture.handler(t))
	defer server.Close()

	cfg := testConfig()
	cfg.Endpoint = server.URL + "/stsAgent/intake"
	cfg.Interval = time.Hour
	cfg.SnapshotMaxAge = 2 * time.Hour
	cfg.HandoverDelay = 300 * time.Millisecond
	exp, err := NewFactory().CreateLogs(context.Background(), exportertest.NewNopSettings(NewFactory().Type()), cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, exp.Shutdown(context.Background())) }()

	for _, logs := range snapshotRecords(t, "1", loadFixture(t)) {
		require.NoError(t, exp.ConsumeLogs(context.Background(), logs))
	}
	time.Sleep(150 * time.Millisecond)
	payloads, _ := capture.snapshot()
	assert.Empty(t, payloads, "nothing is sent within the handover delay")
	require.Eventually(t, func() bool {
		payloads, _ := capture.snapshot()
		return len(payloads) > 0
	}, 5*time.Second, 10*time.Millisecond, "the snapshot is sent once the delay has passed")
}

// ownershipModel follows the topology sync's producer rule
// (ExtTopoSyncTaskState.activeOrMakeActiveHost): a snapshot start is accepted
// from the active host, an unseen host, or a host that is the only recent one;
// stops from other hosts are ignored.
type ownershipModel struct {
	mu     sync.Mutex
	active string
	recent []string
}

func (m *ownershipModel) apply(payload transactional.IntakePayload) {
	m.mu.Lock()
	defer m.mu.Unlock()
	host := payload.InternalHostname
	for _, topology := range payload.Topologies {
		if topology.StartSnapshot {
			sole := len(m.recent) > 0
			for _, h := range m.recent {
				sole = sole && h == host
			}
			if m.active == "" || m.active == host || !slices.Contains(m.recent, host) || sole {
				m.active = host
			}
			m.recent = append([]string{host}, m.recent...)
			if len(m.recent) > 10 {
				m.recent = m.recent[:10]
			}
		}
	}
}

func (m *ownershipModel) owner() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active
}

// A former leader's first request can reach the sync after its successor has
// started. Its hostname is then unseen, so it would take ownership back.
func TestHandoverDelayKeepsAFormerLeadersLateRequestFromTakingOver(t *testing.T) {
	const processingLag = 200 * time.Millisecond
	for _, tc := range []struct {
		name          string
		handoverDelay time.Duration
		owner         string
	}{
		{"without a handover delay the former leader takes over", 0, "producer-a"},
		{"a handover delay longer than the lag keeps the successor", 3 * processingLag, "producer-b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model := &ownershipModel{}
			var applied sync.WaitGroup
			var delayedA atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reader, err := gzip.NewReader(r.Body)
				require.NoError(t, err)
				body, err := io.ReadAll(reader)
				require.NoError(t, err)
				var payload transactional.IntakePayload
				require.NoError(t, json.Unmarshal(body, &payload))
				if payload.InternalHostname == "producer-a" && delayedA.CompareAndSwap(false, true) {
					// Accepted, but processed late even though the client gives up.
					applied.Add(1)
					go func() {
						defer applied.Done()
						time.Sleep(processingLag)
						model.apply(payload)
					}()
					return
				}
				model.apply(payload)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			start := func(host string, delay time.Duration) (*topologyExporter, func()) {
				cfg := testConfig()
				cfg.Endpoint = server.URL + "/stsAgent/intake"
				cfg.InternalHostname = host
				cfg.Interval = time.Hour
				cfg.SnapshotMaxAge = 2 * time.Hour
				cfg.HandoverDelay = delay
				exp := newTopologyExporter(cfg, testLogger(t), testSender(server.URL))
				require.NoError(t, exp.start(context.Background(), componenttest.NewNopHost()))
				return exp, func() { require.NoError(t, exp.shutdown(context.Background())) }
			}

			a, stopA := start("producer-a", 0)
			defer stopA()
			for _, logs := range snapshotRecords(t, "a", loadFixture(t)) {
				require.NoError(t, a.consumeLogs(context.Background(), logs))
			}
			require.Eventually(t, delayedA.Load, 5*time.Second, 5*time.Millisecond)
			require.NoError(t, a.consumeLogs(context.Background(), boundaryRecord("reset", "", false)))

			b, stopB := start("producer-b", tc.handoverDelay)
			defer stopB()
			for _, logs := range snapshotRecords(t, "b", loadFixture(t)) {
				require.NoError(t, b.consumeLogs(context.Background(), logs))
			}
			require.Eventually(t, func() bool { return model.owner() != "" }, 5*time.Second, 5*time.Millisecond)
			applied.Wait()
			if tc.handoverDelay > 0 {
				require.Eventually(t, func() bool { return model.owner() == "producer-b" }, 5*time.Second, 10*time.Millisecond)
			}
			assert.Equal(t, tc.owner, model.owner())
		})
	}
}
