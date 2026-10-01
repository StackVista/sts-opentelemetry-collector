package stsk8stopologyexporter //nolint:testpackage // Exercises the internal mode selector and discovery wiring.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/openapiclient/features"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func valid(advertised map[string]any) features.Result {
	return features.Result{Class: features.Valid, Features: advertised}
}

func TestModeSelector(t *testing.T) {
	off := map[string]any{capabilityLegacyKubernetesTopology: false}
	on := map[string]any{capabilityLegacyKubernetesTopology: true}

	m := newModeSelector(3)
	assert.True(t, m.legacyEnabled(), "legacy topology is sent until the platform says otherwise")

	legacy, changed := m.observe(features.Result{Class: features.Transient})
	assert.True(t, legacy)
	assert.False(t, changed)

	legacy, changed = m.observe(valid(off))
	assert.False(t, legacy, "the first valid answer decides the mode")
	assert.True(t, changed)

	for i := 0; i < 2; i++ {
		legacy, changed = m.observe(valid(on))
		assert.False(t, legacy, "a change needs consecutive answers")
		assert.False(t, changed)
	}
	m.observe(features.Result{Class: features.Timeout})
	for i := 0; i < 2; i++ {
		legacy, _ = m.observe(valid(on))
		assert.False(t, legacy, "a failed query resets the streak")
	}
	legacy, changed = m.observe(valid(on))
	assert.True(t, legacy)
	assert.True(t, changed)

	legacy, _ = m.observe(valid(map[string]any{}))
	assert.True(t, legacy, "an absent capability means legacy topology is still required")
}

type platform struct {
	mu        sync.Mutex
	features  map[string]any
	status    int
	snapshots atomic.Int32
}

func (p *platform) set(status int, advertised map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status, p.features = status, advertised
}

func (p *platform) handler(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/stsAgent/features"):
		p.mu.Lock()
		status, advertised := p.status, p.features
		p.mu.Unlock()
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		body, err := json.Marshal(advertised)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	case strings.HasSuffix(r.URL.Path, "/stsAgent/intake"):
		p.snapshots.Add(1)
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func startWithDiscovery(t *testing.T, p *platform, interval time.Duration) func() {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(p.handler))
	cfg := testConfig()
	cfg.Endpoint = server.URL + "/stsAgent/intake"
	cfg.DiscoveryEnabled = true
	cfg.Interval = interval
	cfg.SnapshotMaxAge = 2 * time.Hour
	opts := defaultDiscoveryOptions()
	opts.query.Timeout, opts.query.AttemptTimeout, opts.query.MaxAttempts = time.Second, time.Second, 1
	opts.poll = features.PollOptions{Interval: 20 * time.Millisecond}

	exp, err := buildLogsExporter(context.Background(), exportertest.NewNopSettings(NewFactory().Type()), cfg, opts)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))
	for _, logs := range snapshotRecords(t, "1", loadFixture(t)) {
		require.NoError(t, exp.ConsumeLogs(context.Background(), logs))
	}
	return func() {
		require.NoError(t, exp.Shutdown(context.Background()))
		server.Close()
	}
}

func TestOlderPlatformWithoutFeaturesKeepsLegacyTopology(t *testing.T) {
	p := &platform{}
	p.set(http.StatusNotFound, nil)
	stop := startWithDiscovery(t, p, time.Hour)
	defer stop()
	require.Eventually(t, func() bool { return p.snapshots.Load() > 0 }, 10*time.Second, 10*time.Millisecond)
}

func TestPlatformThatNoLongerNeedsLegacyTopologyReceivesNone(t *testing.T) {
	p := &platform{}
	p.set(http.StatusOK, map[string]any{capabilityLegacyKubernetesTopology: false})
	stop := startWithDiscovery(t, p, 20*time.Millisecond)
	defer stop()
	time.Sleep(300 * time.Millisecond)
	assert.Zero(t, p.snapshots.Load())
}

func TestLegacyTopologyFollowsThePlatformSwitch(t *testing.T) {
	p := &platform{}
	p.set(http.StatusOK, map[string]any{capabilityLegacyKubernetesTopology: true})
	stop := startWithDiscovery(t, p, 20*time.Millisecond)
	defer stop()
	require.Eventually(t, func() bool { return p.snapshots.Load() > 2 }, 10*time.Second, 10*time.Millisecond)

	p.set(http.StatusOK, map[string]any{capabilityLegacyKubernetesTopology: false})
	time.Sleep(300 * time.Millisecond)
	sent := p.snapshots.Load()
	p.set(http.StatusServiceUnavailable, nil)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, sent, p.snapshots.Load(), "no legacy topology after the switch, including during a features outage")

	p.set(http.StatusOK, map[string]any{capabilityLegacyKubernetesTopology: true})
	require.Eventually(t, func() bool { return p.snapshots.Load() > sent }, 10*time.Second, 10*time.Millisecond)
}

func TestResumingSendsWithoutWaitingForTheInterval(t *testing.T) {
	p := &platform{}
	p.set(http.StatusOK, map[string]any{capabilityLegacyKubernetesTopology: false})
	stop := startWithDiscovery(t, p, time.Hour)
	defer stop()
	time.Sleep(200 * time.Millisecond)
	require.Zero(t, p.snapshots.Load())

	p.set(http.StatusOK, map[string]any{capabilityLegacyKubernetesTopology: true})
	require.Eventually(t, func() bool { return p.snapshots.Load() > 0 }, 10*time.Second, 10*time.Millisecond)
}
