package stsk8stopologyexporter

import (
	"context"
	"sync"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/openapiclient/features"
	"go.uber.org/zap"
)

// capabilityOtelClusterTopology is true once the platform maps OTel cluster
// topology, so legacy Kubernetes topology is no longer needed. Absent means it
// is still needed.
const capabilityOtelClusterTopology = "otel-cluster-topology"

type discoveryOptions struct {
	query              features.QueryOptions
	poll               features.PollOptions
	stableObservations int
}

func defaultDiscoveryOptions() discoveryOptions {
	return discoveryOptions{
		query: features.QueryOptions{
			Timeout: 20 * time.Second, AttemptTimeout: 5 * time.Second, MaxAttempts: 3,
			InitialBackoff: 500 * time.Millisecond, MaxBackoff: 2 * time.Second,
			BooleanCapabilities: []string{capabilityOtelClusterTopology},
		},
		poll:               features.PollOptions{Interval: time.Minute, Jitter: 0.2},
		stableObservations: 3,
	}
}

// modeSelector decides whether legacy topology is sent. Uncertainty keeps the
// current mode, which starts as sending: a duplicate legacy stream merges with
// native topology, while a missing one removes topology.
type modeSelector struct {
	mu      sync.Mutex
	legacy  bool
	decided bool
	streak  int
	stable  int
}

func newModeSelector(stableObservations int) *modeSelector {
	return &modeSelector{legacy: true, stable: stableObservations}
}

func (m *modeSelector) legacyEnabled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.legacy
}

// observe applies one feature query result and returns (legacy, changed). The
// first valid answer decides the mode; later changes need consecutive matching
// answers.
func (m *modeSelector) observe(result features.Result) (bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if result.Class != features.Valid {
		m.streak = 0
		return m.legacy, false
	}
	want := legacyRequired(result.Features)
	if !m.decided {
		m.decided = true
		changed := want != m.legacy
		m.legacy = want
		return m.legacy, changed
	}
	if want == m.legacy {
		m.streak = 0
		return m.legacy, false
	}
	m.streak++
	if m.streak < m.stable {
		return m.legacy, false
	}
	m.streak = 0
	m.legacy = want
	return m.legacy, true
}

func legacyRequired(advertised map[string]any) bool {
	otel, ok := advertised[capabilityOtelClusterTopology].(bool)
	return !ok || !otel
}

// discover applies the initial query, closes initialized, then follows the
// poller until ctx ends.
func (e *topologyExporter) discover(ctx context.Context, initialized chan<- struct{}) {
	defer e.wg.Done()
	authCtx, cancel := context.WithCancel(context.WithoutCancel(e.authCtx))
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()

	e.applyObservation(e.features.FetchFeatures(authCtx))
	close(initialized)

	poller, err := e.features.StartPolling(authCtx, e.discovery.poll)
	if err != nil {
		e.logger.Error("Cannot poll platform features; legacy topology mode is fixed", zap.Error(err))
		return
	}
	defer poller.Stop()
	for {
		select {
		case <-authCtx.Done():
			return
		case result, ok := <-poller.Results():
			if !ok {
				return
			}
			e.applyObservation(result)
		}
	}
}

func (e *topologyExporter) applyObservation(result features.Result) {
	if result.Class == features.Authentication || result.Class == features.Configuration ||
		result.Class == features.Rejected {
		e.logger.Warn("Platform feature query failed; keeping the current legacy topology mode",
			zap.String("class", string(result.Class)), zap.Int("status", result.StatusCode))
	}
	// Under the delivery lock, which a delivery holds while it checks the mode:
	// a confirmed disable either precedes the delivery or cancels it.
	e.delivery.Lock()
	legacy, changed := e.mode.observe(result)
	if changed && !legacy && e.cancelDelivery != nil {
		e.cancelDelivery()
	}
	e.delivery.Unlock()
	if !changed {
		return
	}
	if legacy {
		e.logger.Info("Platform requires legacy Kubernetes topology; resuming export")
		select {
		case e.ready <- struct{}{}:
		default:
		}
		return
	}
	e.logger.Info("Platform no longer requires legacy Kubernetes topology; stopping export")
}
