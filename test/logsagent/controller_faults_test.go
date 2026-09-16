//nolint:goconst // Keep feature outcomes and lifecycle assertions visible together.
package logsagent_test

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func scriptedFeatures(f *fixture, initialMode string) {
	f.backend.mu.Lock()
	defer f.backend.mu.Unlock()
	f.backend.featureReplies = make(chan featureReply, 1)
	f.backend.featureReplies <- featureReply{mode: initialMode, status: http.StatusOK}
}

func scriptedConfig(config map[string]any) {
	capability := section(config, "extensions", "stslogscapability/logs")
	capability["max_attempts"] = 1
	capability["query_timeout"] = "5s"
	capability["attempt_timeout"] = "5s"
}

func completedQueries(p *process) int {
	return len(stressEvents(p, "Logs capability query completed", "")) +
		len(stressEvents(p, "Logs capability query rejected", ""))
}

func deliverFeature(t *testing.T, f *fixture, p *process, reply featureReply) {
	t.Helper()
	before := completedQueries(p)
	select {
	case f.backend.featureReplies <- reply:
	case <-p.done:
		t.Fatal("agent exited before the next feature observation")
	case <-time.After(2 * time.Second):
		t.Fatal("agent stopped requesting feature observations")
	}
	eventually(t, time.Second, "completed feature query", func() bool {
		return completedQueries(p) == before+1
	})
}

func assertNoRestart(t *testing.T, p *process) {
	t.Helper()
	select {
	case <-p.done:
		t.Fatal("agent restarted before a fresh stable capability sequence")
	default:
	}
	if len(stressEvents(p, "Logs capability restart requested", "")) != 0 {
		t.Fatal("agent requested a premature capability restart")
	}
}

func TestCapabilityObservationReset(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"legacy", "native"} {
		for _, reset := range []struct {
			name    string
			reply   featureReply
			outcome string
		}{
			{"current", featureReply{mode: mode, status: 200}, "valid"},
			{"malformed", featureReply{status: 200, body: `{"otel-logs":"yes"}`}, "malformed"},
			{"authentication", featureReply{status: 401}, "authentication"},
			{"transient", featureReply{status: 503}, "transient"},
			{"unsupported", featureReply{status: 404}, "unsupported"},
		} {
			t.Run(mode+"/"+reset.name, func(t *testing.T) {
				t.Parallel()
				f := newFixture(t, mode)
				scriptedFeatures(f, mode)
				p := f.start(stressValidated(t, scriptedConfig), true)
				p.ready()
				target := featureReply{mode: otherMode(mode), status: http.StatusOK}
				deliverFeature(t, f, p, target)
				deliverFeature(t, f, p, target)
				deliverFeature(t, f, p, reset.reply)
				if !p.event("Logs capability query completed", map[string]any{"outcome": reset.outcome}) &&
					!p.event("Logs capability query rejected", map[string]any{"outcome": reset.outcome}) {
					t.Fatalf("missing completed reset query outcome %q", reset.outcome)
				}
				assertNoRestart(t, p)
				if reset.name == "authentication" {
					eventually(t, time.Second, "authentication readiness failure", func() bool {
						return p.status("/ready") == http.StatusServiceUnavailable
					})
				}
				deliverFeature(t, f, p, target)
				p.ready()
				deliverFeature(t, f, p, target)
				assertNoRestart(t, p)
				bodies := f.appendRecords(0, 0, 1)
				f.backend.waitBodies(mode, bodies, true)
				deliverFeature(t, f, p, target)
				p.wait(5*time.Second, true)
				assertOrdinaryDrain(t, p, true)
				f.backend.assertOnly(mode)
				f.backend.assertRecords(mode, bodies, true)
				if len(stressEvents(p, "Logs capability restart requested", "")) != 1 {
					t.Fatal("fresh stable sequence did not request exactly one restart")
				}
				state := readControllerState(t, f)
				if state["pending_intent"] != true || state["new_mode"] != target.mode {
					t.Fatal("fresh stable sequence did not persist the requested mode")
				}
			})
		}
	}
}

func TestRestartWriteFailureRecoversAfterCooldown(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"legacy", "native"} {
		for _, stage := range []string{"marker", "message"} {
			t.Run(mode+"/"+stage, func(t *testing.T) {
				t.Parallel()
				f := newFixture(t, mode)
				scriptedFeatures(f, mode)
				const cooldown = 2 * time.Second
				p := f.start(stressValidated(t, func(config map[string]any) {
					scriptedConfig(config)
					section(config, "extensions", "stslogscapability/logs")["restart_cooldown"] = cooldown.String()
				}), true)
				p.ready()
				path := f.settings.Termination
				if stage == "marker" {
					path = filepath.Join(f.settings.Controller, "state.json")
				}
				if err := os.MkdirAll(path, 0700); err != nil {
					t.Fatal(err)
				}
				target := featureReply{mode: otherMode(mode), status: http.StatusOK}
				for range 3 {
					deliverFeature(t, f, p, target)
				}
				eventually(t, time.Second, "restart write failure", func() bool {
					return p.event("Logs capability restart request failed", map[string]any{"stage": stage})
				})
				failedAt := time.Now()
				p.ready()
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				for range 3 {
					deliverFeature(t, f, p, target)
					assertNoRestart(t, p)
				}
				bodies := f.appendRecords(0, 0, 1)
				f.backend.waitBodies(mode, bodies, true)
				if time.Since(failedAt) >= cooldown {
					t.Fatal("fixture did not exercise observations inside the restart cooldown")
				}
				timer := time.NewTimer(time.Until(failedAt.Add(cooldown)))
				select {
				case <-timer.C:
				case <-p.done:
					timer.Stop()
					t.Fatal("agent exited while waiting for cooldown expiry")
				}
				for range 2 {
					deliverFeature(t, f, p, target)
					assertNoRestart(t, p)
				}
				deliverFeature(t, f, p, target)
				p.wait(5*time.Second, true)
				assertOrdinaryDrain(t, p, true)
				if len(stressEvents(p, "Logs capability restart request failed", "")) != 1 ||
					len(stressEvents(p, "Logs capability restart requested", "")) != 1 {
					t.Fatal("restart write failure did not recover exactly once after cooldown")
				}
				state := readControllerState(t, f)
				if state["pending_intent"] != true || state["old_mode"] != mode || state["new_mode"] != target.mode {
					t.Fatal("recovered restart did not persist the capability transition")
				}
				f.backend.assertOnly(mode)
				f.backend.assertRecords(mode, bodies, true)
			})
		}
	}
}
