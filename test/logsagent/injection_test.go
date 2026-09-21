//nolint:goconst // Keep fault stages and expected lifecycle fields beside each assertion.
package logsagent_test

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type faultControl struct {
	t    *testing.T
	root string
}

func newFaultFixture(t *testing.T, mode string) (*fixture, faultControl) {
	t.Helper()
	f := newBinaryFixture(t, mode, "OTEL_AGENT_FAULT_BINARY")
	control := faultControl{t: t, root: t.TempDir()}
	f.childEnv = append(f.childEnv, "OTEL_LOGS_FAULT_DIR="+control.root, "GORACE=atexit_sleep_ms=0")
	return f, control
}

func (c faultControl) hold(name string) {
	c.t.Helper()
	if err := os.WriteFile(filepath.Join(c.root, name+".hold"), nil, 0600); err != nil {
		c.t.Fatal(err)
	}
}

func (c faultControl) release(name string) {
	c.t.Helper()
	if err := os.Remove(filepath.Join(c.root, name+".hold")); err != nil {
		c.t.Fatal(err)
	}
}

func (c faultControl) count(name string) int {
	c.t.Helper()
	files, err := filepath.Glob(filepath.Join(c.root, name+"-*"))
	if err != nil {
		c.t.Fatal(err)
	}
	return len(files)
}

func (c faultControl) wait(name string, count int) {
	c.t.Helper()
	eventually(c.t, 5*time.Second, name, func() bool { return c.count(name) == count })
}

func assertNoFinalDrain(t *testing.T, p *process) {
	t.Helper()
	select {
	case <-p.done:
		t.Fatalf("collector exited while its export call was held: %v", p.err)
	default:
	}
	if len(stressEvents(p, "Logs export drain finished", "")) != 0 {
		t.Fatal("collector finalized the drain before export completion")
	}
	if p.status("/live") != http.StatusOK || p.status("/ready") != http.StatusServiceUnavailable {
		t.Fatal("collector must remain live and unready while joining export calls")
	}
}

func TestInjectedAdmissionSaturation(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"legacy", "native"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f, control := newFaultFixture(t, mode)
			stage, concurrent := "completion", f.settings.Concurrency
			if os.Getenv("OTEL_COMPARISON_DESIGN") == "queued" {
				stage, concurrent = "worker", 4
			}
			f.backend.responseDelay = 100 * time.Millisecond
			control.hold(stage)
			control.hold("admission")
			port, endpoint := stressMetrics(t)
			p := f.start(stressValidated(t, func(config map[string]any) {
				section(config, "service", "telemetry")["metrics"] = map[string]any{
					"level": "detailed",
					"readers": []any{map[string]any{"pull": map[string]any{"exporter": map[string]any{
						"prometheus": map[string]any{"host": "127.0.0.1", "port": port},
					}}}},
				}
			}), true)
			p.ready()
			bodies := f.appendRecords(0, 0, 1)
			control.wait("admission_full", 1)
			control.wait(stage+"_entered", concurrent)
			control.wait("admission_rejected", 1)
			eventually(t, 3*time.Second, "exact saturation counters", func() bool {
				return stressMetric(p, endpoint, "stslogsroute_pre_export_rejected_requests", "admission_saturated") == 1 &&
					stressMetric(p, endpoint, "stslogsroute_pre_export_rejected_records", "admission_saturated") == 1
			})
			if len(stressEvents(p, "Logs export completed", "")) != 0 {
				t.Fatal("admission was not saturated with outstanding result waiters")
			}
			if f.backend.attempts(mode) != concurrent || f.backend.bodies(mode, false)[bodies[0]] != 0 {
				t.Fatal("excess calls reached the backend while completions were held")
			}
			f.backend.mu.Lock()
			maxConcurrent := f.backend.maxActive
			f.backend.mu.Unlock()
			if maxConcurrent != concurrent {
				t.Fatalf("backend concurrency=%d, want %d", maxConcurrent, concurrent)
			}
			t.Logf("Observed backend concurrency=%d for %d admitted calls", maxConcurrent, f.settings.Concurrency)
			p.signal()
			assertDraining(t, p)
			assertNoFinalDrain(t, p)
			control.release("admission")
			control.release(stage)
			p.wait(8*time.Second, true)
			control.wait("drain_finalized", 1)
			assertOrdinaryDrain(t, p, true)
			if len(stressEvents(p, "Logs export completed", "acknowledged")) != f.settings.Concurrency {
				t.Fatal("not all admitted calls completed after completion release")
			}
			admitted := make([]string, 0, f.settings.Concurrency)
			for i := range f.settings.Concurrency {
				admitted = append(admitted, fmt.Sprintf("%s injected-call=%d", bodies[0], i))
			}
			f.backend.assertRecords(mode, admitted, true)
			f.backend.assertOnly(mode)
			f.backend.assertIdentity()
		})
	}
}

func TestInjectedDelayedSynchronousCompletion(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"legacy", "native"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f, control := newFaultFixture(t, mode)
			control.hold("completion")
			p := f.start(stressValidated(t, nil), true)
			p.ready()
			bodies := f.appendRecords(0, 0, 1)
			control.wait("completion_entered", 1)
			f.backend.waitBodies(mode, bodies, true)
			p.signal()
			assertDraining(t, p)
			timer := time.NewTimer(f.settings.Lifetime + 100*time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-p.done:
				t.Fatal("collector exited with an admitted call held")
			}
			if len(stressEvents(p, "Logs export completed", "")) != 0 || control.count("connector_stopped") != 0 {
				t.Fatal("synchronous completion escaped the admission boundary")
			}
			assertNoFinalDrain(t, p)
			control.release("completion")
			p.wait(5*time.Second, true)
			control.wait("completion_released", 1)
			control.wait("completion_finished", 1)
			control.wait("drain_finalized", 1)
			stressFinal(t, p, 1, 1, 0)
			f.backend.assertRecords(mode, bodies, true)
			f.backend.assertOnly(mode)
			if f.backend.attempts(mode) != 1 {
				t.Fatal("held post-send completion caused an unexpected retry")
			}
		})
	}
}

func TestInjectedSignalFailureRecoversAfterCooldown(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"legacy", "native"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f, control := newFaultFixture(t, mode)
			control.hold("signal")
			scriptedFeatures(f, mode)
			const cooldown = 2 * time.Second
			p := f.start(stressValidated(t, func(config map[string]any) {
				scriptedConfig(config)
				section(config, "extensions", "stslogscapability/logs")["restart_cooldown"] = cooldown.String()
			}), true)
			p.ready()
			target := featureReply{mode: otherMode(mode), status: http.StatusOK}
			for range 3 {
				deliverFeature(t, f, p, target)
			}
			eventually(t, time.Second, "signal callback failure", func() bool {
				return p.event("Logs capability restart request failed", map[string]any{"stage": "signal"})
			})
			failedAt := time.Now()
			control.wait("signal_attempt", 1)
			p.ready()
			state := readControllerState(t, f)
			if state["pending_intent"] != true || state["old_mode"] != mode || state["new_mode"] != target.mode {
				t.Fatal("signal failure lost the persisted restart intent")
			}
			message, err := os.ReadFile(f.settings.Termination)
			if err != nil || string(message) != fmt.Sprintf(
				"Logs capability changed from %s to %s; requesting graceful restart.\n", mode, target.mode) {
				t.Fatal("signal callback ran without the expected termination message")
			}
			control.release("signal")
			for range 3 {
				deliverFeature(t, f, p, target)
				assertNoRestart(t, p)
			}
			bodies := f.appendRecords(0, 0, 1)
			f.backend.waitBodies(mode, bodies, true)
			if time.Since(failedAt) >= cooldown || control.count("signal_attempt") != 1 {
				t.Fatal("fixture failed to prove callback suppression inside cooldown")
			}
			timer := time.NewTimer(time.Until(failedAt.Add(cooldown)))
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-p.done:
				t.Fatal("collector exited during cooldown")
			}
			for range 2 {
				deliverFeature(t, f, p, target)
				assertNoRestart(t, p)
			}
			if control.count("signal_attempt") != 1 {
				t.Fatal("signal callback retried before three fresh observations")
			}
			deliverFeature(t, f, p, target)
			p.wait(5*time.Second, true)
			control.wait("signal_attempt", 2)
			assertOrdinaryDrain(t, p, true)
			if len(stressEvents(p, "Logs capability restart request failed", "")) != 1 ||
				len(stressEvents(p, "Logs capability restart requested", "")) != 1 {
				t.Fatal("signal failure did not recover with exactly one real restart")
			}
			f.backend.assertRecords(mode, bodies, true)
			f.backend.assertOnly(mode)
		})
	}
}
