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

func newFaultFixture(t *testing.T) (*fixture, faultControl) {
	t.Helper()
	f := newBinaryFixture(t, "OTEL_AGENT_FAULT_BINARY")
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
	f, control := newFaultFixture(t)
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
		return stressMetric(p, endpoint, "stslogsagent_pre_export_rejected_requests", "admission_saturated") == 1 &&
			stressMetric(p, endpoint, "stslogsagent_pre_export_rejected_records", "admission_saturated") == 1
	})
	if len(stressEvents(p, "Logs export completed", "")) != 0 {
		t.Fatal("admission was not saturated with outstanding result waiters")
	}
	if f.backend.attempts() != concurrent || f.backend.bodies(false)[bodies[0]] != 0 {
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
	f.backend.assertRecords(admitted, true)
	f.backend.assertOnly()
	f.backend.assertIdentity()
}

func TestInjectedDelayedSynchronousCompletion(t *testing.T) {
	t.Parallel()
	f, control := newFaultFixture(t)
	control.hold("completion")
	p := f.start(stressValidated(t, nil), true)
	p.ready()
	bodies := f.appendRecords(0, 0, 1)
	control.wait("completion_entered", 1)
	f.backend.waitBodies(bodies, true)
	p.signal()
	assertDraining(t, p)
	timer := time.NewTimer(f.settings.Lifetime + 100*time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-p.done:
		t.Fatal("collector exited with an admitted call held")
	}
	if len(stressEvents(p, "Logs export completed", "")) != 0 || control.count("delivery_stopped") != 0 {
		t.Fatal("synchronous completion escaped the admission boundary")
	}
	assertNoFinalDrain(t, p)
	control.release("completion")
	p.wait(5*time.Second, true)
	control.wait("completion_released", 1)
	control.wait("completion_finished", 1)
	control.wait("drain_finalized", 1)
	stressFinal(t, p, 1, 1, 0)
	f.backend.assertRecords(bodies, true)
	f.backend.assertOnly()
	if f.backend.attempts() != 1 {
		t.Fatal("held post-send completion caused an unexpected retry")
	}
}
