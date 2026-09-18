//nolint:goconst // Keep scenario names and expected outcomes together in lifecycle cases.
package logsagent_test

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRoutes(t *testing.T) {
	for _, mode := range []string{"legacy", "native"} {
		for _, scenario := range []struct {
			name       string
			plan       responsePlan
			accepted   bool
			retried    bool
			completion []string
		}{
			{"success", responsePlan{status: 200}, true, false, []string{"acknowledged"}},
			{"recovery", responsePlan{status: 200, failures: 2}, true, true, []string{"acknowledged"}},
			{"outage", responsePlan{status: 503}, false, true, []string{"retry_exhausted", "terminal_export_error"}},
			{"unauthorized", responsePlan{status: 401}, false, false, []string{"permanent_rejection"}},
			{"forbidden", responsePlan{status: 403}, false, false, []string{"permanent_rejection"}},
			{"not_found", responsePlan{status: 404}, false, false, []string{"permanent_rejection"}},
			{"too_large", responsePlan{status: 413}, false, false, []string{"permanent_rejection"}},
		} {
			t.Run(mode+"/"+scenario.name, func(t *testing.T) {
				f := newFixture(t, mode)
				f.backend.setPlan(mode, scenario.plan)
				p := f.start(nil, true)
				p.ready()
				bodies := f.appendRecords(0, 0, 1)
				f.backend.waitBodies(mode, bodies, scenario.accepted)
				p.waitExport(scenario.completion...)
				p.signal()
				p.wait(5*time.Second, true)
				drain := "failed"
				if scenario.accepted {
					drain = "completed"
					f.backend.assertRecords(mode, bodies, true)
				}
				p.assertDrain(drain)
				attempts := f.backend.attempts(mode)
				if (attempts > 1) != scenario.retried {
					t.Fatalf("attempts=%d, want retried=%v", attempts, scenario.retried)
				}
				f.backend.assertOnly(mode)
				f.backend.assertIdentity()
			})
		}
	}
}

func (p *process) waitExport(outcomes ...string) {
	p.t.Helper()
	eventually(p.t, 10*time.Second, "terminal export result", func() bool {
		for _, outcome := range outcomes {
			if p.event("Logs export completed", map[string]any{"outcome": outcome}) {
				return true
			}
		}
		return false
	})
}

func TestNativePartialSuccess(t *testing.T) {
	f := newFixture(t, "native")
	f.backend.setPlan("native", responsePlan{status: 200, partial: true})
	p := f.start(nil, true)
	p.ready()
	bodies := f.appendRecords(0, 0, 3)
	f.backend.waitBodies("native", bodies, true)
	p.waitExport("acknowledged")
	p.signal()
	p.wait(5*time.Second, true)
	p.assertDrain("completed")
	f.backend.assertOnly("native")
	f.backend.assertRecords("native", bodies, false)
	if !strings.Contains(p.output.String(), "synthetic record rejection") {
		t.Fatal("native partial-success rejection was not reported")
	}
}

func TestSIGTERMDuringRetries(t *testing.T) {
	for _, mode := range []string{"legacy", "native"} {
		for _, recovery := range []bool{true, false} {
			name := "outage"
			if recovery {
				name = "recovery"
			}
			t.Run(mode+"/"+name, func(t *testing.T) {
				f := newFixture(t, mode)
				f.backend.setPlan(mode, responsePlan{status: 503})
				p := f.start(nil, true)
				p.ready()
				bodies := f.appendRecords(0, 0, 1)
				eventually(t, 5*time.Second, "an export retry", func() bool { return f.backend.attempts(mode) >= 2 })
				start := time.Now()
				p.signal()
				assertDraining(t, p)
				if recovery {
					f.backend.setPlan(mode, responsePlan{status: 200})
				}
				p.wait(8*time.Second, true)
				t.Logf("SIGTERM drain: %s", time.Since(start).Round(time.Millisecond))
				assertOrdinaryDrain(t, p, recovery)
				f.backend.assertOnly(mode)
				if recovery {
					f.backend.assertRecords(mode, bodies, true)
				} else if len(f.backend.bodies(mode, true)) != 0 {
					t.Fatal("outage unexpectedly acknowledged records")
				}
			})
		}
	}
}

func assertDraining(t *testing.T, p *process) {
	t.Helper()
	eventually(t, 2*time.Second, "unready while draining", func() bool {
		return p.status("/ready") == http.StatusServiceUnavailable
	})
	if p.status("/live") != http.StatusOK {
		t.Fatal("liveness failed during export drain")
	}
	select {
	case <-p.done:
		t.Fatal("process exited before its in-flight retry completed")
	default:
	}
}

func assertOrdinaryDrain(t *testing.T, p *process, recovered bool) {
	t.Helper()
	outcome := "failed"
	if recovered {
		outcome = "completed"
	}
	p.assertDrain(outcome)
	if !p.event("Logs export drain finished", map[string]any{
		"outcome": outcome, "outstanding_calls": float64(0), "deadline_expired": float64(0),
		"drain_rejected": float64(0), "exporter_stopped": true,
	}) {
		t.Fatal("normal retry outcome was not finalized after exporter shutdown")
	}
}

func TestCapabilityRestartDuringRetries(t *testing.T) {
	for _, oldMode := range []string{"legacy", "native"} {
		for _, recovery := range []bool{true, false} {
			name := "outage"
			if recovery {
				name = "recovery"
			}
			t.Run(oldMode+"/"+name, func(t *testing.T) {
				newMode := otherMode(oldMode)
				f := newFixture(t, oldMode)
				f.backend.setPlan(oldMode, responsePlan{status: 503})
				p := f.start(nil, true)
				p.ready()
				before := f.appendRecords(0, 0, 2)
				eventually(t, 5*time.Second, "old-mode retry", func() bool { return f.backend.attempts(oldMode) >= 2 })
				f.backend.setFeature(newMode, 200, "")
				assertDraining(t, p)
				if recovery {
					f.backend.setPlan(oldMode, responsePlan{status: 200})
				}
				p.wait(8*time.Second, true)
				assertOrdinaryDrain(t, p, recovery)
				f.backend.assertOnly(oldMode)
				if recovery {
					f.backend.assertRecords(oldMode, before, true)
				}
				state := readControllerState(t, f)
				if state["pending_intent"] != true || state["old_mode"] != oldMode || state["new_mode"] != newMode {
					t.Fatalf("incorrect restart intent: %+v", state)
				}
				termination, err := os.ReadFile(f.settings.Termination)
				if err != nil || !strings.Contains(string(termination), oldMode) || !strings.Contains(string(termination), newMode) {
					t.Fatal("restart termination message missing transition")
				}
				checkpoints, err := os.ReadDir(f.settings.Checkpoints)
				if err != nil || len(checkpoints) == 0 {
					t.Fatal("Filelog did not leave checkpoint files")
				}
				p2 := f.start(nil, true)
				p2.ready()
				after := f.appendRecords(0, 2, 2)
				f.backend.waitBodies(newMode, after, true)
				p2.signal()
				p2.wait(5*time.Second, true)
				p2.assertDrain("completed")
				f.backend.assertRecords(newMode, after, false)
				if !p2.event("Logs capability restart intent observed", map[string]any{"selected_requested_mode": true}) {
					t.Fatal("replacement did not observe matching intent")
				}
				next := readControllerState(t, f)
				if next["pending_intent"] != false || next["last_attempt_at"] != state["last_attempt_at"] {
					t.Fatal("replacement failed to clear intent while retaining cooldown")
				}
				for _, body := range before {
					if f.backend.bodies(newMode, false)[body] != 0 {
						t.Fatal("replacement replayed a checkpointed record")
					}
				}
			})
		}
	}
}

func readControllerState(t *testing.T, f *fixture) map[string]any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.settings.Controller, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]any
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func otherMode(mode string) string {
	if mode == "native" {
		return "legacy"
	}
	return "native"
}

func TestConcurrentFiles(t *testing.T) {
	for _, mode := range []string{"legacy", "native"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t, mode)
			f.backend.setPlan(mode, responsePlan{status: 200, failures: 4})
			p := f.start(nil, true)
			p.ready()
			var expected []string
			for file := 0; file < f.settings.Files; file++ {
				expected = append(expected, f.appendRecords(file, 0, 10)...)
			}
			f.backend.waitBodies(mode, expected, true)
			p.signal()
			p.wait(5*time.Second, true)
			assertOrdinaryDrain(t, p, true)
			f.backend.assertOnly(mode)
			f.backend.assertRecords(mode, expected, true)
			f.backend.assertIdentity()
		})
	}
}

func TestFreshStateReplaysAvailableFiles(t *testing.T) {
	for _, mode := range []string{"legacy", "native"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t, mode)
			before := f.appendRecords(0, 0, 3)
			p := f.start(nil, true)
			p.ready()
			f.backend.waitBodies(mode, before, true)
			p.signal()
			p.wait(5*time.Second, true)
			f.settings.Checkpoints = filepath.Join(f.root, "replacement-checkpoints")
			f.settings.Controller = filepath.Join(f.root, "replacement-controller")
			p2 := f.start(nil, true)
			p2.ready()
			eventually(t, 5*time.Second, "fresh-state replay", func() bool {
				counts := f.backend.bodies(mode, true)
				for _, body := range before {
					if counts[body] < 2 {
						return false
					}
				}
				return true
			})
			p2.signal()
			p2.wait(5*time.Second, true)
			counts := f.backend.bodies(mode, true)
			for _, body := range before {
				if counts[body] != 2 {
					t.Fatalf("fresh state delivered record %d times, want 2", counts[body])
				}
			}
			t.Logf("fresh state replayed %d of %d records", len(counts), len(before))
		})
	}
}
