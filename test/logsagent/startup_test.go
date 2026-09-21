//nolint:goconst // Keep fixture mutations and their expected error fields together.
package logsagent_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStartupRejectsInvalidBounds(t *testing.T) {
	type boundCase struct {
		name   string
		reason string
		mutate func(map[string]any)
	}
	tests := make([]boundCase, 0, 27)
	tests = append(tests, []boundCase{
		{"deadline_below_required", "export_lifetime", func(c map[string]any) {
			section(c, "connectors", "stslogsroute/logs")["export_lifetime"] = "22s"
		}},
		{"negative_deadline", "export_lifetime", func(c map[string]any) {
			section(c, "connectors", "stslogsroute/logs")["export_lifetime"] = "-1s"
		}},
		{"admission_below_files_plus_two", "max_concurrent_calls", func(c map[string]any) {
			section(c, "connectors", "stslogsroute/logs")["max_concurrent_calls"] = 5
		}},
		{"receiver_retry_enabled", "retry_on_failure", func(c map[string]any) {
			section(c, "receivers", "filelog/pods", "retry_on_failure")["enabled"] = true
		}},
		{"too_many_files", "max_concurrent_files", func(c map[string]any) {
			section(c, "receivers", "filelog/pods")["max_concurrent_files"] = 7
		}},
	}...)
	for _, exporter := range []string{"stsk8slogs/legacy", "otlp_http/native"} {
		for _, item := range []struct {
			name   string
			reason string
			edit   func(map[string]any)
		}{
			{"enabled_queue", "sending_queue", func(e map[string]any) { e["sending_queue"] = map[string]any{"enabled": true} }},
			{"unlimited_retry", "retry", func(e map[string]any) { section(e, "retry_on_failure")["max_elapsed_time"] = "0s" }},
			{"long_retry", "export_lifetime", func(e map[string]any) { section(e, "retry_on_failure")["max_elapsed_time"] = "10s" }},
			{"missing_timeout", "export_lifetime", func(e map[string]any) { delete(e, "timeout") }},
			{"missing_retry_budget", "export_lifetime", func(e map[string]any) { delete(section(e, "retry_on_failure"), "max_elapsed_time") }},
		} {
			reason := item.reason
			tests = append(tests, boundCase{
				exporter + "/" + item.name, reason, func(c map[string]any) { item.edit(section(c, "exporters", exporter)) },
			})
		}
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "legacy")
			f.appendRecords(0, 0, 1)
			p := f.start(tc.mutate, true)
			p.wait(8*time.Second, false)
			p.assertStartupReason(tc.reason)
			if len(f.backend.snapshot()) != 0 {
				t.Fatal("invalid configuration began collection")
			}
		})
	}
}

func TestStartupAcceptsSafeEffectiveDefaults(t *testing.T) {
	for _, tc := range []struct {
		name, mode, exporter string
		omit                 []string
		retry, timeout       time.Duration
	}{
		{"legacy_timeout", "legacy", "stsk8slogs/legacy", []string{"timeout"}, 2 * time.Second, 5 * time.Second},
		{"native_timeout", "native", "otlp_http/native", []string{"timeout"}, 2 * time.Second, 30 * time.Second},
		{"legacy_retry", "legacy", "stsk8slogs/legacy", []string{"retry_on_failure", "max_elapsed_time"}, 30 * time.Second, 200 * time.Millisecond},
		{"native_retry", "native", "otlp_http/native", []string{"retry_on_failure", "max_elapsed_time"}, 300 * time.Second, 200 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tc.mode)
			effective := f.settings
			effective.RetryBudget, effective.AttemptTimeout = tc.retry, tc.timeout
			f.settings.Lifetime = effective.minimumLifetime()
			p := f.start(func(c map[string]any) {
				exporter := section(c, "exporters", tc.exporter)
				delete(section(exporter, tc.omit[:len(tc.omit)-1]...), tc.omit[len(tc.omit)-1])
			}, true)
			p.ready()
			expected := f.appendRecords(0, 0, 1)
			f.backend.waitBodies(tc.mode, expected, true)
			p.waitExport("acknowledged")
			p.signal()
			p.wait(5*time.Second, true)
			p.assertDrain("completed")
			f.backend.assertOnly(tc.mode)
			f.backend.assertRecords(tc.mode, expected, true)
		})
	}
}

func TestStartupRequiresSynchronousGate(t *testing.T) {
	f := newFixture(t, "legacy")
	f.appendRecords(0, 0, 1)
	p := f.start(nil, false)
	p.wait(8*time.Second, false)
	p.assertStartupReason("logs collection requires --feature-gates=stanza.synchronousLogEmitter")
	if len(f.backend.snapshot()) != 0 {
		t.Fatal("disabled synchronous gate allowed collection")
	}
}

func TestDiscoveryStartup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		starts bool
	}{
		{"unsupported", 404, "", true},
		{"transient", 503, "", true},
		{"malformed", 200, `{"otel-logs":"wrong-type"}`, true},
		{"unauthorized", 401, "", false},
		{"forbidden", 403, "", false},
		{"rejected", 400, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, "native")
			f.backend.setFeature("native", tc.status, tc.body)
			p := f.start(nil, true)
			if !tc.starts {
				p.wait(8*time.Second, false)
				reason := "authentication"
				if tc.status == 400 {
					reason = "rejected"
				}
				p.assertStartupReason("logs capability startup failed: " + reason)
				if len(f.backend.snapshot()) != 0 {
					t.Fatal("discovery startup failure allowed collection")
				}
				return
			}
			p.ready()
			expected := f.appendRecords(0, 0, 1)
			f.backend.waitBodies("legacy", expected, true)
			p.signal()
			p.wait(5*time.Second, true)
			f.backend.assertOnly("legacy")
			f.backend.assertRecords("legacy", expected, true)
		})
	}
}

func TestRuntimeDiscoveryAuthenticationRecovery(t *testing.T) {
	for _, mode := range []string{"legacy", "native"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t, mode)
			p := f.start(nil, true)
			p.ready()
			f.backend.setFeature(mode, 401, "")
			eventually(t, 3*time.Second, "authentication unreadiness", func() bool { return p.status("/ready") == 503 })
			if p.status("/live") != 200 {
				t.Fatal("runtime authentication failure killed liveness")
			}
			expected := f.appendRecords(0, 0, 1)
			f.backend.waitBodies(mode, expected, true)
			f.backend.setFeature(mode, 200, "")
			p.ready()
			p.signal()
			p.wait(5*time.Second, true)
			f.backend.assertOnly(mode)
			f.backend.assertRecords(mode, expected, true)
		})
	}
}

func TestPersistedIntentDoesNotOverrideDiscovery(t *testing.T) {
	for _, selected := range []string{"legacy", "native"} {
		t.Run(selected, func(t *testing.T) {
			f := newFixture(t, selected)
			if err := os.MkdirAll(f.settings.Controller, 0700); err != nil {
				t.Fatal(err)
			}
			state := map[string]any{
				"schema_version": 1, "last_attempt_at": time.Now().UTC().Format(time.RFC3339Nano),
				"old_mode": selected, "new_mode": otherMode(selected), "pending_intent": true,
			}
			data, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(f.settings.Controller, "state.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			p := f.start(nil, true)
			p.ready()
			if !p.event("Logs capability restart intent observed", map[string]any{"selected_requested_mode": false}) {
				t.Fatal("startup did not report that discovery differs from intent")
			}
			f.backend.setFeature(otherMode(selected), 200, "")
			polls := f.backend.polls()
			eventually(t, 3*time.Second, "observations inside persisted cooldown", func() bool { return f.backend.polls() >= polls+6 })
			if p.status("/ready") != 200 {
				t.Fatal("persisted cooldown did not prevent another restart")
			}
			expected := f.appendRecords(0, 0, 1)
			f.backend.waitBodies(selected, expected, true)
			p.signal()
			p.wait(5*time.Second, true)
			f.backend.assertOnly(selected)
			next := readControllerState(t, f)
			if next["pending_intent"] != false || next["last_attempt_at"] != state["last_attempt_at"] {
				t.Fatal("startup lost cooldown or left intent pending")
			}
		})
	}
}

func TestCorruptControllerStateFailsStartup(t *testing.T) {
	f := newFixture(t, "legacy")
	if err := os.MkdirAll(f.settings.Controller, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.settings.Controller, "state.json")
	corrupt := []byte("{corrupt synthetic state")
	if err := os.WriteFile(path, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	p := f.start(nil, true)
	p.wait(8*time.Second, false)
	p.assertStartupReason("invalid logs controller state")
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, corrupt) {
		t.Fatal("corrupt controller state was silently replaced")
	}
	if len(f.backend.snapshot()) != 0 {
		t.Fatal("corrupt controller state allowed collection")
	}
}

func (p *process) assertStartupReason(reason string) {
	p.t.Helper()
	for _, line := range strings.Split(p.output.String(), "\n") {
		if strings.HasPrefix(line, "Error:") && strings.Contains(line, reason) {
			return
		}
	}
	p.t.Fatalf("startup failed without the expected error category %q", reason)
}

func TestCorruptFileStorageFailsStartup(t *testing.T) {
	f := newFixture(t, "legacy")
	p := f.start(nil, true)
	p.ready()
	expected := f.appendRecords(0, 0, 1)
	f.backend.waitBodies("legacy", expected, true)
	p.signal()
	p.wait(5*time.Second, true)
	entries, err := os.ReadDir(f.settings.Checkpoints)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := []byte("corrupt synthetic checkpoint")
	var paths []string
	for _, entry := range entries {
		if entry.Type().IsRegular() {
			path := filepath.Join(f.settings.Checkpoints, entry.Name())
			if err := os.WriteFile(path, corrupt, 0600); err != nil {
				t.Fatal(err)
			}
			paths = append(paths, path)
		}
	}
	if len(paths) == 0 {
		t.Fatal("no actual checkpoint file was available to corrupt")
	}
	attempts := len(f.backend.snapshot())
	p2 := f.start(nil, true)
	p2.wait(8*time.Second, false)
	if len(f.backend.snapshot()) != attempts {
		t.Fatal("corrupt checkpoints allowed source replay")
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(data, corrupt) {
			t.Fatal("recreate:false did not preserve corrupt checkpoint")
		}
	}
}

func TestLegacyInvalidSibling(t *testing.T) {
	f := newFixture(t, "legacy")
	p := f.start(func(c map[string]any) {
		transform := section(c, "processors", "transform/identity")
		statements, ok := transform["log_statements"].([]any)
		if !ok {
			t.Fatal("transform fixture log_statements must be a list")
		}
		transform["log_statements"] = append(statements, map[string]any{
			"context":    "log",
			"statements": []any{`set(body, 42) where IsMatch(body, "record=1 ")`},
		})
	}, true)
	p.ready()
	bodies := f.appendRecords(0, 0, 3)
	valid := []string{bodies[0], bodies[2]}
	f.backend.waitBodies("legacy", valid, true)
	p.waitExport("acknowledged")
	p.signal()
	p.wait(5*time.Second, true)
	f.backend.assertOnly("legacy")
	f.backend.assertRecords("legacy", valid, false)
}
