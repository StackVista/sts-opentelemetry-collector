//nolint:goconst // Keep fixture fields beside the exact expected lifecycle outcomes.
package logsagent_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/confmap"
)

func stressSource(t *testing.T, f *fixture, file int, content string) {
	t.Helper()
	path := filepath.Join(f.root, "pods",
		fmt.Sprintf("default_stress-%d_12345678-1234-1234-1234-123456789abc", file), "app", "0.log")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func stressPartials(t *testing.T, f *fixture, sources int) ([]string, []string) {
	t.Helper()
	partials := make([]string, 0, sources)
	markers := make([]string, 0, sources)
	for i := range sources {
		partial := fmt.Sprintf("partial-source=%d %s", i, strings.Repeat("synthetic ", 16))
		marker := fmt.Sprintf("parsed-source=%d", i)
		docker, err := json.Marshal(map[string]string{
			"log": marker, "stream": "stdout", "time": "2026-09-10T12:00:00.123456789Z",
		})
		if err != nil {
			t.Fatal(err)
		}
		// Docker entries are emitted after the same batch's CRI fragments have been buffered.
		stressSource(t, f, i, "2026-09-10T12:00:00.123456789Z stdout P "+partial+"\n"+string(docker)+"\n")
		partials = append(partials, partial)
		markers = append(markers, marker)
	}
	return partials, markers
}

func stressValidated(t *testing.T, mutate func(map[string]any)) func(map[string]any) {
	t.Helper()
	return func(config map[string]any) {
		if mutate != nil {
			mutate(config)
		}
		section(config, "service", "telemetry", "logs")["sampling"] = map[string]any{"enabled": false}
		if _, err := logsagent.ValidatePipelineConfig(confmap.NewFromStringMap(config)); err != nil {
			t.Fatalf("stress fixture violates pipeline bounds: %v", err)
		}
	}
}

func stressEvents(p *process, message, outcome string) []map[string]any {
	var events []map[string]any
	for _, line := range strings.Split(p.output.String(), "\n") {
		var event map[string]any
		if json.Unmarshal([]byte(line), &event) == nil && event["msg"] == message &&
			(outcome == "" || event["outcome"] == outcome) {
			events = append(events, event)
		}
	}
	return events
}

func stressFinal(t *testing.T, p *process, failed, deadlines, rejected int) {
	t.Helper()
	if !p.event("Logs export drain finished", map[string]any{
		"outcome": "failed", "outstanding_calls": float64(0), "failed_calls": float64(failed),
		"deadline_expired": float64(deadlines), "drain_rejected": float64(rejected), "exporter_stopped": true,
	}) {
		t.Fatalf("missing exact final drain counters: failed=%d deadline=%d rejected=%d", failed, deadlines, rejected)
	}
}

func stressPending(t *testing.T, f *fixture, mode string, partials []string) {
	t.Helper()
	counts := f.backend.bodies(mode, false)
	for _, body := range partials {
		if counts[body] != 0 {
			t.Fatal("partial source flushed before the shutdown scenario began")
		}
	}
}

func TestStressSequentialPartialDrain(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"promtail", "native"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, mode)
			const sources = 16
			partials, markers := stressPartials(t, f, sources)
			p := f.start(stressValidated(t, nil), true)
			p.ready()
			f.backend.waitBodies(mode, markers, true)
			stressPending(t, f, mode, partials)
			f.backend.setPlan(mode, responsePlan{status: http.StatusServiceUnavailable})
			started := time.Now()
			p.signal()
			assertDraining(t, p)
			p.wait(f.settings.Lifetime+3*time.Second, true)
			elapsed := time.Since(started)

			exhausted := stressEvents(p, "Logs export completed", "retry_exhausted")
			rejected := stressEvents(p, "Logs export rejected", "drain_budget_insufficient")
			if len(exhausted) < 2 || len(rejected) == 0 || len(exhausted)+len(rejected) != sources {
				t.Fatalf("sequential partials: retry_exhausted=%d drain_budget_insufficient=%d sources=%d",
					len(exhausted), len(rejected), sources)
			}
			for _, event := range append(exhausted, rejected...) {
				if event["draining"] != true || event["log_records"] != float64(1) || event["mode"] != mode {
					t.Fatalf("partial flush did not preserve one-source synchronous drain: %+v", event)
				}
			}
			stressFinal(t, p, len(exhausted), 0, len(rejected))
			counts := f.backend.bodies(mode, false)
			attempted := 0
			for _, body := range partials {
				if counts[body] > 0 {
					attempted++
					if counts[body] < 2 {
						t.Fatal("an exhausted partial source did not retry")
					}
				}
				if f.backend.bodies(mode, true)[body] != 0 {
					t.Fatal("outage acknowledged a partial source")
				}
			}
			if attempted != len(exhausted) {
				t.Fatalf("backend saw %d partial sources, terminal results cover %d", attempted, len(exhausted))
			}
			f.backend.assertOnly(mode)
			f.backend.assertIdentity()
			t.Logf("%d partials exhausted retries, %d rejected before export; absolute drain finished in %s",
				len(exhausted), len(rejected), elapsed.Round(time.Millisecond))
		})
	}
}

func stressStopped(t *testing.T, p *process) {
	t.Helper()
	eventually(t, time.Second, "own collector child stopped", func() bool {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", p.cmd.Process.Pid))
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "State:") {
				return strings.Contains(line, "T (stopped)")
			}
		}
		return false
	})
}

func TestStressTimerFlushDeadlineAfterSchedulingStall(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"promtail", "native"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, mode)
			const sources = 4
			partials, markers := stressPartials(t, f, sources)
			p := f.start(stressValidated(t, nil), true)
			p.ready()
			f.backend.waitBodies(mode, markers, true)
			stressPending(t, f, mode, partials)
			f.backend.setPlan(mode, responsePlan{status: http.StatusServiceUnavailable})
			eventually(t, 8*time.Second, "partial timer flush holding the recombiner lock during retries", func() bool {
				counts := f.backend.bodies(mode, false)
				for _, body := range partials {
					if counts[body] >= 2 {
						return true
					}
				}
				return false
			})
			if len(stressEvents(p, "Logs export completed", "retry_exhausted")) != 0 {
				t.Fatal("timer export already completed before the contention scenario")
			}
			p.signal()
			assertDraining(t, p)
			if err := p.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.cmd.Process.Signal(syscall.SIGCONT) })
			stressStopped(t, p)
			for _, event := range stressEvents(p, "Logs export completed", "") {
				if event["outcome"] != "acknowledged" {
					t.Fatalf("scheduling-stall setup lost the timer export before SIGSTOP: outcome=%v", event["outcome"])
				}
			}
			if len(stressEvents(p, "Logs export rejected", "")) != 0 {
				t.Fatal("scheduling-stall setup reached late drain rejections before SIGSTOP")
			}
			stoppedBodies := f.backend.bodies(mode, false)
			stoppedSources := 0
			for _, body := range partials {
				if stoppedBodies[body] > 0 {
					stoppedSources++
					if stoppedBodies[body] < 2 {
						t.Fatal("scheduling-stall setup stopped a partial source before its retry")
					}
				}
			}
			if stoppedSources != 1 {
				t.Fatalf("scheduling-stall setup reached %d partial sources before SIGSTOP, want 1", stoppedSources)
			}
			// Exceed the validated lifetime without changing Collector or exporter timing invariants.
			timer := time.NewTimer(f.settings.Lifetime + time.Second)
			defer timer.Stop()
			<-timer.C
			stressStopped(t, p)
			attempts := f.backend.attempts(mode)
			if err := p.cmd.Process.Signal(syscall.SIGCONT); err != nil {
				t.Fatal(err)
			}
			p.wait(5*time.Second, true)
			if len(stressEvents(p, "Logs export completed", "deadline_expired")) != 1 ||
				len(stressEvents(p, "Logs export completed", "retry_exhausted")) != 0 ||
				len(stressEvents(p, "Logs export rejected", "drain_budget_insufficient")) != sources-1 {
				t.Fatal("resumed timer flush did not distinguish the expired call from late drain rejections")
			}
			stressFinal(t, p, 1, 1, sources-1)
			if f.backend.attempts(mode) != attempts {
				t.Fatal("expired export or late partials made new backend requests after resume")
			}
			attempted := 0
			for _, body := range partials {
				if f.backend.bodies(mode, false)[body] > 0 {
					attempted++
				}
				if f.backend.bodies(mode, true)[body] != 0 {
					t.Fatal("outage acknowledged a partial source")
				}
			}
			if attempted != 1 {
				t.Fatalf("backend saw %d partial sources, want only the original timer flush", attempted)
			}
			f.backend.assertOnly(mode)
		})
	}
}

func stressMetrics(t *testing.T) (int, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	number, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return number, "http://" + address + "/metrics"
}

func stressMetric(p *process, endpoint, metric, reason string) float64 {
	resp, err := p.client.Get(endpoint)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(io.LimitReader(resp.Body, 4<<20))
	total := float64(0)
	found := false
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 || strings.HasPrefix(line, "#") {
			continue
		}
		name, _, _ := strings.Cut(fields[0], "{")
		if !strings.HasSuffix(strings.TrimSuffix(name, "_total"), metric) ||
			!strings.Contains(line, `reason="`+reason+`"`) {
			continue
		}
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			p.t.Fatal(err)
		}
		total += value
		found = true
	}
	if err := scanner.Err(); err != nil {
		p.t.Fatal(err)
	}
	if !found {
		return -1
	}
	return total
}

func TestStressSourceSizeRejections(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"promtail", "native"} {
		for _, scenario := range []struct {
			reason string
			count  int
			bytes  int
		}{
			{"record_too_large", 1, 262144},
			{"request_too_large", 5, 220000},
		} {
			t.Run(mode+"/"+scenario.reason, func(t *testing.T) {
				f := newFixture(t, mode)
				var input strings.Builder
				for i := range scenario.count {
					fmt.Fprintf(&input, "2026-09-10T12:00:00.123456789Z stdout F %s\n",
						strings.Repeat(string(rune('a'+i)), scenario.bytes))
				}
				stressSource(t, f, 0, input.String())
				port, endpoint := stressMetrics(t)
				p := f.start(stressValidated(t, func(config map[string]any) {
					section(config, "receivers", "file_log/pods")["max_log_size"] = "512KiB"
					section(config, "service", "telemetry")["metrics"] = map[string]any{
						"level": "detailed",
						"readers": []any{map[string]any{"pull": map[string]any{"exporter": map[string]any{
							"prometheus": map[string]any{"host": "127.0.0.1", "port": port},
						}}}},
					}
				}), true)
				p.ready()
				eventually(t, 5*time.Second, "specific source size rejection", func() bool {
					return p.event("Logs export rejected", map[string]any{
						"outcome": scenario.reason, "mode": mode, "log_records": float64(scenario.count), "draining": false,
					})
				})
				eventually(t, 3*time.Second, "exact rejected request and record counters", func() bool {
					return stressMetric(p, endpoint, "stslogsagent_pre_export_rejected_requests", scenario.reason) == 1 &&
						stressMetric(p, endpoint, "stslogsagent_pre_export_rejected_records", scenario.reason) == float64(scenario.count)
				})
				if len(f.backend.snapshot()) != 0 {
					t.Fatal("oversized source data reached an exporter")
				}
				valid := f.appendRecords(1, 0, 1)
				f.backend.waitBodies(mode, valid, true)
				p.waitExport("acknowledged")
				p.signal()
				p.wait(5*time.Second, true)
				assertOrdinaryDrain(t, p, true)
				f.backend.assertRecords(mode, valid, true)
				f.backend.assertOnly(mode)
			})
		}
	}
}
