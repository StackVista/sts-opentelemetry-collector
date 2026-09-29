//nolint:goconst // Keep fixture fields beside the exact expected lifecycle outcomes.
package logsagent_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
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

func stressPending(t *testing.T, f *fixture, partials []string) {
	t.Helper()
	counts := f.backend.bodies(false)
	for _, body := range partials {
		if counts[body] != 0 {
			t.Fatal("partial source flushed before the shutdown scenario began")
		}
	}
}

func TestStressSequentialPartialDrain(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	const sources = 16
	partials, markers := stressPartials(t, f, sources)
	p := f.start(stressValidated(t, nil), true)
	p.ready()
	f.backend.waitBodies(markers, true)
	stressPending(t, f, partials)
	f.backend.setPlan(responsePlan{status: http.StatusServiceUnavailable})
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
		if event["draining"] != true || event["log_records"] != float64(1) {
			t.Fatalf("partial flush did not preserve one-source synchronous drain: %+v", event)
		}
	}
	stressFinal(t, p, len(exhausted), 0, len(rejected))
	counts := f.backend.bodies(false)
	attempted := 0
	for _, body := range partials {
		if counts[body] > 0 {
			attempted++
			if counts[body] < 2 {
				t.Fatal("an exhausted partial source did not retry")
			}
		}
		if f.backend.bodies(true)[body] != 0 {
			t.Fatal("outage acknowledged a partial source")
		}
	}
	if attempted != len(exhausted) {
		t.Fatalf("backend saw %d partial sources, terminal results cover %d", attempted, len(exhausted))
	}
	f.backend.assertOnly()
	f.backend.assertIdentity()
	t.Logf("%d partials exhausted retries, %d rejected before export; absolute drain finished in %s",
		len(exhausted), len(rejected), elapsed.Round(time.Millisecond))
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
	f := newFixture(t)
	const sources = 4
	partials, markers := stressPartials(t, f, sources)
	type heldResponse struct {
		request  request
		canceled <-chan struct{}
	}
	retried := make(chan heldResponse, sources)
	release := make(chan struct{})
	releaseResponse := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseResponse)
	f.backend.setBeforeResponse(func(ctx context.Context, req request) {
		if len(req.records) != 1 || !slices.Contains(partials, req.records[0].body) {
			return
		}
		if f.backend.bodies(false)[req.records[0].body] == 2 {
			retried <- heldResponse{request: req, canceled: ctx.Done()}
			<-release
		}
	})
	p := f.start(stressValidated(t, nil), true)
	p.ready()
	f.backend.waitBodies(markers, true)
	stressPending(t, f, partials)
	f.backend.setPlan(responsePlan{status: http.StatusServiceUnavailable})
	waitRetry := time.NewTimer(8 * time.Second)
	defer waitRetry.Stop()
	var held heldResponse
	select {
	case held = <-retried:
	case <-waitRetry.C:
		t.Fatal("timed out waiting for a partial timer retry at the backend response boundary")
	}
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
	select {
	case <-held.canceled:
		t.Fatal("held partial retry ended before SIGSTOP")
	default:
	}
	for _, event := range stressEvents(p, "Logs export completed", "") {
		if event["outcome"] != "acknowledged" {
			t.Fatalf("scheduling-stall setup lost the timer export before SIGSTOP: outcome=%v", event["outcome"])
		}
	}
	if len(stressEvents(p, "Logs export rejected", "")) != 0 {
		t.Fatal("scheduling-stall setup reached late drain rejections before SIGSTOP")
	}
	stoppedBodies := f.backend.bodies(false)
	stoppedSources := 0
	for _, body := range partials {
		if stoppedBodies[body] > 0 {
			stoppedSources++
			if stoppedBodies[body] != 2 || body != held.request.records[0].body {
				t.Fatal("scheduling-stall setup did not stop the held second attempt")
			}
		}
	}
	if stoppedSources != 1 {
		t.Fatalf("scheduling-stall setup reached %d partial sources before SIGSTOP, want 1", stoppedSources)
	}
	attempts := f.backend.attempts()
	// Exceed the validated lifetime without changing Collector or exporter timing invariants.
	timer := time.NewTimer(f.settings.Lifetime)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-held.canceled:
		t.Fatal("held partial retry ended while the collector was stopped")
	}
	stressStopped(t, p)
	resumedAt := time.Now()
	if err := p.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	p.wait(5*time.Second, true)
	waitCancellation := time.NewTimer(5 * time.Second)
	defer waitCancellation.Stop()
	select {
	case <-held.canceled:
	case <-waitCancellation.C:
		t.Fatal("collector exited without canceling the held partial retry")
	}
	releaseResponse()
	f.backend.server.Close()
	if len(stressEvents(p, "Logs export completed", "deadline_expired")) != 1 ||
		len(stressEvents(p, "Logs export completed", "retry_exhausted")) != 0 ||
		len(stressEvents(p, "Logs export rejected", "drain_budget_insufficient")) != sources-1 {
		t.Fatal("resumed timer flush did not distinguish the expired call from late drain rejections")
	}
	stressFinal(t, p, 1, 1, sources-1)
	if requests := f.backend.snapshot(); len(requests) != attempts {
		for _, req := range requests[attempts:] {
			t.Errorf("unexpected request observed %s relative to SIGCONT: %+v",
				req.receivedAt.Sub(resumedAt), req.records)
		}
		t.Fatal("expired export or late partials made new backend requests after resume")
	}
	attempted := 0
	for _, body := range partials {
		if f.backend.bodies(false)[body] > 0 {
			attempted++
		}
		if f.backend.bodies(true)[body] != 0 {
			t.Fatal("outage acknowledged a partial source")
		}
	}
	if attempted != 1 {
		t.Fatalf("backend saw %d partial sources, want only the original timer flush", attempted)
	}
	f.backend.assertOnly()
	t.Logf("held second attempt observed %s before SIGCONT; one expired call, %d late partials rejected, no new requests",
		resumedAt.Sub(held.request.receivedAt), sources-1)
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
			(reason != "" && !strings.Contains(line, `reason="`+reason+`"`)) {
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

func TestStressSourceSizeHandling(t *testing.T) {
	t.Parallel()
	for _, scenario := range []struct {
		name    string
		count   int
		bytes   int
		dropped bool
	}{
		{"oversized record", 1, 262144, true},
		{"large batch", 5, 220000, false},
		{"reader batch", 100, 11000, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newFixture(t)
			var input strings.Builder
			var expected []string
			for i := range scenario.count {
				body := fmt.Sprintf("%03d %s", i, strings.Repeat("x", scenario.bytes))
				fmt.Fprintf(&input, "2026-09-10T12:00:00.123456789Z stdout F %s\n", body)
				if !scenario.dropped {
					expected = append(expected, body)
				}
			}
			stressSource(t, f, 0, input.String())
			port, endpoint := stressMetrics(t)
			p := f.start(stressValidated(t, func(config map[string]any) {
				section(config, "receivers", "filelog/pods")["max_log_size"] = "512KiB"
				section(config, "service", "telemetry")["metrics"] = map[string]any{
					"level": "detailed",
					"readers": []any{map[string]any{"pull": map[string]any{"exporter": map[string]any{
						"prometheus": map[string]any{"host": "127.0.0.1", "port": port},
					}}}},
				}
			}), true)
			p.ready()
			if scenario.dropped {
				eventually(t, 5*time.Second, "individual oversized record dropped", func() bool {
					return stressMetric(p, endpoint, "stslogsagent_oversized_records", "") == 1
				})
			} else {
				f.backend.waitBodies(expected, true)
				if len(f.backend.snapshot()) < 2 {
					t.Fatal("large batch was not split")
				}
			}
			valid := f.appendRecords(1, 0, 1)
			expected = append(expected, valid...)
			f.backend.waitBodies(expected, true)
			p.waitExport("acknowledged")
			p.signal()
			p.wait(5*time.Second, true)
			assertOrdinaryDrain(t, p, !scenario.dropped)
			f.backend.assertRecords(expected, true)
			f.backend.assertOnly()
			p2 := f.start(nil, true)
			p2.ready()
			valid = f.appendRecords(1, 1, 1)
			expected = append(expected, valid...)
			f.backend.waitBodies(expected, true)
			p2.signal()
			p2.wait(5*time.Second, true)
			f.backend.assertRecords(expected, true)
		})
	}
}

func TestOversizedCRIPreservesHealthyNeighbour(t *testing.T) {
	f := newFixture(t)
	var input strings.Builder
	for range 20 {
		fmt.Fprintf(&input, "2026-09-10T12:00:00.123456789Z stdout P %s\n", strings.Repeat("x", 16000))
	}
	input.WriteString("2026-09-10T12:00:00.123456789Z stdout F end\n2026-09-10T12:00:00.123456789Z stdout F healthy-neighbour\n")
	stressSource(t, f, 0, input.String())
	p := f.start(nil, true)
	p.ready()
	f.backend.waitBodies([]string{"healthy-neighbour"}, true)
	p.waitExport("permanent_rejection")
	p.signal()
	p.wait(5*time.Second, true)
	assertOrdinaryDrain(t, p, false)
	f.backend.assertRecords([]string{"healthy-neighbour"}, true)
	p2 := f.start(nil, true)
	p2.ready()
	valid := f.appendRecords(1, 1, 1)
	f.backend.waitBodies(valid, true)
	p2.signal()
	p2.wait(5*time.Second, true)
	f.backend.assertRecords(append([]string{"healthy-neighbour"}, valid...), true)
	f.backend.assertOnly()
}
