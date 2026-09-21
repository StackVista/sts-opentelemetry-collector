//nolint:goconst // Keep wire fields and comparison scenarios beside their measurements.
package logsagent_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type comparisonResult struct {
	Design                string         `json:"design"`
	Transport             string         `json:"transport"`
	Scenario              string         `json:"scenario"`
	Records               int            `json:"physical_records"`
	RecordBytes           int            `json:"record_body_bytes"`
	Files                 int            `json:"concurrent_files"`
	Format                string         `json:"format"`
	Delivered             int            `json:"delivered"`
	Lost                  int            `json:"lost"`
	Duplicates            int            `json:"duplicates"`
	Requests              int            `json:"requests"`
	BackendConcurrency    int            `json:"backend_concurrency"`
	UndeliveredAfter500ms int            `json:"undelivered_after_500ms"`
	Seconds               float64        `json:"seconds"`
	RecordsPerSecond      float64        `json:"records_per_second"`
	RequestsPerSecond     float64        `json:"requests_per_second"`
	CPUSeconds            float64        `json:"cpu_seconds"`
	PeakRSSKiB            int64          `json:"peak_rss_kib"`
	DrainSeconds          float64        `json:"drain_seconds"`
	Drain                 map[string]any `json:"drain"`
}

func comparisonCounts(b *backend) (map[string]int, map[string]bool, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	accepted := make(map[string]int)
	attempted := make(map[string]bool)
	for _, req := range b.requests {
		for _, record := range req.records {
			attempted[record.body] = true
			if req.status == http.StatusOK {
				accepted[record.body]++
			}
		}
	}
	return accepted, attempted, len(b.requests)
}

func comparisonSources(t *testing.T, f *fixture, count, size int, docker bool) []string {
	t.Helper()
	paths := make([]string, f.settings.Files)
	for file := range f.settings.Files {
		path := f.sourcePath(file)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		var data strings.Builder
		for id := file; id < count; id += f.settings.Files {
			body := fmt.Sprintf("record=%08d ", id)
			body += strings.Repeat("x", size-len(body))
			if docker {
				line, err := json.Marshal(map[string]string{
					"log": body, "stream": "stdout", "time": "2026-09-10T12:00:00.123456789Z",
				})
				if err != nil {
					t.Fatal(err)
				}
				data.Write(line)
				data.WriteByte('\n')
			} else {
				fmt.Fprintf(&data, "2026-09-10T12:00:00.123456789Z stdout F %s\n", body)
			}
		}
		if err := os.WriteFile(path+".pending", []byte(data.String()), 0600); err != nil {
			t.Fatal(err)
		}
		paths[file] = path
	}
	return paths
}

func TestQueueComparison(t *testing.T) {
	design := os.Getenv("OTEL_COMPARISON_DESIGN")
	if design == "" {
		t.Skip("comparative capacity run requires OTEL_COMPARISON_DESIGN")
	}
	if design != "queued" && design != "synchronous" {
		t.Fatal("unknown comparison design")
	}
	for _, transport := range []string{"legacy", "native", "native_grpc"} {
		for _, scenario := range []struct {
			name    string
			records int
			size    int
			delay   time.Duration
			outage  bool
			recover bool
			docker  bool
		}{
			{"healthy", 32768, 512, 0, false, false, false},
			{"saturation", 4096, 512, 100 * time.Millisecond, false, false, false},
			{"docker_saturation", 4098, 512, 100 * time.Millisecond, false, false, true},
			{"outage_recovery", 8192, 512, 0, true, true, false},
			{"retry_exhaustion", 400, 512, 0, true, false, false},
			{"near_record_limit", 16, 255 * 1024, 0, false, false, false},
		} {
			t.Run(transport+"/"+scenario.name, func(t *testing.T) {
				f, mode := newTransportFixture(t, transport)
				format := "CRI"
				if scenario.docker {
					f.settings.Files = 6
					format = "Docker"
				}
				f.backend.responseDelay = scenario.delay
				if scenario.outage {
					f.backend.setPlan(mode, responsePlan{status: http.StatusServiceUnavailable})
				}
				paths := comparisonSources(t, f, scenario.records, scenario.size, scenario.docker)
				p := f.start(func(config map[string]any) {
					section(config, "service", "telemetry", "logs")["level"] = "info"
				}, true)
				p.ready()
				started := time.Now()
				if scenario.recover {
					timer := time.AfterFunc(time.Second, func() {
						f.backend.setPlan(mode, responsePlan{status: http.StatusOK})
					})
					defer timer.Stop()
				}
				for _, path := range paths {
					if err := os.Rename(path+".pending", path); err != nil {
						t.Fatal(err)
					}
				}
				undelivered := 0
				sampled := false
				eventually(t, 45*time.Second, "all numbered records observed", func() bool {
					accepted, attempted, _ := comparisonCounts(f.backend)
					if !sampled && time.Since(started) >= 500*time.Millisecond {
						undelivered, sampled = scenario.records-len(accepted), true
					}
					if scenario.outage && !scenario.recover {
						return len(attempted) == scenario.records
					}
					return len(accepted) == scenario.records
				})
				if !sampled {
					early, _, _ := comparisonCounts(f.backend)
					undelivered = scenario.records - len(early)
				}
				drainStarted := time.Now()
				p.signal()
				p.wait(f.settings.Lifetime+3*time.Second, true)
				drainSeconds := time.Since(drainStarted).Seconds()
				elapsed := time.Since(started).Seconds()
				accepted, _, requests := comparisonCounts(f.backend)
				duplicates := 0
				for _, count := range accepted {
					duplicates += count - 1
				}
				if duplicates != 0 {
					t.Fatalf("comparison duplicated %d record IDs", duplicates)
				}
				expected := scenario.records
				if scenario.outage && !scenario.recover {
					expected = 0
				}
				if len(accepted) != expected {
					t.Fatalf("delivered %d records, expected %d", len(accepted), expected)
				}
				f.backend.assertOnly(mode)
				f.backend.assertIdentity()
				drains := stressEvents(p, "Logs export drain finished", "")
				if len(drains) != 1 || drains[0]["outstanding_calls"] != float64(0) ||
					drains[0]["exporter_stopped"] != true {
					t.Fatal("comparison did not finalize export drain")
				}
				usage, ok := p.cmd.ProcessState.SysUsage().(*syscall.Rusage)
				if !ok {
					t.Fatal("child resource usage unavailable")
				}
				f.backend.mu.Lock()
				concurrency := f.backend.maxActive
				f.backend.mu.Unlock()
				result := comparisonResult{
					Design: design, Transport: transport, Scenario: scenario.name,
					Records: scenario.records, RecordBytes: scenario.size,
					Files: f.settings.Files, Format: format,
					Delivered: len(accepted), Lost: scenario.records - len(accepted), Duplicates: duplicates,
					Requests: requests, BackendConcurrency: concurrency, UndeliveredAfter500ms: undelivered,
					Seconds: elapsed, RecordsPerSecond: float64(len(accepted)) / elapsed,
					RequestsPerSecond: float64(requests) / elapsed,
					CPUSeconds:        p.cmd.ProcessState.UserTime().Seconds() + p.cmd.ProcessState.SystemTime().Seconds(),
					PeakRSSKiB:        usage.Maxrss, DrainSeconds: drainSeconds, Drain: drains[0],
				}
				data, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("QUEUE_COMPARISON %s", data)
			})
		}
	}
}
