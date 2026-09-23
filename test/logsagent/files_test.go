//nolint:goconst // Keep route names beside the process scenarios.
package logsagent_test

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func (f *fixture) sourcePath(file int) string {
	return filepath.Join(f.root, "pods",
		fmt.Sprintf("default_fixture-%d_12345678-1234-1234-1234-123456789abc", file), "app", "0.log")
}

func TestLostResponseDuplicatesStoredRecords(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.backend.setPlan(responsePlan{status: http.StatusOK, lostResponses: 1})
	p := f.start(nil, true)
	p.ready()
	before := f.appendRecords(0, 0, 3)
	p.waitExport("acknowledged")
	p.signal()
	p.wait(5*time.Second, true)
	assertOrdinaryDrain(t, p, true)
	if attempts := f.backend.attempts(); attempts != 2 {
		t.Fatalf("lost response made %d attempts, want 2", attempts)
	}
	assertBodyCopies(t, f.backend, before, 2)
	p2 := f.start(nil, true)
	p2.ready()
	after := f.appendRecords(0, 3, 1)
	f.backend.waitBodies(after, true)
	p2.signal()
	p2.wait(5*time.Second, true)
	assertOrdinaryDrain(t, p2, true)
	assertBodyCopies(t, f.backend, before, 2)
	assertBodyCopies(t, f.backend, after, 1)
	f.backend.assertOnly()
	f.backend.assertIdentity()
}

func TestSourceRotationAndTruncation(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"rename", "copytruncate"} {
		for _, offline := range []bool{false, true} {
			timing := "running"
			if offline {
				timing = "stopped"
			}
			t.Run(operation+"/"+timing, func(t *testing.T) {
				t.Parallel()
				f := newFixture(t)
				before := f.appendTimedRecords(0, 10)
				defaultFingerprint := stressValidated(t, func(config map[string]any) {
					delete(section(config, "receivers", "filelog/pods"), "fingerprint_size")
				})
				p := f.start(defaultFingerprint, true)
				p.ready()
				f.backend.waitBodies(recordBodies(before), true)
				if offline {
					p.signal()
					p.wait(5*time.Second, true)
					assertOrdinaryDrain(t, p, true)
				}
				path := f.sourcePath(0)
				oldContents := readSource(t, path)
				rotateSource(t, path, operation, oldContents)
				after := f.appendTimedRecords(10, 2)
				newContents := readSource(t, path)
				if len(oldContents) < 1000 || len(newContents) >= 1000 ||
					bytes.HasPrefix(oldContents, newContents) || bytes.Equal(oldContents[:32], newContents[:32]) {
					t.Fatal("fixture must replace a full fingerprint with a distinct, shorter timestamped source")
				}
				if !bytes.Equal(readSource(t, path+".1"), oldContents) {
					t.Fatal("rotation did not retain the original file contents")
				}
				if offline {
					p = f.start(defaultFingerprint, true)
					p.ready()
				}
				f.backend.waitBodies(recordBodies(after), true)
				grown := f.appendTimedRecords(12, 12)
				if len(readSource(t, path)) <= len(oldContents) {
					t.Fatal("replacement file did not grow beyond the old offset")
				}
				f.backend.waitBodies(recordBodies(grown), true)
				p.signal()
				p.wait(5*time.Second, true)
				assertOrdinaryDrain(t, p, true)

				p2 := f.start(defaultFingerprint, true)
				p2.ready()
				restarted := f.appendTimedRecords(24, 1)
				f.backend.waitBodies(recordBodies(restarted), true)
				p2.signal()
				p2.wait(5*time.Second, true)
				assertOrdinaryDrain(t, p2, true)
				expected := append(append(append(before, after...), grown...), restarted...)
				f.backend.assertRecords(recordBodies(expected), true)
				assertWireRecords(t, f.backend, expected)
				f.backend.assertOnly()
			})
		}
	}
}

func TestRepeatedFingerprintAfterTruncation(t *testing.T) {
	t.Parallel()
	for _, behavior := range []string{"default", "read_whole_file"} {
		t.Run(behavior, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			before := f.appendRecords(0, 0, 10)
			p := f.start(stressValidated(t, func(config map[string]any) {
				filelog := section(config, "receivers", "filelog/pods")
				filelog["fingerprint_size"] = 32
				if behavior != "default" {
					filelog["on_truncate"] = behavior
				}
			}), true)
			p.ready()
			f.backend.waitBodies(before, true)
			path := f.sourcePath(0)
			oldContents := readSource(t, path)
			rotateSource(t, path, "copytruncate", oldContents)
			after := f.appendRecords(0, 10, 2)
			newContents := readSource(t, path)
			if !bytes.Equal(oldContents[:32], newContents[:32]) {
				t.Fatal("edge fixture did not preserve its fingerprint")
			}
			if behavior == "default" {
				eventually(t, 2*time.Second, "debug-only retained-offset reporting", func() bool {
					return p.event("Stored offset exceeds current file size. Keeping original offset", map[string]any{
						"level": "debug", "stored_offset": float64(len(oldContents)), "current_file_size": float64(len(newContents)),
					})
				})
			} else {
				f.backend.waitBodies(after, true)
			}
			p.signal()
			p.wait(5*time.Second, true)
			assertOrdinaryDrain(t, p, true)
			expected := before
			if behavior != "default" {
				expected = append(expected, after...)
			}
			f.backend.assertRecords(expected, true)
			f.backend.assertOnly()
		})
	}
}

func readSource(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func rotateSource(t *testing.T, path, operation string, contents []byte) {
	t.Helper()
	if operation == "rename" {
		if err := os.Rename(path, path+".1"); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err := os.WriteFile(path+".1", contents, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) appendTimedRecords(first, count int) []wireRecord {
	f.t.Helper()
	path := f.sourcePath(0)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		f.t.Fatal(err)
	}
	out, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		f.t.Fatal(err)
	}
	var records []wireRecord
	for i := first; i < first+count; i++ {
		ts := time.Date(2026, 9, 10, 12, 0, 0, 123456789, time.UTC).Add(time.Duration(i) * time.Second)
		body := fmt.Sprintf("file=0 record=%d %s", i, strings.Repeat("synthetic ", 16))
		if _, err := fmt.Fprintf(out, "%s stdout F %s\n", ts.Format(time.RFC3339Nano), body); err != nil {
			_ = out.Close()
			f.t.Fatal(err)
		}
		records = append(records, wireRecord{
			body: body, timestamp: uint64(ts.UnixNano()), cluster: "fixture-cluster",
			podUID: "12345678-1234-1234-1234-123456789abc", podName: "fixture-0", container: "app", stream: "stdout",
		})
	}
	if err := out.Close(); err != nil {
		f.t.Fatal(err)
	}
	return records
}

func recordBodies(records []wireRecord) []string {
	bodies := make([]string, 0, len(records))
	for _, record := range records {
		bodies = append(bodies, record.body)
	}
	return bodies
}

func assertWireRecords(t *testing.T, b *backend, records []wireRecord) {
	t.Helper()
	expected := make(map[string]wireRecord, len(records))
	for _, record := range records {
		expected[record.body] = record
	}
	for _, req := range b.snapshot() {
		for _, record := range req.records {
			if record != expected[record.body] {
				t.Errorf("incorrect decoded identity or timestamp: %+v", record)
			}
		}
	}
}

func assertBodyCopies(t *testing.T, b *backend, bodies []string, copies int) {
	t.Helper()
	counts := b.bodies(true)
	for _, body := range bodies {
		if counts[body] != copies {
			t.Fatalf("stored record %q appeared %d times, want %d", body, counts[body], copies)
		}
	}
}
