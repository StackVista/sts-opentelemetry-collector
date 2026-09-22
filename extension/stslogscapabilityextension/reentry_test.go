//nolint:testpackage // Exercises callbacks on the controller's real polling loop.
package stslogscapabilityextension

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
)

func TestPollingRestartCallbackReentry(t *testing.T) {
	const concurrentShutdown = "concurrent_shutdown"
	for _, action := range []string{"readiness", "shutdown", concurrentShutdown} {
		t.Run(action, func(t *testing.T) {
			var changed atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"otel-logs":%t}`, changed.Load())
			}))
			defer server.Close()
			var c *controller
			var calls atomic.Int32
			entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			c = testController(t, func() error {
				calls.Add(1)
				close(entered)
				if action == concurrentShutdown {
					<-release
				}
				if err := c.Ready(); err != nil && action != concurrentShutdown {
					finished <- err
					return err
				}
				checkHealth(t, c, "/ready", http.StatusServiceUnavailable)
				err := c.NotReady()
				if action != "readiness" {
					err = c.Shutdown(context.Background())
				}
				finished <- err
				return err
			})
			c.cfg.ReceiverURL = server.URL + "/stsAgent"
			c.cfg.PollInterval, c.cfg.Jitter = time.Millisecond, 0
			if err := c.Start(context.Background(), nil); err != nil {
				t.Fatal(err)
			}
			makeReady(c, logsagent.PromtailMode)
			if err := c.Ready(); err != nil {
				t.Fatal(err)
			}
			changed.Store(true)
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("polling did not dispatch restart")
			}
			if action == concurrentShutdown {
				done := make(chan error, 1)
				go func() { done <- c.Shutdown(context.Background()) }()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(2 * time.Second):
					t.Error("shutdown waited for its reentrant callback")
				}
				close(release)
			}
			select {
			case err := <-finished:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("restart callback deadlocked during lifecycle reentry")
			}
			if err := c.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			select {
			case <-c.pollDone:
			default:
				t.Fatal("shutdown did not join observations")
			}
			if calls.Load() != 1 || c.SelectedMode() != logsagent.PromtailMode {
				t.Fatal("duplicate callback or route changed")
			}
		})
	}
}

func TestSchemaOneModeCompatibility(t *testing.T) {
	const nativeWireMode = "native"
	for _, old := range []string{"legacy", nativeWireMode} {
		for _, pending := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pending=%t", old, pending), func(t *testing.T) {
				next := nativeWireMode
				if old == next {
					next = "legacy"
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprintf(w, `{"otel-logs":%t}`, old == nativeWireMode)
				}))
				defer server.Close()
				c := testController(t, func() error { return nil })
				c.cfg.ReceiverURL = server.URL + "/stsAgent"
				now := time.Now().UTC()
				data := fmt.Sprintf(`{"schema_version":1,"last_attempt_at":%q,"old_mode":%q,"new_mode":%q,"pending_intent":%t}`,
					now.Format(time.RFC3339Nano), old, next, pending)
				path := filepath.Join(c.cfg.StateDirectory, "state.json")
				if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
					t.Fatal(err)
				}
				before, err := loadState(c.cfg.StateDirectory)
				if err != nil || before.PendingIntent != pending {
					t.Fatalf("schema-1 decoding: %+v %v", before, err)
				}
				if err := c.Start(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
				after, err := loadState(c.cfg.StateDirectory)
				if err != nil || after.PendingIntent || !after.LastAttemptAt.Equal(now) ||
					string(after.OldMode) != old || string(after.NewMode) != next {
					t.Fatalf("schema-1 state changed: %+v %v", after, err)
				}
				if string(c.SelectedMode()) != old {
					t.Fatal("saved intent overrode fresh capabilities")
				}
				encoded, err := json.Marshal(after)
				if err != nil {
					t.Fatal(err)
				}
				var wire map[string]any
				if err := json.Unmarshal(encoded, &wire); err != nil || wire["old_mode"] != old || wire["new_mode"] != next {
					t.Fatalf("mode serialization changed: %s %v", encoded, err)
				}
			})
		}
	}
}

func TestPollingShutdownJoinsIntentWrite(t *testing.T) {
	for _, stage := range []string{restartMarker, restartMessage} {
		t.Run(stage, func(t *testing.T) {
			var changed atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"otel-logs":%t}`, changed.Load())
			}))
			defer server.Close()
			var signals atomic.Int32
			c := testController(t, func() error { signals.Add(1); return nil })
			c.cfg.ReceiverURL = server.URL + "/stsAgent"
			c.cfg.PollInterval, c.cfg.Jitter = time.Millisecond, 0
			writing, release := make(chan struct{}), make(chan struct{})
			block := func() {
				close(writing)
				<-release
			}
			if stage == restartMarker {
				c.writeState = func(string, restartState) error { block(); return nil }
			} else {
				c.writeMessage = func(string, []byte) error { block(); return nil }
			}
			if err := c.Start(context.Background(), nil); err != nil {
				t.Fatal(err)
			}
			makeReady(c, logsagent.PromtailMode)
			if err := c.Ready(); err != nil {
				t.Fatal(err)
			}
			changed.Store(true)
			select {
			case <-writing:
			case <-time.After(2 * time.Second):
				t.Fatal("polling did not start intent write")
			}
			if err := c.NotReady(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- c.Shutdown(context.Background()) }()
			select {
			case <-done:
				t.Error("shutdown returned before intent write finished")
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("shutdown did not finish after intent write")
			}
			// Joining an observation can leave its suppressed callback scheduled.
			c.signalRestart()
			if signals.Load() != 0 {
				t.Fatal("shutdown did not supersede the restart")
			}
		})
	}
}
