//nolint:testpackage // Exercises restart failure injection and concurrent lifecycle callbacks.
package stslogscapabilityextension

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/openapiclient/features"
	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func testController(t *testing.T, restart func() error) *controller {
	t.Helper()
	cfg, ok := createDefaultConfig().(*Config)
	if !ok {
		t.Fatal("wrong default config")
	}
	cfg.ReceiverURL = "http://127.0.0.1:1/stsAgent"
	cfg.APIKey = configopaque.String("synthetic-logs-test")
	cfg.StateDirectory = t.TempDir()
	cfg.TerminationMessagePath = filepath.Join(t.TempDir(), "termination")
	cfg.HealthEndpoint = "127.0.0.1:0"
	cfg.PollInterval = time.Hour
	cfg.AttemptTimeout = 100 * time.Millisecond
	cfg.QueryTimeout = 200 * time.Millisecond
	cfg.MaxAttempts = 1
	set := extensiontest.NewNopSettings(component.MustNewType("stslogscapability"))
	c, err := newController(*cfg, set, restart)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Shutdown(context.Background()) })
	return c
}

func makeReady(c *controller, mode logsagent.Mode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mode = mode
	c.initialized, c.configured, c.ready = true, true, true
	c.observer = &logsagent.Accounting{}
	c.bounds.ExportLifetime = 90 * time.Second
	c.bounds.QueueRetryBound = 70 * time.Second
	c.bounds.LegacyExporterID = "stsk8slogs/legacy"
	c.bounds.NativeExporterID = "otlp_http/native"
}

func observation(mode logsagent.Mode) features.Result {
	return features.Result{Class: features.Valid, Features: map[string]any{"otel-logs": mode == logsagent.Native}}
}

func TestStartupClassification(t *testing.T) {
	for _, tc := range []struct {
		class features.Class
		mode  logsagent.Mode
		fail  bool
	}{
		{features.Valid, logsagent.Legacy, false}, {features.Unsupported, logsagent.Legacy, false},
		{features.Transient, logsagent.Legacy, false}, {features.Timeout, logsagent.Legacy, false},
		{features.Malformed, logsagent.Legacy, false}, {features.Authentication, "", true},
		{features.Configuration, "", true}, {features.Rejected, "", true}, {features.Canceled, "", true},
	} {
		t.Run(string(tc.class), func(t *testing.T) {
			mode, err := startupMode(features.Result{Class: tc.class})
			if (err != nil) != tc.fail || mode != tc.mode {
				t.Fatalf("mode %q error %v", mode, err)
			}
		})
	}
}

func TestDiscoveryUsesSharedClientAndHealth(t *testing.T) {
	for _, tc := range []struct {
		body   string
		status int
		mode   logsagent.Mode
		fail   bool
	}{
		{`{"otel-logs":true}`, 200, logsagent.Native, false},
		{`{"otel-logs":true,"k8s-rbac":"true","capacity":42}`, 200, logsagent.Native, false},
		{`{"otel-logs":false,"k8s-rbac":null}`, 200, logsagent.Legacy, false},
		{`{}`, 200, logsagent.Legacy, false},
		{`unavailable`, 503, logsagent.Legacy, false},
		{`not found`, 404, logsagent.Legacy, false},
		{`{"otel-logs":"true"}`, 200, logsagent.Legacy, false},
		{`{"otel-logs":null,"k8s-rbac":true}`, 200, logsagent.Legacy, false},
		{`denied`, 401, "", true},
		{`forbidden`, 403, "", true},
	} {
		t.Run(tc.body, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/prefix/stsAgent/features" || r.Header.Get("Authorization") != "ApiKey synthetic-logs-test" {
					t.Error("wrong discovery path or authentication")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			c := testController(t, func() error { return nil })
			c.cfg.ReceiverURL = server.URL + "/prefix/stsAgent"
			err := c.Start(context.Background(), nil)
			if (err != nil) != tc.fail || c.SelectedMode() != tc.mode {
				t.Fatalf("mode %q error %v", c.SelectedMode(), err)
			}
			if tc.fail {
				return
			}
			if calls.Load() != 1 {
				t.Fatalf("unexpected discovery count %d", calls.Load())
			}
			checkHealth(t, c, "/ready", http.StatusServiceUnavailable)
			makeReady(c, tc.mode)
			checkHealth(t, c, "/ready", http.StatusOK)
			c.observe(features.Result{Class: features.Authentication})
			checkHealth(t, c, "/ready", http.StatusServiceUnavailable)
			c.observe(features.Result{Class: features.Transient})
			checkHealth(t, c, "/ready", http.StatusServiceUnavailable)
			c.observe(observation(tc.mode))
			checkHealth(t, c, "/ready", http.StatusOK)
			_ = c.NotReady()
			checkHealth(t, c, "/ready", http.StatusServiceUnavailable)
			checkHealth(t, c, "/live", http.StatusOK)
		})
	}
}

func checkHealth(t *testing.T, c *controller, path string, status int) {
	t.Helper()
	response := httptest.NewRecorder()
	c.server.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != status {
		t.Fatalf("%s: got %d, want %d", path, response.Code, status)
	}
}

func TestFailedRestartRecoveryAndCooldown(t *testing.T) {
	for _, stage := range []string{restartMarker, restartMessage, restartSignal} {
		t.Run(stage, func(t *testing.T) {
			signals, writes, messages := 0, 0, 0
			fail := true
			c := testController(t, func() error {
				signals++
				if stage == restartSignal && fail {
					return errors.New("synthetic")
				}
				return nil
			})
			makeReady(c, logsagent.Legacy)
			now := time.Now()
			c.now = func() time.Time { return now }
			c.writeState = func(directory string, state restartState) error {
				writes++
				if stage == restartMarker && fail {
					return errors.New("synthetic")
				}
				return saveState(directory, state)
			}
			c.writeMessage = func(path string, data []byte) error {
				messages++
				if stage == restartMessage && fail {
					return errors.New("synthetic")
				}
				return os.WriteFile(path, data, 0o600)
			}
			for range 3 {
				c.observe(observation(logsagent.Native))
			}
			if c.restartPending || c.observations != 0 || c.mode != logsagent.Legacy || !c.ready {
				t.Fatal("failed attempt changed routing or remained latched")
			}
			if c.state.LastAttemptAt != now || writes != 1 {
				t.Fatal("failed attempt did not establish cooldown")
			}
			fail = false
			for range 5 {
				c.observe(observation(logsagent.Native))
			}
			if writes != 1 {
				t.Fatal("restart attempted during cooldown")
			}
			now = now.Add(c.cfg.RestartCooldown)
			for range 2 {
				c.observe(observation(logsagent.Native))
			}
			if writes != 1 {
				t.Fatal("cooldown retained old observations")
			}
			c.observe(observation(logsagent.Native))
			if !c.restartPending || writes != 2 || messages < 1 || signals < 1 {
				t.Fatal("recovery did not request restart")
			}
			count := signals
			c.observe(features.Result{Class: features.Authentication})
			c.observe(observation(logsagent.Legacy))
			c.observe(observation(logsagent.Native))
			if !c.restartPending || signals != count || c.mode != logsagent.Legacy {
				t.Fatal("successful signal latch was undone")
			}
			persisted, err := loadState(c.cfg.StateDirectory)
			if err != nil || !persisted.PendingIntent || persisted.NewMode != logsagent.Native {
				t.Fatalf("restart intent: %+v %v", persisted, err)
			}
		})
	}
}

func TestObservationResetsAndBothDirections(t *testing.T) {
	for _, initial := range []logsagent.Mode{logsagent.Legacy, logsagent.Native} {
		t.Run(string(initial), func(t *testing.T) {
			target := logsagent.Native
			if initial == target {
				target = logsagent.Legacy
			}
			signals := 0
			c := testController(t, func() error { signals++; return nil })
			makeReady(c, initial)
			for _, reset := range []features.Result{
				{Class: features.Transient}, {Class: features.Malformed}, {Class: features.Authentication},
				{Class: features.Unsupported}, {Class: features.Rejected}, {Class: features.Configuration},
				observation(initial),
			} {
				c.observe(observation(target))
				c.observe(observation(target))
				c.observe(reset)
				if signals != 0 || c.observations != 0 {
					t.Fatal("failed/current observation did not reset sequence")
				}
			}
			for range 3 {
				c.observe(observation(target))
			}
			if signals != 1 || c.SelectedMode() != initial {
				t.Fatal("transition changed the fixed route")
			}
		})
	}
}

func TestDrainDeadlineAndExporterCompletion(t *testing.T) {
	c := testController(t, func() error { return nil })
	makeReady(c, logsagent.Legacy)
	now := time.Now()
	c.now = func() time.Time { return now }
	_ = c.NotReady()
	deadline := c.DrainDeadline()
	now = now.Add(time.Minute)
	_ = c.NotReady()
	if c.DrainDeadline() != deadline {
		t.Fatal("drain deadline moved")
	}
	core, logs := observer.New(zap.InfoLevel)
	c.set.Logger = zap.New(core)
	exporter := componentstatus.NewInstanceID(component.MustNewIDWithName("stsk8slogs", "legacy"),
		component.KindExporter)
	c.ComponentStatusChanged(exporter, componentstatus.NewEvent(componentstatus.StatusStopping))
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	entry := logs.FilterMessage("Logs export drain finished").All()[0]
	if entry.ContextMap()["outcome"] != "incomplete" {
		t.Fatal("zero calls was treated as completed exporter work")
	}
}

func TestStateStartupRetainsCooldownAndChoosesIndependently(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c := testController(t, func() error { return nil })
	c.cfg.ReceiverURL = server.URL + "/stsAgent"
	state := restartState{SchemaVersion: 1, LastAttemptAt: time.Now(),
		OldMode: logsagent.Legacy, NewMode: logsagent.Native, PendingIntent: true}
	if err := saveState(c.cfg.StateDirectory, state); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	got, err := loadState(c.cfg.StateDirectory)
	if err != nil || got.PendingIntent || !got.LastAttemptAt.Equal(state.LastAttemptAt) {
		t.Fatalf("startup state %+v %v", got, err)
	}
	if c.SelectedMode() != logsagent.Legacy {
		t.Fatal("pending intent forced startup mode")
	}
}

func TestCorruptStateRejected(t *testing.T) {
	for _, data := range []string{
		"", "null", "{}", `{"schema_version":2}`, "invalid", strings.Repeat("x", 16385),
		`{"last_attempt_at":"2026-01-01T00:00:00Z","old_mode":"legacy","new_mode":"native","pending_intent":true}`,
		`{"schema_version":null,"last_attempt_at":"2026-01-01T00:00:00Z","old_mode":"legacy","new_mode":"native"}`,
	} {
		t.Run(data[:min(len(data), 20)], func(t *testing.T) {
			c := testController(t, func() error { return nil })
			if err := os.WriteFile(filepath.Join(c.cfg.StateDirectory, "state.json"), []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := c.Start(context.Background(), nil); err == nil {
				t.Fatal("corrupt state accepted")
			}
		})
	}
}

func TestShutdownPreventsRestartWhileWritingIntent(t *testing.T) {
	for _, stage := range []string{restartMarker, restartMessage} {
		t.Run(stage, func(t *testing.T) {
			var signals atomic.Int32
			c := testController(t, func() error { signals.Add(1); return nil })
			makeReady(c, logsagent.Legacy)
			writing, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			if stage == restartMarker {
				c.writeState = func(string, restartState) error {
					close(writing)
					<-release
					return nil
				}
			} else {
				c.writeMessage = func(string, []byte) error {
					close(writing)
					<-release
					return nil
				}
			}
			c.observe(observation(logsagent.Native))
			c.observe(observation(logsagent.Native))
			go func() {
				defer close(finished)
				c.observe(observation(logsagent.Native))
			}()
			<-writing
			_ = c.NotReady()
			close(release)
			<-finished
			if signals.Load() != 0 || c.SelectedMode() != logsagent.Legacy {
				t.Fatal("restart signal escaped into an already stopping service")
			}
		})
	}
}

func TestConcurrentStatusAndShutdown(t *testing.T) {
	c := testController(t, func() error { return nil })
	makeReady(c, logsagent.Native)
	exporter := componentstatus.NewInstanceID(component.MustNewIDWithName("otlp_http", "native"),
		component.KindExporter)
	var workers sync.WaitGroup
	for range 10 {
		workers.Go(func() {
			for range 100 {
				c.observe(observation(logsagent.Native))
				c.ComponentStatusChanged(exporter, componentstatus.NewEvent(componentstatus.StatusOK))
				_ = c.DrainDeadline()
			}
		})
	}
	_ = c.NotReady()
	workers.Wait()
	c.ComponentStatusChanged(exporter, componentstatus.NewEvent(componentstatus.StatusStopped))
}

func TestPollingStartsAfterReadyAndJoinsOnShutdown(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) > 1 {
			close(started)
			<-r.Context().Done()
			close(stopped)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c := testController(t, func() error { return nil })
	c.cfg.ReceiverURL = server.URL + "/stsAgent"
	c.cfg.PollInterval = 20 * time.Millisecond
	c.cfg.Jitter = 0
	c.cfg.AttemptTimeout = 5 * time.Second
	c.cfg.QueryTimeout = 10 * time.Second
	if err := c.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if c.poller != nil {
		t.Fatal("polling began before Ready")
	}
	makeReady(c, logsagent.Legacy)
	if err := c.Ready(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("polling did not begin")
	}
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.poller.Done():
	default:
		t.Fatal("shutdown did not join poller")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("poller left HTTP request running")
	}
}

func TestConcurrentShutdownFinalizesOnce(t *testing.T) {
	c := testController(t, func() error { return nil })
	makeReady(c, logsagent.Legacy)
	core, entries := observer.New(zap.InfoLevel)
	c.set.Logger = zap.New(core)
	exporter := componentstatus.NewInstanceID(component.MustNewIDWithName("stsk8slogs", "legacy"),
		component.KindExporter)
	c.ComponentStatusChanged(exporter, componentstatus.NewEvent(componentstatus.StatusStopped))
	var workers sync.WaitGroup
	for range 10 {
		workers.Go(func() {
			if err := c.Shutdown(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	if entries.FilterMessage("Logs export drain finished").Len() != 1 {
		t.Fatal("shutdown did not finalize exactly once")
	}
}

var _ extension.Extension = (*controller)(nil)
