//nolint:testpackage // Exercises health, accounting and concurrent lifecycle callbacks.
package stslogsagentextension

import (
	"context"

	"net/http"
	"net/http/httptest"

	"sync"

	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"

	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func testController(t *testing.T) *controller {
	t.Helper()
	cfg, ok := createDefaultConfig().(*Config)
	if !ok {
		t.Fatal("wrong default config")
	}
	cfg.HealthEndpoint = "127.0.0.1:0"
	set := extensiontest.NewNopSettings(component.MustNewType("stslogsagent"))
	c, err := newController(*cfg, set)
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
	c.bounds.RetryBound = 70 * time.Second
	c.bounds.PromtailExporterID = "stsk8slogs/promtail"
}

func checkHealth(t *testing.T, c *controller, path string, status int) {
	t.Helper()
	response := httptest.NewRecorder()
	c.server.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != status {
		t.Fatalf("%s: got %d, want %d", path, response.Code, status)
	}
}

func TestDrainDeadlineAndExporterCompletion(t *testing.T) {
	c := testController(t)
	makeReady(c, logsagent.PromtailMode)
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
	exporter := componentstatus.NewInstanceID(component.MustNewIDWithName("stsk8slogs", "promtail"),
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

func TestConcurrentStatusAndShutdown(t *testing.T) {
	c := testController(t)
	makeReady(c, logsagent.PromtailMode)
	exporter := componentstatus.NewInstanceID(component.MustNewIDWithName("stsk8slogs", "promtail"),
		component.KindExporter)
	var workers sync.WaitGroup
	for range 10 {
		workers.Go(func() {
			for range 100 {
				_ = c.Ready()
				c.ComponentStatusChanged(exporter, componentstatus.NewEvent(componentstatus.StatusOK))
				_ = c.DrainDeadline()
			}
		})
	}
	_ = c.NotReady()
	workers.Wait()
	c.ComponentStatusChanged(exporter, componentstatus.NewEvent(componentstatus.StatusStopped))
}

func TestConcurrentShutdownFinalizesOnce(t *testing.T) {
	c := testController(t)
	makeReady(c, logsagent.PromtailMode)
	core, entries := observer.New(zap.InfoLevel)
	c.set.Logger = zap.New(core)
	exporter := componentstatus.NewInstanceID(component.MustNewIDWithName("stsk8slogs", "promtail"),
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

func TestFixedLifecycle(t *testing.T) {
	c := testController(t)
	if err := c.Ready(); err == nil {
		t.Fatal("uninitialized agent became ready")
	}
	if err := c.RegisterExportObserver(&logsagent.Accounting{}); err == nil {
		t.Fatal("uninitialized agent accepted route")
	}
	if err := c.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if c.SelectedMode() != logsagent.PromtailMode {
		t.Fatal("wrong fixed route")
	}
	checkHealth(t, c, "/live", http.StatusOK)
	checkHealth(t, c, "/ready", http.StatusServiceUnavailable)
	c.bounds = logsagent.PipelineConfig{ExportLifetime: time.Minute, RetryBound: time.Second}
	c.configured = true
	if err := c.Ready(); err == nil {
		t.Fatal("agent without route became ready")
	}
	if err := c.RegisterExportObserver(nil); err == nil {
		t.Fatal("accepted nil observer")
	}
	if err := c.RegisterExportObserver(&logsagent.Accounting{}); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterExportObserver(&logsagent.Accounting{}); err == nil {
		t.Fatal("accepted a second route")
	}
	if err := c.Ready(); err != nil {
		t.Fatal(err)
	}
	checkHealth(t, c, "/ready", http.StatusOK)
	if c.RetryBound() != time.Second {
		t.Fatal("wrong retry bound")
	}
	if err := c.NotReady(); err != nil {
		t.Fatal(err)
	}
	checkHealth(t, c, "/ready", http.StatusServiceUnavailable)
	checkHealth(t, c, "/live", http.StatusOK)
	if err := c.Ready(); err == nil {
		t.Fatal("stopping agent became ready")
	}
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConfigValidation(t *testing.T) {
	for _, endpoint := range []string{"", "invalid", "http://localhost:1234"} {
		if err := (&Config{HealthEndpoint: endpoint}).Validate(); err == nil {
			t.Fatal("invalid health endpoint accepted")
		}
	}
	if err := (&Config{HealthEndpoint: "127.0.0.1:0"}).Validate(); err != nil {
		t.Fatal(err)
	}
	c := testController(t)
	if err := c.NotifyConfig(context.Background(), nil); err == nil {
		t.Fatal("missing config accepted")
	}
}
