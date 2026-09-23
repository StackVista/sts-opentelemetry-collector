package stsk8slogsexporter //nolint:testpackage // Exercises delivery around the real exporter-helper retry chain.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

type deliveryController struct {
	component.StartFunc
	component.ShutdownFunc
	observer logsagent.ExportObserver
}

func (*deliveryController) SelectedMode() logsagent.Mode { return logsagent.PromtailMode }
func (*deliveryController) DrainDeadline() time.Time     { return time.Time{} }
func (*deliveryController) RetryBound() time.Duration    { return time.Second }

func (c *deliveryController) RegisterExportObserver(observer logsagent.ExportObserver) error {
	if c.observer != nil {
		return errors.New("duplicate delivery observer")
	}
	c.observer = observer
	return nil
}

type deliveryHost struct {
	component.Host
	id         component.ID
	controller *deliveryController
}

func (h deliveryHost) GetExtensions() map[component.ID]component.Component {
	return map[component.ID]component.Component{h.id: h.controller}
}

func TestDeliveryShutdownPreservesAdmittedRetry(t *testing.T) {
	var attempts atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		if attempts.Add(1) == 1 {
			close(entered)
			<-release
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	cfg := factoryConfig(t, server.URL+"/stsAgent/logs/k8s")
	cfg.BackOffConfig.InitialInterval = time.Millisecond
	cfg.BackOffConfig.MaxInterval = time.Millisecond
	cfg.BackOffConfig.MaxElapsedTime = time.Second
	id := component.MustNewIDWithName("stslogsagent", "logs")
	cfg.Delivery = &logsagent.DeliveryConfig{
		ControllerExtension: id, MaxConcurrentCalls: 8,
		MaxRecordBytes: 262144, MaxRequestBytes: 1048576, ExportLifetime: 10 * time.Second,
	}
	exp, err := NewFactory().CreateLogs(context.Background(),
		exportertest.NewNopSettings(component.MustNewType("stsk8slogs")), cfg)
	if err != nil {
		t.Fatal(err)
	}
	controller := &deliveryController{}
	if err := exp.Start(context.Background(), deliveryHost{componenttest.NewNopHost(), id, controller}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumed := make(chan error, 1)
	go func() { consumed <- exp.ConsumeLogs(ctx, testLogs()) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("export did not reach backend")
	}
	cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- exp.Shutdown(context.Background()) }()
	select {
	case err := <-stopped:
		close(release)
		t.Fatalf("shutdown returned before admitted export completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	for _, done := range []<-chan error{consumed, stopped} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("delivery did not finish")
		}
	}
	snapshot := controller.observer.Snapshot()
	if attempts.Load() != 2 || snapshot.Outstanding != 0 || snapshot.Acknowledged != 1 || snapshot.Failed != 0 {
		t.Fatalf("admitted retry was not acknowledged once: attempts=%d, accounting=%+v", attempts.Load(), snapshot)
	}
}
