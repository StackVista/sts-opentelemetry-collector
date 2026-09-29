package stsk8slogsexporter //nolint:testpackage // Checks attempt classification through the factory's helper chain.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestFactoryRetriesAttemptTimeouts(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, req.Body)
		<-req.Context().Done()
	}))
	defer server.Close()
	cfg := factoryConfig(t, server.URL+"/stsAgent/logs/k8s")
	cfg.TimeoutSettings.Timeout = 20 * time.Millisecond
	cfg.BackOffConfig.InitialInterval = time.Millisecond
	cfg.BackOffConfig.MaxInterval = time.Millisecond
	cfg.BackOffConfig.MaxElapsedTime = 200 * time.Millisecond
	cfg.BackOffConfig.RandomizationFactor = 0
	exp := newStartedLogsExporter(t, cfg)
	err := exp.ConsumeLogs(context.Background(), testLogs())
	var failure *exportFailure
	if !errors.As(err, &failure) || failure.reason != failureAttemptTimeout {
		t.Fatalf("attempt timeout was mistaken for an export lifetime deadline: %v", err)
	}
	if calls.Load() < 2 {
		t.Fatalf("attempt timeout was not retried: %d calls", calls.Load())
	}
}
