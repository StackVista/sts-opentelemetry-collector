package stsk8slogsexporter //nolint:testpackage // Exercises the factory's queue and transport integration.

import (
	"context"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/xconfmap"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func factoryConfig(t *testing.T, endpoint string) *Config {
	t.Helper()
	cfg, ok := NewFactory().CreateDefaultConfig().(*Config)
	if !ok {
		t.Fatal("factory returned an unexpected config type")
	}
	cfg.Endpoint = endpoint
	cfg.APIKey = configopaque.String(syntheticKey)
	cfg.ClusterName = testClusterName
	return cfg
}

func newStartedLogsExporter(t *testing.T, cfg *Config) exporter.Logs {
	t.Helper()
	exp, err := NewFactory().CreateLogs(
		context.Background(),
		exportertest.NewNopSettings(component.MustNewType("stsk8slogs")),
		cfg,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := exp.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return exp
}

func TestFactoryDefaultsAndConfiguration(t *testing.T) {
	const queueKey = "sending_queue"
	const retryKey = "retry_on_failure"
	const enabledKey = "enabled"
	factory := NewFactory()
	cfg, ok := factory.CreateDefaultConfig().(*Config)
	if !ok {
		t.Fatal("factory returned an unexpected config type")
	}
	if factory.Type() != component.MustNewType("stsk8slogs") || cfg.TimeoutSettings.Timeout != 5*time.Second ||
		!cfg.BackOffConfig.Enabled || cfg.BackOffConfig.InitialInterval != time.Second ||
		cfg.BackOffConfig.MaxInterval != 5*time.Second || cfg.BackOffConfig.MaxElapsedTime != 30*time.Second ||
		cfg.QueueSettings.HasValue() {
		t.Fatal("factory defaults do not preserve legacy delivery guarantees")
	}
	if err := componenttest.CheckConfigStruct(cfg); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name        string
		data        map[string]any
		valid       bool
		decodeFails bool
	}{
		{
			name: "full",
			data: map[string]any{
				"tls": map[string]any{
					"ca_file":              "/etc/ssl/promtail-ca.pem",
					"insecure_skip_verify": true,
				},
				"proxy_url": "http://proxy.example:8080",
				"timeout":   "7s",
				retryKey: map[string]any{
					"initial_interval": "2s",
					"max_interval":     "4s",
					"max_elapsed_time": "20s",
				},
				queueKey: map[string]any{enabledKey: false},
			},
			valid: true,
		},
		{
			name: "batch",
			data: map[string]any{
				queueKey: map[string]any{"batch": map[string]any{}},
			},
		},
		{
			name:        "disabled_queue",
			decodeFails: true,
			data: map[string]any{
				queueKey: false,
			},
		},
		{name: "disabled_queue_section", valid: true, data: map[string]any{queueKey: map[string]any{enabledKey: false}}},
		{name: "no_result_wait", data: map[string]any{queueKey: map[string]any{"wait_for_result": false}}},
		{name: "no_overflow_wait", data: map[string]any{queueKey: map[string]any{"block_on_overflow": false}}},
		{name: "disabled_retries", data: map[string]any{retryKey: map[string]any{enabledKey: false}}},
		{name: "unbounded_retries", data: map[string]any{retryKey: map[string]any{"max_elapsed_time": "0s"}}},
		{
			name: "storage",
			data: map[string]any{
				queueKey: map[string]any{"storage": "file_storage"},
			},
		},
		{
			name: "zero_retry_duration",
			data: map[string]any{
				retryKey: map[string]any{
					"initial_interval": "0s",
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := factory.CreateDefaultConfig()
			data := map[string]any{
				"endpoint":     "https://logs.example/stsAgent/logs/k8s",
				"api_key":      syntheticKey,
				"cluster_name": testClusterName,
			}
			for key, value := range tc.data {
				data[key] = value
			}
			if err := confmap.NewFromStringMap(data).Unmarshal(cfg); err != nil {
				if tc.decodeFails {
					return
				}
				t.Fatal(err)
			}
			if tc.decodeFails {
				t.Fatal("accepted a disabled queue")
			}
			err := xconfmap.Validate(cfg)
			if tc.valid {
				if err != nil {
					t.Fatal(err)
				}
				decoded, ok := cfg.(*Config)
				if !ok {
					t.Fatal("unexpected config type")
				}
				if tc.name == "full" && (decoded.TimeoutSettings.Timeout != 7*time.Second || decoded.ProxyURL != "http://proxy.example:8080" ||
					decoded.TLS.CAFile != "/etc/ssl/promtail-ca.pem" || !decoded.TLS.InsecureSkipVerify ||
					decoded.QueueSettings.HasValue()) {
					t.Fatal("configuration did not decode into the factory config")
				}
			} else if err == nil {
				t.Fatal("accepted an unsafe queue or retry configuration")
			}
		})
	}
}

func TestFactoryPipelineStatusHandling(t *testing.T) {
	const authenticationScenario = "authentication"
	const recoveryScenario = "recovery"
	for _, scenario := range []string{"success", authenticationScenario, recoveryScenario} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/stsAgent/logs/k8s" ||
					r.Header.Get("sts-api-key") != syntheticKey {
					t.Error("factory did not preserve sender wire contract")
				}
				_, _ = io.Copy(io.Discard, r.Body)
				call := calls.Add(1)
				switch scenario {
				case authenticationScenario:
					w.WriteHeader(http.StatusUnauthorized)
				case recoveryScenario:
					if call == 1 {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					w.WriteHeader(http.StatusAccepted)
				}
			}))
			defer server.Close()

			cfg := factoryConfig(t, server.URL+"/stsAgent/logs/k8s")
			cfg.BackOffConfig.InitialInterval = time.Millisecond
			cfg.BackOffConfig.MaxInterval = time.Millisecond
			cfg.BackOffConfig.MaxElapsedTime = time.Second
			cfg.BackOffConfig.RandomizationFactor = 0
			exp := newStartedLogsExporter(t, cfg)
			if exp.Capabilities().MutatesData {
				t.Fatal("factory incorrectly reports that it mutates telemetry")
			}
			err := exp.ConsumeLogs(context.Background(), testLogs())
			if scenario == authenticationScenario {
				if err == nil || calls.Load() != 1 {
					t.Fatalf("authentication failure retried: calls=%d error=%v", calls.Load(), err)
				}
				return
			}
			wantCalls := int32(1)
			if scenario == recoveryScenario {
				wantCalls = 2
			}
			if err != nil || calls.Load() != wantCalls {
				t.Fatalf("pipeline result: calls=%d error=%v", calls.Load(), err)
			}
		})
	}
}

func TestFactorySynchronousCompletionAndCancellation(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()
	exp := newStartedLogsExporter(t, factoryConfig(t, server.URL+"/stsAgent/logs/k8s"))
	done := make(chan error, 1)
	go func() { done <- exp.ConsumeLogs(context.Background(), testLogs()) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("backend not called")
	}
	select {
	case err := <-done:
		t.Fatalf("returned before completion: %v", err)
	default:
	}
	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() { canceled <- exp.ConsumeLogs(ctx, testLogs()) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("second synchronous call serialized")
	}
	cancel()
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation ignored")
	}
	finish()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("completion ignored")
	}
}

func TestFactoryTLSProxyAndRedactedConfigurationErrors(t *testing.T) {
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer tlsServer.Close()
	caFile := t.TempDir() + "/receiver-ca.pem"
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: tlsServer.Certificate().Raw,
	}), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := factoryConfig(t, tlsServer.URL+"/stsAgent/logs/k8s")
	cfg.TLS.CAFile = caFile
	exp := newStartedLogsExporter(t, cfg)
	if err := exp.ConsumeLogs(context.Background(), testLogs()); err != nil {
		t.Fatal(err)
	}

	var proxyCalls atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalls.Add(1)
		if r.URL.String() != "http://unresolvable.invalid/stsAgent/logs/k8s" {
			t.Error("proxy target changed")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer proxy.Close()
	cfg = factoryConfig(t, "http://unresolvable.invalid/stsAgent/logs/k8s")
	cfg.ProxyURL = configopaque.String(proxy.URL)
	exp = newStartedLogsExporter(t, cfg)
	if err := exp.ConsumeLogs(context.Background(), testLogs()); err != nil || proxyCalls.Load() != 1 {
		t.Fatalf("explicit proxy was not used: calls=%d error=%v", proxyCalls.Load(), err)
	}

	cfg = factoryConfig(t, "https://user:private-endpoint@logs.example/stsAgent/logs/k8s?private=query")
	cfg.APIKey = configopaque.String("private-api-key")
	err := cfg.Validate()
	if err == nil || strings.Contains(err.Error(), "private") {
		t.Fatalf("invalid URL was accepted or leaked secret data: %v", err)
	}
}
