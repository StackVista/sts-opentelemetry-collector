package stsk8slogsexporter //nolint:testpackage // Exercises the internal sender and failure classifications.

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	syntheticKey  = "synthetic-api-key"
	phaseBefore   = "before"
	phaseDuring   = "during"
	phaseLifetime = "lifetime"
	phaseAttempt  = "attempt"
	recovery      = "recover"
	permanent     = "permanent"
	exhausted     = "exhausted"
	throttled     = "retry_after"
	beyondBudget  = "retry_after_beyond_budget"
	lostResponse  = "lost_response"
)

func cloneTransport(t *testing.T, transport http.RoundTripper) *http.Transport {
	t.Helper()
	base, ok := transport.(*http.Transport)
	if !ok {
		t.Fatal("expected standard HTTP transport")
	}
	return base.Clone()
}

func testSender(t *testing.T, server *httptest.Server) *sender {
	t.Helper()
	s, err := newSender(server.Client(), server.URL+"/deployment/stsAgent/logs/k8s", syntheticKey, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSingleAttemptStatusAndWire(t *testing.T) {
	for _, status := range []int{200, 202, 204, 299, 301, 302, 307, 308, 400, 401, 403, 404, 408, 413, 429, 500, 503, 599} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			payload := testEncoder(t).encode(testLogs()).payload
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodPost || r.URL.Path != "/deployment/stsAgent/logs/k8s" || r.URL.RawQuery != "" ||
					r.Header.Get("Content-Type") != "application/x-protobuf" || r.Header.Get("sts-api-key") != syntheticKey {
					t.Error("invalid request path or headers")
				}
				body, err := io.ReadAll(r.Body)
				if err != nil || !bytes.Equal(payload, body) {
					t.Error("payload changed during HTTP send")
				}
				decodeRequest(t, body)
				w.Header().Set("Location", "/must-not-follow")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "synthetic-sensitive-body "+syntheticKey)
			}))
			defer server.Close()
			err := testSender(t, server).send(context.Background(), payload)
			if calls.Load() != 1 {
				t.Fatal("single-attempt sender retried or followed a redirect")
			}
			if status >= 200 && status < 300 {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			permanent := status != 408 && status != 429 && status < 500
			if err == nil || consumererror.IsPermanent(err) != permanent {
				t.Fatalf("incorrect status classification: %v", err)
			}
			var failure *exportFailure
			if !errors.As(err, &failure) || failure.status != status {
				t.Fatal("missing bounded failure information")
			}
			if strings.Contains(err.Error(), syntheticKey) || strings.Contains(err.Error(), "synthetic-sensitive-body") ||
				strings.Contains(err.Error(), server.URL) {
				t.Fatal("error exposed credentials, response data or endpoint")
			}
		})
	}
}

func TestSenderValidation(t *testing.T) {
	client := &http.Client{Transport: cloneTransport(t, http.DefaultTransport)}
	for _, endpoint := range []string{"/relative", "ftp://host/stsAgent/logs/k8s", "http://host", "http://user:synthetic@host/stsAgent/logs/k8s",
		"http://host/stsAgent/logs/k8s?key=synthetic", "http://host/stsAgent/logs/k8s?", "http://host/stsAgent/logs/k8s#"} {
		if _, err := newSender(client, endpoint, syntheticKey, time.Second); err == nil || strings.Contains(err.Error(), "synthetic") {
			t.Fatal("invalid endpoint accepted or leaked")
		}
	}
	for _, key := range []string{"", " ", "synthetic\r\n", "synthetic\x00", "synthetic\x7f"} {
		if _, err := newSender(client, "http://host/stsAgent/logs/k8s", key, time.Second); err == nil || strings.Contains(err.Error(), "synthetic") {
			t.Fatal("invalid credential accepted or leaked")
		}
	}
	for _, client := range []*http.Client{nil, {}} {
		if _, err := newSender(client, "http://host/stsAgent/logs/k8s", syntheticKey, time.Second); err == nil {
			t.Fatal("accepted ambient transport")
		}
	}
	if _, err := newSender(client, "http://host/stsAgent/logs/k8s", syntheticKey, 0); err == nil {
		t.Fatal("accepted unbounded attempt timeout")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSenderDeadlinesAndCancellation(t *testing.T) {
	for _, phase := range []string{phaseBefore, phaseDuring, phaseLifetime, phaseAttempt} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				type contextKey struct{}
				parent := context.WithValue(context.Background(), contextKey{}, "preserved")
				ctx, cancel := context.WithCancel(parent)
				defer cancel()
				if phase == phaseBefore {
					cancel()
				}
				if phase == phaseLifetime {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, time.Second)
					defer stop()
				}
				client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					calls++
					if r.Context().Value(contextKey{}) != "preserved" {
						t.Error("attempt lost context values")
					}
					if phase == phaseDuring {
						cancel()
					}
					<-r.Context().Done()
					return nil, r.Context().Err()
				})}
				s, err := newSender(client, "http://host/stsAgent/logs/k8s", syntheticKey, 2*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				started := time.Now()
				err = s.send(ctx, []byte("payload"))
				var failure *exportFailure
				if !errors.As(err, &failure) {
					t.Fatal("missing failure classification")
				}
				switch phase {
				case phaseBefore, phaseDuring:
					if failure.reason != "canceled" || !errors.Is(err, context.Canceled) || time.Since(started) != 0 {
						t.Fatal("cancellation did not stop immediately")
					}
				case phaseLifetime:
					if failure.reason != "deadline_expired" || !errors.Is(err, context.DeadlineExceeded) || time.Since(started) != time.Second {
						t.Fatal("lifetime expiry not distinguished")
					}
				case phaseAttempt:
					if failure.reason != failureAttemptTimeout || errors.Is(err, context.DeadlineExceeded) || time.Since(started) != 2*time.Second {
						t.Fatal("attempt timeout misclassified as lifetime expiry")
					}
				}
				if calls > 1 || (phase == phaseBefore && calls != 0) {
					t.Fatal("unexpected HTTP attempt count")
				}
			})
		})
	}
}

func TestSenderHTTPContextCancellation(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
		close(canceled)
	}))
	defer server.Close()
	s := testSender(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.send(ctx, []byte("payload")) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach server")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sender did not stop")
	}
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("request context did not reach server")
	}
}

func TestSenderTLSAndProxyTransport(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer server.Close()
	for _, mode := range []string{"untrusted", "custom_ca", "skip_verification"} {
		t.Run(mode, func(t *testing.T) {
			client := server.Client()
			transport := cloneTransport(t, client.Transport)
			transport.TLSClientConfig.RootCAs = x509.NewCertPool()
			if mode == "custom_ca" {
				transport.TLSClientConfig.RootCAs.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
			}
			transport.TLSClientConfig.InsecureSkipVerify = mode == "skip_verification"
			client = &http.Client{Transport: transport}
			s, err := newSender(client, server.URL+"/stsAgent/logs/k8s", syntheticKey, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			err = s.send(context.Background(), []byte("payload"))
			if mode == "untrusted" {
				var failure *exportFailure
				if !consumererror.IsPermanent(err) || !errors.As(err, &failure) || failure.reason != failureConfiguration {
					t.Fatalf("trust error not permanent: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if s.client.Transport != transport {
				t.Fatal("sender replaced the supplied transport")
			}
			transport.CloseIdleConnections()
		})
	}
	proxyCalls := 0
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalls++
		if r.URL.String() != "http://unresolvable.invalid/stsAgent/logs/k8s" {
			t.Error("proxy target changed")
		}
		w.WriteHeader(204)
	}))
	defer proxy.Close()
	proxyURL := proxy.URL
	transport := cloneTransport(t, http.DefaultTransport)
	request, _ := http.NewRequest(http.MethodGet, proxyURL, nil)
	transport.Proxy = http.ProxyURL(request.URL)
	defer transport.CloseIdleConnections()
	s, err := newSender(&http.Client{Transport: transport}, "http://unresolvable.invalid/stsAgent/logs/k8s", syntheticKey, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.send(context.Background(), []byte("payload")); err != nil || proxyCalls != 1 {
		t.Fatalf("explicit proxy was not used: %v", err)
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"2", 2 * time.Second}, {" 3 ", 3 * time.Second},
		{now.Add(5 * time.Second).Format(http.TimeFormat), 5 * time.Second},
		{now.Add(-time.Second).Format(http.TimeFormat), 0},
		{"", 0}, {"-1", 0}, {"invalid", 0}, {"0.5", 0},
		{"184467440737095516160", time.Duration(math.MaxInt64)},
		{"18446744073709551615", time.Duration(math.MaxInt64)},
	} {
		if got := retryAfter(tc.value, now); got != tc.want {
			t.Errorf("retryAfter(%q) = %v, want %v", tc.value, got, tc.want)
		}
	}
}

func TestExporterHelperOwnsRetries(t *testing.T) {
	for _, scenario := range []string{recovery, permanent, exhausted, throttled, beyondBudget, lostResponse} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls, stored int
				var attempts []time.Time
				client := &http.Client{Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
					calls++
					attempts = append(attempts, time.Now())
					status := 503
					switch scenario {
					case recovery, throttled:
						if calls == 2 {
							status = 204
						}
					case permanent:
						status = 401
					case lostResponse:
						stored++
						if calls == 1 {
							return nil, errors.New("synthetic-private-transport-error " + syntheticKey)
						}
						status = 204
					}
					header := make(http.Header)
					switch scenario {
					case throttled:
						header.Set("Retry-After", "2")
					case beyondBudget:
						header.Set("Retry-After", "999999999999999999999")
					}
					return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(""))}, nil
				})}
				s, err := newSender(client, "http://host/stsAgent/logs/k8s", syntheticKey, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				e := testEncoder(t)
				retry := configretry.NewDefaultBackOffConfig()
				retry.InitialInterval, retry.MaxInterval, retry.MaxElapsedTime = time.Second, time.Second, 3*time.Second
				retry.RandomizationFactor = 0
				helper, err := exporterhelper.NewLogs(context.Background(),
					exportertest.NewNopSettings(component.MustNewType("stsk8slogs")), &struct{}{},
					func(ctx context.Context, logs plog.Logs) error { return s.send(ctx, e.encode(logs).payload) },
					exporterhelper.WithRetry(retry),
				)
				if err != nil {
					t.Fatal(err)
				}
				if err := helper.Start(context.Background(), componenttest.NewNopHost()); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := helper.Shutdown(context.Background()); err != nil {
						t.Error(err)
					}
				}()
				err = helper.ConsumeLogs(context.Background(), testLogs())
				switch scenario {
				case recovery, throttled, lostResponse:
					if err != nil || calls != 2 {
						t.Fatalf("recovery failed: calls=%d error=%v", calls, err)
					}
					if scenario == throttled && attempts[1].Sub(attempts[0]) < 2*time.Second {
						t.Fatal("retried earlier than Retry-After")
					}
					if scenario == lostResponse && stored != 2 {
						t.Fatal("lost-response duplicate was not exercised")
					}
				case permanent, beyondBudget:
					if err == nil || calls != 1 {
						t.Fatalf("unexpected retry: calls=%d error=%v", calls, err)
					}
				case exhausted:
					if err == nil || calls != 4 || errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("retry exhaustion misclassified: calls=%d error=%v", calls, err)
					}
				}
				if err != nil && strings.Contains(err.Error(), syntheticKey) {
					t.Fatal("helper exposed raw transport error")
				}
			})
		})
	}
}
