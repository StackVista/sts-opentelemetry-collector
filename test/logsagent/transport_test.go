//nolint:goconst // Keep transport fixture fields and expected outcomes visible at their assertions.
package logsagent_test

import (
	"encoding/pem"

	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"testing"
	"time"
)

func transportTLS(f *fixture) (*httptest.Server, string) {
	f.t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(f.backend.serveHTTP))
	f.t.Cleanup(server.Close)
	path := filepath.Join(f.root, "ca.pem")
	data := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(path, data, 0600); err != nil {
		f.t.Fatal(err)
	}
	return server, path
}

func assertTransportDelivery(f *fixture, p *process, mode string) {
	f.t.Helper()
	p.ready()
	bodies := f.appendRecords(0, 0, 3)
	f.backend.waitBodies(mode, bodies, true)
	p.waitExport("acknowledged")
	p.signal()
	p.wait(5*time.Second, true)
	assertOrdinaryDrain(f.t, p, true)
	f.backend.assertOnly(mode)
	f.backend.assertRecords(mode, bodies, false)
	f.backend.assertIdentity()
}

func TestTransportCustomCAHTTP(t *testing.T) {
	for _, mode := range []string{"promtail"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t, mode)
			server, ca := transportTLS(f)

			f.settings.PromtailURL = server.URL + "/stsAgent/logs/k8s"

			p := f.start(func(config map[string]any) {
				for _, component := range []map[string]any{
					section(config, "exporters", "stsk8slogs/promtail"),
				} {
					component["tls"] = map[string]any{"ca_file": ca}
				}
			}, true)
			assertTransportDelivery(f, p, mode)
			if f.backend.polls() != 0 {
				t.Fatal("fixed agent queried features")
			}
		})
	}
}

func TestTransportMissingCA(t *testing.T) {
	f := newFixture(t, "promtail")
	server, _ := transportTLS(f)
	f.settings.PromtailURL = server.URL + "/stsAgent/logs/k8s"
	f.appendRecords(0, 0, 1)
	p := f.start(nil, true)
	p.ready()
	p.waitExport("permanent_rejection")
	p.signal()
	p.wait(5*time.Second, true)
	assertOrdinaryDrain(t, p, false)
	if !strings.Contains(p.output.String(), "Promtail-compatible log export configuration") {
		t.Fatal("missing TLS rejection reason")
	}
	if len(f.backend.snapshot()) != 0 {
		t.Fatal("untrusted transport delivered records")
	}
}

func TestTransportHTTPExplicitProxy(t *testing.T) {
	for _, mode := range []string{"promtail"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t, mode)
			target, err := url.Parse(f.backend.server.URL)
			if err != nil {
				t.Fatal(err)
			}
			forward := httputil.NewSingleHostReverseProxy(target)
			transport := &http.Transport{Proxy: nil}
			t.Cleanup(transport.CloseIdleConnections)
			forward.Transport = transport
			var mu sync.Mutex
			paths := make(map[string]int)
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !r.URL.IsAbs() || r.URL.Host != target.Host {
					t.Error("explicit proxy did not receive the absolute backend URL")
					http.Error(w, "unexpected proxy target", http.StatusBadRequest)
					return
				}
				mu.Lock()
				paths[r.URL.Path]++
				mu.Unlock()
				forward.ServeHTTP(w, r)
			}))
			t.Cleanup(proxy.Close)
			f.childEnv = []string{"NO_PROXY=*"}
			p := f.start(func(config map[string]any) {
				for _, component := range []map[string]any{
					section(config, "exporters", "stsk8slogs/promtail"),
				} {
					component["proxy_url"] = proxy.URL
				}
			}, true)
			assertTransportDelivery(f, p, mode)
			mu.Lock()
			defer mu.Unlock()
			path := "/otel/v1/logs"
			if mode == "promtail" {
				path = "/stsAgent/logs/k8s"
			}
			if paths["/stsAgent/features"] != 0 || paths[path] != f.backend.attempts(mode) ||
				paths[path] == 0 || len(paths) != 1 {
				t.Fatalf("Promtail export did not exclusively traverse explicit proxy: %v", paths)
			}
		})
	}
}

// Loopback is implicitly bypassed even without NO_PROXY, so use a local interface
// address to distinguish explicit exclusion from the HTTP library's built-in rule.
func newTransportFixture(t *testing.T, transport string) (*fixture, string) {
	t.Helper()
	if transport != "promtail" {
		t.Fatal("fixed fixture requires promtail")
	}
	return newFixture(t, transport), transport
}
