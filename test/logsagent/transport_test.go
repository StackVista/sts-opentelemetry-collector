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

type tlsTrust struct {
	server        *httptest.Server
	caFile        string
	caDir         string
	emptyTrustDir string
}

func transportTLS(f *fixture) tlsTrust {
	f.t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(f.backend.serveHTTP))
	f.t.Cleanup(server.Close)
	trustDir := filepath.Join(f.root, "trust")
	if err := os.Mkdir(trustDir, 0700); err != nil {
		f.t.Fatal(err)
	}
	emptyTrustDir := filepath.Join(f.root, "empty-trust")
	if err := os.Mkdir(emptyTrustDir, 0700); err != nil {
		f.t.Fatal(err)
	}
	path := filepath.Join(trustDir, "ca.pem")
	data := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(path, data, 0600); err != nil {
		f.t.Fatal(err)
	}
	return tlsTrust{server: server, caFile: path, caDir: trustDir, emptyTrustDir: emptyTrustDir}
}

func assertTransportDelivery(f *fixture, p *process) {
	f.t.Helper()
	p.ready()
	bodies := f.appendRecords(0, 0, 3)
	f.backend.waitBodies(bodies, true)
	p.waitExport("acknowledged")
	p.signal()
	p.wait(5*time.Second, true)
	assertOrdinaryDrain(f.t, p, true)
	f.backend.assertOnly()
	f.backend.assertRecords(bodies, false)
	f.backend.assertIdentity()
}

func TestTransportSystemTrust(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  func(*fixture, string, string, string) []string
	}{
		{
			name: "file_only",
			env: func(_ *fixture, caFile, _ string, emptyTrustDir string) []string {
				return []string{"SSL_CERT_FILE=" + caFile, "SSL_CERT_DIR=" + emptyTrustDir}
			},
		},
		{
			name: "directory_only",
			env: func(f *fixture, _ string, caDir, _ string) []string {
				return []string{"SSL_CERT_FILE=" + filepath.Join(f.root, "missing-ca.pem"), "SSL_CERT_DIR=" + caDir}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			trust := transportTLS(f)
			f.settings.PromtailURL = trust.server.URL + "/stsAgent/logs/k8s"
			f.childEnv = tc.env(f, trust.caFile, trust.caDir, trust.emptyTrustDir)
			p := f.start(nil, true)
			assertTransportDelivery(f, p)
			if f.backend.polls() != 0 {
				t.Fatal("fixed agent queried features")
			}
		})
	}
}

func TestTransportMissingCA(t *testing.T) {
	f := newFixture(t)
	trust := transportTLS(f)
	f.settings.PromtailURL = trust.server.URL + "/stsAgent/logs/k8s"
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
	f := newFixture(t)
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
		section(config, "exporters", "stsk8slogs/promtail")["proxy_url"] = proxy.URL
	}, true)
	assertTransportDelivery(f, p)
	mu.Lock()
	defer mu.Unlock()
	path := "/stsAgent/logs/k8s"
	if paths["/stsAgent/features"] != 0 || paths[path] != f.backend.attempts() ||
		paths[path] == 0 || len(paths) != 1 {
		t.Fatalf("Promtail export did not exclusively traverse explicit proxy: %v", paths)
	}
}
