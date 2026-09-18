//nolint:goconst // Keep transport fixture fields and expected outcomes visible at their assertions.
package logsagent_test

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
)

type grpcLogsBackend struct {
	plogotlp.UnimplementedGRPCServer
	backend *backend
	partial bool
}

func (s *grpcLogsBackend) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if values := md.Get("authorization"); len(values) != 1 || values[0] != "SUSEObservability "+syntheticKey {
		s.backend.t.Error("incorrect synthetic native gRPC authorization")
	}
	payload, err := req.MarshalProto()
	if err != nil {
		return plogotlp.NewExportResponse(), err
	}
	records, err := decodeNative(payload)
	if err != nil {
		return plogotlp.NewExportResponse(), err
	}
	s.backend.mu.Lock()
	s.backend.requests = append(s.backend.requests, request{mode: "native", status: http.StatusOK, records: records})
	s.backend.mu.Unlock()
	resp := plogotlp.NewExportResponse()
	if s.partial {
		resp.PartialSuccess().SetRejectedLogRecords(1)
		resp.PartialSuccess().SetErrorMessage("synthetic gRPC record rejection")
	}
	return resp, nil
}

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

func grpcTransport(f *fixture, cert tls.Certificate, host string, partial bool) string {
	f.t.Helper()
	listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		f.t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	})))
	plogotlp.RegisterGRPCServer(server, &grpcLogsBackend{backend: f.backend, partial: partial})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil {
			f.t.Errorf("serve local gRPC: %v", err)
		}
	}()
	f.t.Cleanup(func() {
		server.Stop()
		<-done
	})
	return listener.Addr().String()
}

func useNativeGRPC(config map[string]any, endpoint, ca string) {
	exporters := section(config, "exporters")
	native := section(config, "exporters", "otlp_http/native")
	delete(exporters, "otlp_http/native")
	exporters["otlp/native"] = native
	native["endpoint"] = endpoint
	native["tls"] = map[string]any{
		"ca_file": ca, "include_system_ca_certs_pool": true,
		"server_name_override": "example.com",
	}
	section(config, "service", "pipelines", "logs/native")["exporters"] = []any{"otlp/native"}
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

func TestTransportNativeGRPC(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "success"
		if partial {
			name = "partial_success"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, "native")
			server, ca := transportTLS(f)
			endpoint := grpcTransport(f, server.TLS.Certificates[0], "127.0.0.1", partial)
			p := f.start(func(config map[string]any) {
				useNativeGRPC(config, endpoint, ca)
			}, true)
			assertTransportDelivery(f, p, "native")
			reported := p.event("Partial success response", map[string]any{
				"message": "synthetic gRPC record rejection", "dropped_log_records": float64(1),
			})
			if reported != partial {
				t.Fatalf("gRPC partial-success rejection reported=%v, want %v", reported, partial)
			}
		})
	}
}

func TestTransportCustomCAHTTP(t *testing.T) {
	for _, mode := range []string{"legacy", "native"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t, mode)
			server, ca := transportTLS(f)
			f.settings.ReceiverURL = server.URL + "/stsAgent"
			f.settings.LegacyURL = server.URL + "/stsAgent/logs/k8s"
			f.settings.NativeURL = server.URL + "/otel"
			p := f.start(func(config map[string]any) {
				for _, component := range []map[string]any{
					section(config, "extensions", "stslogscapability/logs"),
					section(config, "exporters", "stsk8slogs/legacy"),
				} {
					component["tls"] = map[string]any{"ca_file": ca}
				}
				section(config, "exporters", "otlp_http/native")["tls"] = map[string]any{
					"ca_file": ca, "include_system_ca_certs_pool": true,
				}
			}, true)
			assertTransportDelivery(f, p, mode)
			if f.backend.polls() == 0 {
				t.Fatal("custom CA discovery was not queried")
			}
		})
	}
}

func TestTransportMissingCA(t *testing.T) {
	for _, client := range []string{"discovery", "legacy", "native_http", "native_grpc"} {
		t.Run(client, func(t *testing.T) {
			mode := "native"
			if client == "legacy" {
				mode = "legacy"
			}
			f := newFixture(t, mode)
			server, ca := transportTLS(f)
			var mutate func(map[string]any)
			switch client {
			case "discovery":
				f.settings.ReceiverURL = server.URL + "/stsAgent"
			case "legacy":
				f.settings.LegacyURL = server.URL + "/stsAgent/logs/k8s"
			case "native_http":
				f.settings.NativeURL = server.URL + "/otel"
			case "native_grpc":
				endpoint := grpcTransport(f, server.TLS.Certificates[0], "127.0.0.1", false)
				mutate = func(config map[string]any) {
					useNativeGRPC(config, endpoint, ca)
					delete(section(config, "exporters", "otlp/native", "tls"), "ca_file")
				}
			}
			f.appendRecords(0, 0, 1)
			p := f.start(mutate, true)
			if client == "discovery" {
				p.wait(5*time.Second, false)
				p.assertStartupReason("logs capability startup failed: configuration")
				if f.backend.polls() != 0 {
					t.Fatal("untrusted discovery reached the HTTP handler")
				}
			} else {
				p.ready()
				if client == "legacy" {
					p.waitExport("permanent_rejection")
				} else {
					p.waitExport("retry_exhausted")
				}
				p.signal()
				p.wait(5*time.Second, true)
				assertOrdinaryDrain(t, p, false)
				reason := "certificate signed by unknown authority"
				if client == "legacy" {
					reason = "legacy log export configuration"
				}
				if !strings.Contains(p.output.String(), reason) {
					t.Fatalf("missing TLS rejection reason %q", reason)
				}
			}
			if len(f.backend.snapshot()) != 0 {
				t.Fatal("untrusted transport delivered records or fell back to another route")
			}
		})
	}
}

func TestTransportHTTPExplicitProxy(t *testing.T) {
	for _, mode := range []string{"legacy", "native"} {
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
					section(config, "extensions", "stslogscapability/logs"),
					section(config, "exporters", "stsk8slogs/legacy"),
					section(config, "exporters", "otlp_http/native"),
				} {
					component["proxy_url"] = proxy.URL
				}
			}, true)
			assertTransportDelivery(f, p, mode)
			mu.Lock()
			defer mu.Unlock()
			path := "/otel/v1/logs"
			if mode == "legacy" {
				path = "/stsAgent/logs/k8s"
			}
			if paths["/stsAgent/features"] != f.backend.polls() || paths[path] != f.backend.attempts(mode) ||
				paths["/stsAgent/features"] == 0 || paths[path] == 0 || len(paths) != 2 {
				t.Fatalf("discovery and selected export did not exclusively traverse explicit proxy: %v", paths)
			}
		})
	}
}

// Loopback is implicitly bypassed even without NO_PROXY, so use a local interface
// address to distinguish explicit exclusion from the HTTP library's built-in rule.
func localProxyTarget(t *testing.T) string {
	t.Helper()
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range addresses {
		ip, ok := address.(*net.IPNet)
		if ok && ip.IP.To4() != nil && ip.IP.IsGlobalUnicast() && !ip.IP.IsLoopback() {
			return ip.IP.String()
		}
	}
	t.Fatal("gRPC NO_PROXY test requires a local non-loopback IPv4 interface")
	return ""
}

func TestTransportGRPCHTTPSProxy(t *testing.T) {
	for _, bypass := range []bool{false, true} {
		name := "connect"
		if bypass {
			name = "no_proxy"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, "native")
			server, ca := transportTLS(f)
			host := localProxyTarget(t)
			endpoint := grpcTransport(f, server.TLS.Certificates[0], host, false)
			var connects atomic.Int64
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodConnect || r.Host != endpoint {
					t.Error("gRPC proxy received traffic other than the native CONNECT target")
					http.Error(w, "unexpected proxy target", http.StatusBadRequest)
					return
				}
				connects.Add(1)
				upstream, err := net.DialTimeout("tcp", endpoint, time.Second)
				if err != nil {
					t.Errorf("connect local upstream: %v", err)
					http.Error(w, "local upstream unavailable", http.StatusBadGateway)
					return
				}
				defer upstream.Close()
				hijacker, ok := w.(http.Hijacker)
				if !ok {
					t.Error("fixture proxy does not support CONNECT hijacking")
					http.Error(w, "CONNECT unavailable", http.StatusInternalServerError)
					return
				}
				client, buffered, err := hijacker.Hijack()
				if err != nil {
					t.Errorf("hijack CONNECT: %v", err)
					return
				}
				defer client.Close()
				_ = client.SetDeadline(time.Now().Add(15 * time.Second))
				_ = upstream.SetDeadline(time.Now().Add(15 * time.Second))
				if _, err := fmt.Fprint(client, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
					t.Errorf("acknowledge CONNECT: %v", err)
					return
				}
				done := make(chan struct{})
				go func() {
					_, _ = io.Copy(upstream, buffered)
					_ = upstream.Close()
					close(done)
				}()
				_, _ = io.Copy(client, upstream)
				_ = client.Close()
				<-done
			}))
			t.Cleanup(proxy.Close)
			noProxy := "127.0.0.1,localhost,.svc"
			if bypass {
				noProxy += "," + host
			}
			f.childEnv = []string{"HTTPS_PROXY=" + proxy.URL, "NO_PROXY=" + noProxy}
			p := f.start(func(config map[string]any) {
				useNativeGRPC(config, endpoint, ca)
			}, true)
			assertTransportDelivery(f, p, "native")
			if got := connects.Load(); (got == 0) != bypass {
				t.Fatalf("native CONNECT requests=%d, want bypass=%v", got, bypass)
			}
		})
	}
}
