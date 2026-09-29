//nolint:goconst // Keep wire values and route names visible in the synthetic endpoint.
package logsagent_test

import (
	"compress/gzip"
	"context"
	_ "embed"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

//go:embed testdata/promtail.descriptor.textproto
var promtailDescriptor string

type wireRecord struct {
	body      string
	timestamp uint64
	cluster   string
	podUID    string
	podName   string
	container string
	stream    string
}

type request struct {
	status     int
	records    []wireRecord
	receivedAt time.Time
}

type responsePlan struct {
	status        int
	failures      int
	lostResponses int
}

type backend struct {
	t                  *testing.T
	server             *httptest.Server
	mu                 sync.Mutex
	featureCalls       int
	plan               responsePlan
	requests           []request
	responseDelay      time.Duration
	active             int
	maxActive          int
	promtailDescriptor protoreflect.MessageDescriptor
	beforeResponse     func(context.Context, request)
}

func newBackend(t *testing.T) *backend {
	t.Helper()
	b := &backend{
		t:                  t,
		plan:               responsePlan{status: http.StatusOK},
		promtailDescriptor: receiverDescriptor(t),
	}
	b.server = httptest.NewServer(http.HandlerFunc(b.serveHTTP))
	t.Cleanup(b.server.Close)
	return b
}

func receiverDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	var descriptor descriptorpb.FileDescriptorProto
	if err := prototext.Unmarshal([]byte(promtailDescriptor), &descriptor); err != nil {
		t.Fatal(err)
	}
	file, err := protodesc.NewFile(&descriptor, nil)
	if err != nil {
		t.Fatal(err)
	}
	return file.Messages().ByName("PushRequest")
}

func (b *backend) setPlan(plan responsePlan) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.plan = plan
}

func (b *backend) setBeforeResponse(hook func(context.Context, request)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.beforeResponse = hook
}

func (b *backend) snapshot() []request {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]request(nil), b.requests...)
}

func (b *backend) polls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.featureCalls
}

func (b *backend) attempts() int {
	return len(b.snapshot())
}

func (b *backend) enter(ctx context.Context) func() {
	b.mu.Lock()
	b.active++
	b.maxActive = max(b.maxActive, b.active)
	delay := b.responseDelay
	b.mu.Unlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
		timer.Stop()
	}
	return func() {
		b.mu.Lock()
		b.active--
		b.mu.Unlock()
	}
}

func (b *backend) record(records []wireRecord, receivedAt time.Time) (request, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	plan := b.plan
	status := plan.status
	if plan.failures > 0 {
		status = http.StatusServiceUnavailable
		plan.failures--
	}
	loseResponse := plan.lostResponses > 0
	if loseResponse {
		plan.lostResponses--
	}
	b.plan = plan
	req := request{status: status, records: records, receivedAt: receivedAt}
	b.requests = append(b.requests, req)
	return req, loseResponse
}

func (b *backend) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		b.t.Error("fixture received unexpected URL query")
	}
	if r.URL.Path == "/stsAgent/features" {
		b.mu.Lock()
		b.featureCalls++
		b.mu.Unlock()
		b.t.Error("fixed agent requested features")
		http.NotFound(w, r)
		return
	}
	switch r.URL.Path {
	case "/stsAgent/logs/k8s":
		if r.Header.Get("sts-api-key") != syntheticKey {
			b.t.Error("incorrect synthetic promtail authorization")
		}
	case "/otel/v1/logs":
		b.t.Error("fixed agent delivered native logs")
		http.NotFound(w, r)
		return
	default:
		b.t.Errorf("unexpected fixture endpoint %q", r.URL.Path)
		http.NotFound(w, r)
		return
	}
	receivedAt := time.Now()
	defer b.enter(r.Context())()
	if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/x-protobuf" {
		b.t.Error("export did not use POST application/x-protobuf")
	}
	var reader io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(reader)
		if err != nil {
			b.t.Errorf("decode gzip: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer zr.Close()
		reader = zr
	}
	payload, err := io.ReadAll(io.LimitReader(reader, 4<<20))
	if err != nil {
		b.t.Errorf("read export: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	records, err := decodePromtail(b.promtailDescriptor, payload)
	if err != nil {
		b.t.Errorf("decode promtail export: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	req, loseResponse := b.record(records, receivedAt)
	b.mu.Lock()
	beforeResponse := b.beforeResponse
	b.mu.Unlock()
	if beforeResponse != nil {
		beforeResponse(r.Context(), req)
	}
	if loseResponse {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			b.t.Error("fixture server does not support dropping responses")
			return
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			b.t.Errorf("drop stored response: %v", err)
			return
		}
		_ = conn.Close()
		return
	}
	w.WriteHeader(req.status)
}

func field(message protoreflect.Message, name protoreflect.Name) protoreflect.Value {
	return message.Get(message.Descriptor().Fields().ByName(name))
}

var labelPattern = regexp.MustCompile(`([a-z_]+)="([^"]*)"`)

func decodePromtail(descriptor protoreflect.MessageDescriptor, payload []byte) ([]wireRecord, error) {
	data, err := snappy.Decode(nil, payload)
	if err != nil {
		return nil, err
	}
	message := dynamicpb.NewMessage(descriptor)
	if err := proto.Unmarshal(data, message); err != nil {
		return nil, err
	}
	var records []wireRecord
	streams := field(message, "streams").List()
	for i := 0; i < streams.Len(); i++ {
		stream := streams.Get(i).Message()
		labels := make(map[string]string)
		for _, match := range labelPattern.FindAllStringSubmatch(field(stream, "labels").String(), -1) {
			labels[match[1]] = match[2]
		}
		entries := field(stream, "entries").List()
		for j := 0; j < entries.Len(); j++ {
			entry := entries.Get(j).Message()
			ts := field(entry, "timestamp").Message()
			seconds, nanos := field(ts, "seconds").Int(), field(ts, "nanos").Int()
			if seconds < 0 || nanos < 0 || nanos >= int64(time.Second) {
				return nil, fmt.Errorf("invalid promtail timestamp: seconds=%d nanos=%d", seconds, nanos)
			}
			if uint64(seconds) > (math.MaxUint64-uint64(nanos))/uint64(time.Second) {
				return nil, fmt.Errorf("promtail timestamp exceeds uint64 nanoseconds")
			}
			records = append(records, wireRecord{
				body:      field(entry, "line").String(),
				timestamp: uint64(seconds)*uint64(time.Second) + uint64(nanos),
				cluster:   labels["sts_cluster_name"], podUID: labels["pod_uid"], podName: labels["pod_name"],
				container: labels["container_name"], stream: labels["stream"],
			})
		}
	}
	return records, nil
}

func (b *backend) bodies(acceptedOnly bool) map[string]int {
	counts := make(map[string]int)
	for _, req := range b.snapshot() {
		if acceptedOnly && req.status/100 != 2 {
			continue
		}
		for _, record := range req.records {
			counts[record.body]++
		}
	}
	return counts
}

func (b *backend) waitBodies(bodies []string, acceptedOnly bool) {
	b.t.Helper()
	eventually(b.t, 10*time.Second, "wire records", func() bool {
		counts := b.bodies(acceptedOnly)
		for _, body := range bodies {
			if counts[body] == 0 {
				return false
			}
		}
		return true
	})
}

func (b *backend) assertOnly() {
	b.t.Helper()
	if b.polls() != 0 {
		b.t.Fatal("fixed agent requested features")
	}
}

func (b *backend) assertRecords(expected []string, acceptedOnly bool) {
	b.t.Helper()
	counts := b.bodies(acceptedOnly)
	if len(counts) != len(expected) {
		b.t.Fatalf("promtail saw %d distinct records; want %d", len(counts), len(expected))
	}
	for _, body := range expected {
		if counts[body] != 1 {
			b.t.Errorf("promtail record %q appeared %d times; want once", strings.TrimSpace(body), counts[body])
		}
	}
}

func (b *backend) assertIdentity() {
	b.t.Helper()
	for _, req := range b.snapshot() {
		for _, record := range req.records {
			if record.cluster != "fixture-cluster" || record.podUID != "12345678-1234-1234-1234-123456789abc" ||
				record.podName != "fixture-0" || record.container != "app" || record.stream != "stdout" ||
				record.timestamp != uint64(time.Date(2026, 9, 10, 12, 0, 0, 123456789, time.UTC).UnixNano()) {
				b.t.Errorf("incorrect identity, timestamp or stream: %+v", record)
			}
		}
	}
}
