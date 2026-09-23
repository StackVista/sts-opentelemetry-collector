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
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
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
	mode    string
	status  int
	records []wireRecord
}

type responsePlan struct {
	status        int
	failures      int
	partial       bool
	lostResponses int
}

type featureReply struct {
	mode   string
	status int
	body   string
}

type backend struct {
	t                  *testing.T
	server             *httptest.Server
	mu                 sync.Mutex
	mode               string
	featureStatus      int
	featureBody        string
	featureCalls       int
	featureReplies     chan featureReply
	plans              map[string]responsePlan
	requests           []request
	responseDelay      time.Duration
	active             int
	maxActive          int
	promtailDescriptor protoreflect.MessageDescriptor
}

func newBackend(t *testing.T, mode string) *backend {
	t.Helper()
	b := &backend{
		t: t, mode: mode, featureStatus: http.StatusOK,
		plans: map[string]responsePlan{
			"promtail": {status: http.StatusOK},
			"native":   {status: http.StatusOK},
		},
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

func (b *backend) setFeature(mode string, status int, body string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mode, b.featureStatus, b.featureBody = mode, status, body
}

func (b *backend) setPlan(mode string, plan responsePlan) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.plans[mode] = plan
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

func (b *backend) attempts(mode string) int {
	count := 0
	for _, req := range b.snapshot() {
		if req.mode == mode {
			count++
		}
	}
	return count
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

func (b *backend) record(mode string, records []wireRecord) (responsePlan, int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	plan := b.plans[mode]
	status := plan.status
	if plan.failures > 0 {
		status = http.StatusServiceUnavailable
		plan.failures--
	}
	loseResponse := plan.lostResponses > 0
	if loseResponse {
		plan.lostResponses--
	}
	b.plans[mode] = plan
	b.requests = append(b.requests, request{mode: mode, status: status, records: records})
	return plan, status, loseResponse
}

func (b *backend) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		b.t.Error("fixture received unexpected URL query")
	}
	if r.URL.Path == "/stsAgent/features" {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "ApiKey "+syntheticKey {
			b.t.Error("incorrect feature method or synthetic authorization")
		}
		b.mu.Lock()
		b.featureCalls++
		mode, status, body := b.mode, b.featureStatus, b.featureBody
		replies := b.featureReplies
		b.mu.Unlock()
		if replies != nil {
			select {
			case reply := <-replies:
				mode, status, body = reply.mode, reply.status, reply.body
			case <-r.Context().Done():
				return
			}
		}
		if body == "" {
			body = fmt.Sprintf(`{"otel-logs":%t}`, mode == "native")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
		return
	}
	var mode string
	switch r.URL.Path {
	case "/stsAgent/logs/k8s":
		mode = "promtail"
		if r.Header.Get("sts-api-key") != syntheticKey {
			b.t.Error("incorrect synthetic promtail authorization")
		}
	case "/otel/v1/logs":
		mode = "native"
		if r.Header.Get("Authorization") != "SUSEObservability "+syntheticKey {
			b.t.Error("incorrect synthetic native authorization")
		}
	default:
		b.t.Errorf("unexpected fixture endpoint %q", r.URL.Path)
		http.NotFound(w, r)
		return
	}
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
	var records []wireRecord
	if mode == "promtail" {
		records, err = decodePromtail(b.promtailDescriptor, payload)
	} else {
		records, err = decodeNative(payload)
	}
	if err != nil {
		b.t.Errorf("decode %s export: %v", mode, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	plan, status, loseResponse := b.record(mode, records)
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
	if mode == "native" {
		w.Header().Set("Content-Type", "application/x-protobuf")
	}
	w.WriteHeader(status)
	if mode == "native" && status == http.StatusOK {
		resp := plogotlp.NewExportResponse()
		if plan.partial {
			resp.PartialSuccess().SetRejectedLogRecords(1)
			resp.PartialSuccess().SetErrorMessage("synthetic record rejection")
		}
		data, err := resp.MarshalProto()
		if err != nil {
			b.t.Errorf("encode partial success: %v", err)
			return
		}
		_, _ = w.Write(data)
	}
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

func decodeNative(payload []byte) ([]wireRecord, error) {
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalProto(payload); err != nil {
		return nil, err
	}
	var records []wireRecord
	resources := req.Logs().ResourceLogs()
	for i := 0; i < resources.Len(); i++ {
		resource := resources.At(i)
		attr := func(key string) string {
			value, _ := resource.Resource().Attributes().Get(key)
			return value.Str()
		}
		scopes := resource.ScopeLogs()
		for j := 0; j < scopes.Len(); j++ {
			logs := scopes.At(j).LogRecords()
			for k := 0; k < logs.Len(); k++ {
				record := logs.At(k)
				stream, _ := record.Attributes().Get("log.iostream")
				records = append(records, wireRecord{
					body: record.Body().Str(), timestamp: uint64(record.Timestamp()),
					cluster: attr("k8s.cluster.name"), podUID: attr("k8s.pod.uid"), podName: attr("k8s.pod.name"),
					container: attr("k8s.container.name"), stream: stream.Str(),
				})
			}
		}
	}
	return records, nil
}

func (b *backend) bodies(mode string, acceptedOnly bool) map[string]int {
	counts := make(map[string]int)
	for _, req := range b.snapshot() {
		if req.mode != mode || (acceptedOnly && req.status/100 != 2) {
			continue
		}
		for _, record := range req.records {
			counts[record.body]++
		}
	}
	return counts
}

func (b *backend) waitBodies(mode string, bodies []string, acceptedOnly bool) {
	b.t.Helper()
	eventually(b.t, 10*time.Second, "wire records on "+mode, func() bool {
		counts := b.bodies(mode, acceptedOnly)
		for _, body := range bodies {
			if counts[body] == 0 {
				return false
			}
		}
		return true
	})
}

func (b *backend) assertOnly(mode string) {
	b.t.Helper()
	for _, req := range b.snapshot() {
		if req.mode != mode {
			b.t.Fatalf("fixed %s route sent a request to %s", mode, req.mode)
		}
	}
}

func (b *backend) assertRecords(mode string, expected []string, acceptedOnly bool) {
	b.t.Helper()
	counts := b.bodies(mode, acceptedOnly)
	if len(counts) != len(expected) {
		b.t.Fatalf("%s saw %d distinct records; want %d", mode, len(counts), len(expected))
	}
	for _, body := range expected {
		if counts[body] != 1 {
			b.t.Errorf("%s record %q appeared %d times; want once", mode, strings.TrimSpace(body), counts[body])
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
				b.t.Errorf("incorrect identity, timestamp or stream on %s: %+v", req.mode, record)
			}
		}
	}
}
