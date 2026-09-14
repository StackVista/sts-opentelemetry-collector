package stsk8slogsexporter //nolint:testpackage // Exercises the internal encoder and rejection counts.

import (
	"bytes"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/golang/snappy"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const testTimestamp = pcommon.Timestamp(1750000000123456789)

func testEncoder(t *testing.T) *encoder {
	t.Helper()
	e, err := newEncoder("production-eu")
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func testLogs() plog.Logs {
	logs := plog.NewLogs()
	resource := logs.ResourceLogs().AppendEmpty()
	attrs := resource.Resource().Attributes()
	attrs.PutStr("k8s.cluster.name", "production-eu")
	attrs.PutStr("k8s.pod.uid", "12345678-1234-1234-1234-123456789abc")
	attrs.PutStr("k8s.pod.name", "payments-0")
	attrs.PutStr("k8s.container.name", "app")
	record := resource.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.SetTimestamp(testTimestamp)
	record.Body().SetStr("application text")
	record.Attributes().PutStr("log.iostream", "stdout")
	return logs
}

func firstRecord(logs plog.Logs) plog.LogRecord {
	return logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
}

func receiverMessage(t *testing.T) *dynamicpb.Message {
	t.Helper()
	data, err := os.ReadFile("testdata/promtail.descriptor.textproto")
	if err != nil {
		t.Fatal(err)
	}
	var descriptor descriptorpb.FileDescriptorProto
	if err := prototext.Unmarshal(data, &descriptor); err != nil {
		t.Fatal(err)
	}
	file, err := protodesc.NewFile(&descriptor, nil)
	if err != nil {
		t.Fatal(err)
	}
	return dynamicpb.NewMessage(file.Messages().ByName("PushRequest"))
}

func decodeRequest(t *testing.T, payload []byte) *dynamicpb.Message {
	t.Helper()
	raw, err := snappy.Decode(nil, payload)
	if err != nil {
		t.Fatalf("decode Snappy block: %v", err)
	}
	message := receiverMessage(t)
	if err := proto.Unmarshal(raw, message); err != nil {
		t.Fatalf("decode Receiver protobuf: %v", err)
	}
	return message
}

func field(message protoreflect.Message, name protoreflect.Name) protoreflect.Value {
	return message.Get(message.Descriptor().Fields().ByName(name))
}

func TestClusterNameValidation(t *testing.T) {
	for _, name := range []string{"production-eu", "cluster with spaces", "cluster's-name", "集群", "cluster{a=b},other"} {
		if _, err := newEncoder(name); err != nil {
			t.Errorf("rejected supported cluster name: %v", err)
		}
	}
	for _, name := range []string{"", " \t", `cluster"quoted`, `cluster\path`, "cluster\nnewline", "cluster\rreturn", "cluster\ttab",
		"cluster\x00nul", "cluster\x7fdel", "cluster\u0085next-line", "cluster\xffinvalid"} {
		t.Run(strings.ReplaceAll(name, "\x00", "NUL"), func(t *testing.T) {
			_, err := newEncoder(name)
			if err == nil {
				t.Fatal("accepted unsupported cluster name")
			}
			if strings.Contains(err.Error(), "cluster_name=") || (name != "" && strings.Contains(err.Error(), name)) {
				t.Fatal("validation exposed the configured value")
			}
		})
	}
}

func TestReceiverWireFixture(t *testing.T) {
	e := testEncoder(t)
	logs := testLogs()
	firstRecord(logs).Body().SetStr("application text \"quoted\"\\path\nsecond line — 日本語")
	records := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	for i, text := range []string{"", "error", "without stream"} {
		record := records.AppendEmpty()
		record.SetTimestamp(testTimestamp + pcommon.Timestamp(i+1))
		record.Body().SetStr(text)
		if i < 2 {
			record.Attributes().PutStr("log.iostream", []string{"stdout", "stderr"}[i])
		}
	}
	before, err := (&plog.ProtoMarshaler{}).MarshalLogs(logs)
	if err != nil {
		t.Fatal(err)
	}
	result := e.encode(logs)
	if result.records != 4 || len(result.invalid) != 0 {
		t.Fatalf("unexpected counts: %+v", result)
	}
	actual := decodeRequest(t, result.payload)
	expected := dynamicpb.NewMessage(actual.Descriptor())
	fixture, err := os.ReadFile("testdata/request.textproto")
	if err != nil {
		t.Fatal(err)
	}
	if err := prototext.Unmarshal(fixture, expected); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(actual, expected) {
		t.Fatalf("Receiver payload differs:\n%s", prototext.Format(actual))
	}
	if !bytes.Equal(result.payload, e.encode(logs).payload) {
		t.Fatal("encoding is not deterministic")
	}
	after, err := (&plog.ProtoMarshaler{}).MarshalLogs(logs)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("encoder mutated its input")
	}
}

func TestInvalidSiblings(t *testing.T) {
	tests := []struct {
		name   string
		reason invalidReason
		mutate func(plog.Logs)
	}{
		{"cluster_mismatch", invalidCluster, func(l plog.Logs) { l.ResourceLogs().At(0).Resource().Attributes().PutStr("k8s.cluster.name", "other") }},
		{"cluster_type", invalidCluster, func(l plog.Logs) { l.ResourceLogs().At(0).Resource().Attributes().PutInt("k8s.cluster.name", 1) }},
		{"missing_uid", invalidIdentity, func(l plog.Logs) { l.ResourceLogs().At(0).Resource().Attributes().Remove("k8s.pod.uid") }},
		{"uuid_compact", invalidIdentity, func(l plog.Logs) {
			l.ResourceLogs().At(0).Resource().Attributes().PutStr("k8s.pod.uid", "12345678123412341234123456789abc")
		}},
		{"uuid_shape", invalidIdentity, func(l plog.Logs) { l.ResourceLogs().At(0).Resource().Attributes().PutStr("k8s.pod.uid", "not-a-uuid") }},
		{"empty_pod", invalidIdentity, func(l plog.Logs) { l.ResourceLogs().At(0).Resource().Attributes().PutStr("k8s.pod.name", "") }},
		{"pod_type", invalidIdentity, func(l plog.Logs) { l.ResourceLogs().At(0).Resource().Attributes().PutBool("k8s.pod.name", true) }},
		{"pod_length", invalidIdentity, func(l plog.Logs) {
			l.ResourceLogs().At(0).Resource().Attributes().PutStr("k8s.pod.name", strings.Repeat("a", 254))
		}},
		{"container_length", invalidIdentity, func(l plog.Logs) {
			l.ResourceLogs().At(0).Resource().Attributes().PutStr("k8s.container.name", strings.Repeat("a", 64))
		}},
		{"container_escaping", invalidIdentity, func(l plog.Logs) {
			l.ResourceLogs().At(0).Resource().Attributes().PutStr("k8s.container.name", `bad"name`)
		}},
		{"absent_timestamp", invalidTimestamp, func(l plog.Logs) { firstRecord(l).SetTimestamp(0); firstRecord(l).SetObservedTimestamp(testTimestamp) }},
		{"body_type", invalidBody, func(l plog.Logs) { firstRecord(l).Body().SetInt(42) }},
		{"body_utf8", invalidBody, func(l plog.Logs) { firstRecord(l).Body().SetStr("\xff") }},
		{"stream_type", invalidStream, func(l plog.Logs) { firstRecord(l).Attributes().PutInt("log.iostream", 1) }},
		{"stream_value", invalidStream, func(l plog.Logs) { firstRecord(l).Attributes().PutStr("log.iostream", `stdout"`) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bad := testLogs()
			firstRecord(bad).Body().SetStr("invalid sibling")
			tc.mutate(bad)
			logs := testLogs()
			bad.ResourceLogs().At(0).CopyTo(logs.ResourceLogs().AppendEmpty())
			result := testEncoder(t).encode(logs)
			if result.records != 1 || !reflect.DeepEqual(result.invalid, map[invalidReason]int{tc.reason: 1}) {
				t.Fatalf("unexpected counts: records=%d invalid=%v", result.records, result.invalid)
			}
			raw, err := snappy.Decode(nil, result.payload)
			if err != nil || bytes.Contains(raw, []byte("invalid sibling")) {
				t.Fatal("invalid record reached the payload")
			}
			if got := testEncoder(t).encode(bad); got.records != 0 || got.payload != nil {
				t.Fatal("all-invalid input produced a request")
			}
		})
	}
}

func TestGroupingAcrossResourcesAndScopes(t *testing.T) {
	logs := testLogs()
	resource := logs.ResourceLogs().At(0)
	resource.CopyTo(logs.ResourceLogs().AppendEmpty())
	resource.ScopeLogs().At(0).CopyTo(resource.ScopeLogs().AppendEmpty())
	other := logs.ResourceLogs().AppendEmpty()
	resource.CopyTo(other)
	other.Resource().Attributes().PutStr("k8s.container.name", "sidecar")
	result := testEncoder(t).encode(logs)
	message := decodeRequest(t, result.payload)
	streams := field(message, "streams").List()
	if result.records != 5 || streams.Len() != 2 || field(streams.Get(0).Message(), "entries").List().Len() != 3 {
		t.Fatal("grouping lost records or merged different containers")
	}
}

func TestTimestampRange(t *testing.T) {
	for _, tc := range []struct {
		timestamp      pcommon.Timestamp
		seconds, nanos int64
	}{
		{1, 0, 1},
		{1_000_000_000, 1, 0},
		{math.MaxInt64, 9_223_372_036, 854_775_807},
		{math.MaxUint64, 18_446_744_073, 709_551_615},
	} {
		logs := testLogs()
		firstRecord(logs).SetTimestamp(tc.timestamp)
		message := decodeRequest(t, testEncoder(t).encode(logs).payload)
		stream := field(message, "streams").List().Get(0).Message()
		entry := field(stream, "entries").List().Get(0).Message()
		value := field(entry, "timestamp").Message()
		if field(value, "seconds").Int() != tc.seconds || field(value, "nanos").Int() != tc.nanos {
			t.Fatal("timestamp overflow or precision loss")
		}
	}
}

func TestMissingClusterAndEmptyLogs(t *testing.T) {
	logs := testLogs()
	logs.ResourceLogs().At(0).Resource().Attributes().Remove("k8s.cluster.name")
	result := testEncoder(t).encode(logs)
	if result.records != 1 {
		t.Fatal("missing resource cluster should use configured cluster")
	}
	if got := testEncoder(t).encode(plog.NewLogs()); got.records != 0 || got.payload != nil {
		t.Fatal("empty input produced a request")
	}
}
