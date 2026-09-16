//nolint:goconst // Keep authored keys and expected rejection fields together.
package logsagent_test

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestFixtureBounds(t *testing.T) {
	s := defaultSettings()
	if s.Lifetime < s.minimumLifetime() || s.minimumLifetime() <= 20*time.Second {
		t.Fatalf("lifetime %s does not cover queue/retry bound %s", s.Lifetime, s.minimumLifetime())
	}
	c := renderConfig(t, s)
	bounds, err := logsagent.ValidatePipelineConfig(confmap.NewFromStringMap(c))
	if err != nil {
		t.Fatalf("shared validator rejected authored fixture: %v", err)
	}
	if bounds.ExportLifetime != s.Lifetime || bounds.QueueRetryBound != s.minimumLifetime()-20*time.Second {
		t.Fatalf("shared validator returned unexpected fixture bounds: %+v", bounds)
	}
	route := section(c, "connectors", "stslogsroute/logs")
	files := section(c, "receivers", "filelog/pods")
	if route["max_concurrent_calls"] != s.Concurrency || files["max_concurrent_files"] != s.Files ||
		s.Concurrency < s.Files+2 {
		t.Fatal("fixture lacks admission headroom for recombination")
	}
	if section(files, "retry_on_failure")["enabled"] != false ||
		section(c, "extensions", "file_storage/logs")["recreate"] != false {
		t.Fatal("fixture enabled receiver retry or checkpoint recreation")
	}
	for _, mode := range []string{"legacy", "native"} {
		exporter := "stsk8slogs/legacy"
		if mode == "native" {
			exporter = "otlp_http/native"
		}
		e := section(c, "exporters", exporter)
		q := section(e, "sending_queue")
		if q["queue_size"] != s.Concurrency || q["num_consumers"] != s.Workers || s.Workers > s.Concurrency ||
			q["wait_for_result"] != true || q["block_on_overflow"] != true || q["sizer"] != "requests" {
			t.Fatalf("%s queue does not preserve bounded synchronous export", mode)
		}
		if _, exists := q["batch"]; exists {
			t.Fatal("fixture enabled queue merging")
		}
		if _, exists := q["storage"]; exists {
			t.Fatal("fixture enabled a persistent payload queue")
		}
		if _, exists := section(c, "service", "pipelines", "logs/"+mode)["processors"]; exists {
			t.Fatal("terminal fixture pipeline contains processors")
		}
	}
}

func TestAuthoredFixtureRejectsOmittedBounds(t *testing.T) {
	paths := make([][]string, 0, 29)
	paths = append(paths, [][]string{
		{"connectors", "stslogsroute/logs", "export_lifetime"},
		{"connectors", "stslogsroute/logs", "max_concurrent_calls"},
		{"connectors", "stslogsroute/logs", "max_record_bytes"},
		{"connectors", "stslogsroute/logs", "max_request_bytes"},
		{"receivers", "filelog/pods", "max_concurrent_files"},
		{"receivers", "filelog/pods", "retry_on_failure", "enabled"},
		{"extensions", "file_storage/logs", "recreate"},
	}...)
	for _, exporter := range []string{"stsk8slogs/legacy", "otlp_http/native"} {
		for _, field := range [][]string{
			{"timeout"},
			{"retry_on_failure", "enabled"},
			{"retry_on_failure", "initial_interval"},
			{"retry_on_failure", "max_interval"},
			{"retry_on_failure", "max_elapsed_time"},
			{"sending_queue", "enabled"},
			{"sending_queue", "sizer"},
			{"sending_queue", "queue_size"},
			{"sending_queue", "num_consumers"},
			{"sending_queue", "wait_for_result"},
			{"sending_queue", "block_on_overflow"},
		} {
			paths = append(paths, append([]string{"exporters", exporter}, field...))
		}
	}
	for _, path := range paths {
		t.Run(strings.Join(path, "/"), func(t *testing.T) {
			c := renderConfig(t, defaultSettings())
			key := path[len(path)-1]
			delete(section(c, path[:len(path)-1]...), key)
			_, err := logsagent.ValidatePipelineConfig(confmap.NewFromStringMap(c))
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("omitted %s: expected field-specific rejection, got %v", key, err)
			}
		})
	}
}

func TestAuthoredFixtureRejectsInvalidBounds(t *testing.T) {
	for _, exporter := range []string{"stsk8slogs/legacy", "otlp_http/native"} {
		for _, tc := range []struct {
			name   string
			reason string
			mutate func(map[string]any)
		}{
			{"fewer_workers", "export_lifetime", func(e map[string]any) {
				section(e, "sending_queue")["num_consumers"] = 1
			}},
			{"queue_below_admission", "queue_size", func(e map[string]any) {
				section(e, "sending_queue")["queue_size"] = 7
			}},
			{"workers_above_admission", "num_consumers", func(e map[string]any) {
				section(e, "sending_queue")["num_consumers"] = 9
			}},
			{"longer_retry", "export_lifetime", func(e map[string]any) {
				section(e, "retry_on_failure")["max_elapsed_time"] = "10s"
			}},
			{"unlimited_retry", "max_elapsed_time", func(e map[string]any) {
				section(e, "retry_on_failure")["max_elapsed_time"] = "0s"
			}},
			{"queue_batching", "batch", func(e map[string]any) {
				section(e, "sending_queue")["batch"] = map[string]any{}
			}},
		} {
			t.Run(exporter+"/"+tc.name, func(t *testing.T) {
				c := renderConfig(t, defaultSettings())
				tc.mutate(section(c, "exporters", exporter))
				_, err := logsagent.ValidatePipelineConfig(confmap.NewFromStringMap(c))
				if err == nil || !strings.Contains(err.Error(), tc.reason) {
					t.Fatalf("expected %s rejection, got %v", tc.reason, err)
				}
			})
		}
	}
}

func TestChildEnvironmentIsExplicit(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "synthetic-parent-sentinel")
	t.Setenv("HTTPS_PROXY", "http://synthetic.invalid")
	t.Setenv("STS_API_KEY", "synthetic-parent-sentinel")
	root := t.TempDir()
	actual := childEnvironment(root)
	expected := []string{"HOME=" + root, "TMPDIR=" + root, "PATH=/usr/bin:/bin", "GOMAXPROCS=2", "STS_API_KEY=" + syntheticKey}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatal("child environment differs from its explicit fixture allowlist")
	}
	for _, item := range actual {
		if strings.Contains(item, os.Getenv("AWS_SESSION_TOKEN")) {
			t.Fatal("parent environment leaked into child")
		}
	}
}

func TestWireDecoders(t *testing.T) {
	descriptor := receiverDescriptor(t)
	message := dynamicpb.NewMessage(descriptor)
	err := prototext.Unmarshal([]byte(`streams: {
		labels: "{sts_cluster_name=\"fixture-cluster\",pod_uid=\"12345678-1234-1234-1234-123456789abc\",pod_name=\"fixture-0\",container_name=\"app\",stream=\"stdout\"}"
		entries: {timestamp: {seconds: 42 nanos: 123456789} line: "synthetic text"}
	}`), message)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := decodeLegacy(descriptor, snappy.Encode(nil, payload))
	if err != nil {
		t.Fatal(err)
	}
	nativeRequest := plogotlp.NewExportRequest()
	err = nativeRequest.UnmarshalJSON([]byte(`{"resourceLogs":[{"resource":{"attributes":[
		{"key":"k8s.cluster.name","value":{"stringValue":"fixture-cluster"}},
		{"key":"k8s.pod.uid","value":{"stringValue":"12345678-1234-1234-1234-123456789abc"}},
		{"key":"k8s.pod.name","value":{"stringValue":"fixture-0"}},
		{"key":"k8s.container.name","value":{"stringValue":"app"}}
	]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"42123456789","body":{"stringValue":"synthetic text"},
		"attributes":[{"key":"log.iostream","value":{"stringValue":"stdout"}}]}]}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	payload, err = nativeRequest.MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	native, err := decodeNative(payload)
	if err != nil {
		t.Fatal(err)
	}
	expected := []wireRecord{{
		body: "synthetic text", timestamp: 42123456789,
		cluster: "fixture-cluster", podUID: "12345678-1234-1234-1234-123456789abc",
		podName: "fixture-0", container: "app", stream: "stdout",
	}}
	if !reflect.DeepEqual(legacy, expected) || !reflect.DeepEqual(native, expected) {
		t.Fatalf("wire decoders disagree with fixture: legacy=%+v native=%+v", legacy, native)
	}
}
