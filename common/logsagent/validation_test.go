//nolint:goconst // Keep fixture paths and expected errors together in test tables.
package logsagent_test

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/confmap"
	"go.yaml.in/yaml/v3"
)

const pipelineFixture = `
extensions:
  file_storage/logs:
    directory: /tmp/logs-checkpoints
    create_directory: true
    recreate: false
  stslogscapability/logs:
    receiver_url: http://127.0.0.1:18080
    state_directory: /tmp/logs-controller
    health_endpoint: 127.0.0.1:13133
    query_timeout: 20s
    attempt_timeout: 5s
    max_attempts: 3
    initial_backoff: 500ms
    max_backoff: 2s
    poll_interval: 60s
    jitter: 0.2
    stable_observations: 3
    restart_cooldown: 10m
receivers:
  filelog/pods:
    include: [/tmp/pods/*/*/*.log]
    include_file_path: true
    start_at: beginning
    preserve_trailing_whitespaces: true
    storage: file_storage/logs
    max_concurrent_files: 4
    max_log_size: 256KiB
    retry_on_failure:
      enabled: false
    operators:
      - type: container
        id: container
processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 128
    spike_limit_mib: 32
  transform/static_pod:
    error_mode: propagate
    log_statements:
      - context: resource
        statements: ['delete_key(attributes, "k8s.pod.uid")']
  k8sattributes:
    auth_type: serviceAccount
  transform/cluster:
    error_mode: propagate
    log_statements:
      - context: resource
        statements: ['set(attributes["k8s.cluster.name"], "fixture")']
connectors:
  stslogsroute/logs:
    capability_extension: stslogscapability/logs
    legacy_pipeline: logs/promtail
    native_pipeline: logs/otel_native
    max_concurrent_calls: 8
    max_record_bytes: 262144
    max_request_bytes: 1048576
    export_lifetime: 55s
exporters:
  stsk8slogs/promtail:
    endpoint: http://127.0.0.1:18080/stsAgent/logs/k8s
    cluster_name: fixture
    timeout: 5s
    retry_on_failure:
      enabled: true
      initial_interval: 1s
      max_interval: 5s
      max_elapsed_time: 30s
    sending_queue:
      enabled: false
  otlp_http/otel_native:
    endpoint: http://127.0.0.1:14318
    timeout: 5s
    retry_on_failure:
      enabled: true
      initial_interval: 1s
      max_interval: 5s
      max_elapsed_time: 30s
    sending_queue:
      enabled: false
service:
  extensions: [file_storage/logs, stslogscapability/logs]
  pipelines:
    logs/input:
      receivers: [filelog/pods]
      processors: [memory_limiter, transform/static_pod, k8sattributes, transform/cluster]
      exporters: [stslogsroute/logs]
    logs/promtail:
      receivers: [stslogsroute/logs]
      exporters: [stsk8slogs/promtail]
    logs/otel_native:
      receivers: [stslogsroute/logs]
      exporters: [otlp_http/otel_native]
`

func fixtureMap(t *testing.T, fixture string) map[string]any {
	t.Helper()
	var values map[string]any
	if err := yaml.Unmarshal([]byte(fixture), &values); err != nil {
		t.Fatal(err)
	}
	return values
}

func fixtureSet(t *testing.T, values map[string]any, path string, value any) {
	t.Helper()
	keys := strings.Split(path, "::")
	for _, key := range keys[:len(keys)-1] {
		var ok bool
		values, ok = values[key].(map[string]any)
		if !ok {
			t.Fatalf("fixture path %q is not a mapping", key)
		}
	}
	values[keys[len(keys)-1]] = value
}

func fixtureDelete(t *testing.T, values map[string]any, path string) {
	t.Helper()
	keys := strings.Split(path, "::")
	for _, key := range keys[:len(keys)-1] {
		var ok bool
		values, ok = values[key].(map[string]any)
		if !ok {
			t.Fatalf("fixture path %q is not a mapping", key)
		}
	}
	delete(values, keys[len(keys)-1])
}

func TestValidatePipelineConfig(t *testing.T) {
	base := logsagent.PipelineConfig{
		ExtensionID:          "stslogscapability/logs",
		PromtailExporterID:   "stsk8slogs/promtail",
		OTELNativeExporterID: "otlp_http/otel_native",
		ExportLifetime:       55 * time.Second,
		RetryBound:           35 * time.Second,
	}
	tests := []struct {
		name    string
		fixture string
		changes map[string]any
		want    logsagent.PipelineConfig
	}{
		{name: "spec graph", want: base},
		{
			name: "grpc and configurable component and pipeline IDs",
			fixture: strings.NewReplacer(
				"stslogscapability/logs", "stslogscapability/custom",
				"stslogsroute/logs", "stslogsroute/custom",
				"logs/promtail", "logs/old",
				"logs/otel_native", "logs/new",
				"logs/input", "logs/source",
				"filelog/pods", "filelog/custom",
				"file_storage/logs", "file_storage/custom",
				"stsk8slogs/promtail", "stsk8slogs/custom",
				"otlp_http/otel_native", "otlp/custom",
			).Replace(pipelineFixture),
			want: logsagent.PipelineConfig{
				ExtensionID: "stslogscapability/custom", PromtailExporterID: "stsk8slogs/custom",
				OTELNativeExporterID: "otlp/custom", ExportLifetime: base.ExportLifetime,
				RetryBound: base.RetryBound,
			},
		},
		{
			name: "local file graph without enrichment",
			changes: map[string]any{
				"service::pipelines::logs/input::processors": nil,
			},
			want: base,
		},
		{
			name: "slower inactive OTELNative exporter determines bound",
			changes: map[string]any{
				"exporters::otlp_http/otel_native::retry_on_failure::max_elapsed_time": "31s",
				"connectors::stslogsroute/logs::export_lifetime":                       "92s",
			},
			want: logsagent.PipelineConfig{
				ExtensionID: base.ExtensionID, PromtailExporterID: base.PromtailExporterID,
				OTELNativeExporterID: base.OTELNativeExporterID, ExportLifetime: 92 * time.Second,
				RetryBound: 36 * time.Second,
			},
		},
		{
			name: "explicit retry tuning",
			changes: map[string]any{
				"exporters::otlp_http/otel_native::retry_on_failure::randomization_factor": 0,
				"exporters::otlp_http/otel_native::retry_on_failure::multiplier":           1.0,
			},
			want: base,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := tt.fixture
			if fixture == "" {
				fixture = pipelineFixture
			}
			values := fixtureMap(t, fixture)
			for path, value := range tt.changes {
				fixtureSet(t, values, path, value)
			}
			conf := confmap.NewFromStringMap(values)
			before := conf.ToStringMap()
			got, err := logsagent.ValidatePipelineConfig(conf)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
			if !reflect.DeepEqual(before, conf.ToStringMap()) {
				t.Fatal("validation mutated the configuration")
			}
		})
	}
}

func TestValidatePipelineConfigRejectsExporterChanges(t *testing.T) {
	tests := []struct {
		path  string
		value any
		want  string
	}{
		{"timeout", "0s", "timeout"},
		{"timeout", "-5s", "timeout"},
		{"timeout", "6s", "export_lifetime"},
		{"retry_on_failure", nil, "enabled"},
		{"retry_on_failure::enabled", false, "enabled"},
		{"retry_on_failure::initial_interval", "0s", "initial_interval"},
		{"retry_on_failure::initial_interval", "6s", "initial_interval <="},
		{"retry_on_failure::max_interval", "31s", "max_interval <="},
		{"retry_on_failure::max_elapsed_time", "0s", "max_elapsed_time"},
		{"retry_on_failure::max_elapsed_time", "31s", "export_lifetime"},
		{"retry_on_failure::multiplier", math.NaN(), "multiplier"},
		{"retry_on_failure::multiplier", math.Inf(1), "multiplier"},
		{"retry_on_failure::multiplier", 0.9, "multiplier"},
		{"retry_on_failure::multiplier", "1.5", "multiplier"},
		{"retry_on_failure::randomization_factor", math.NaN(), "randomization_factor"},
		{"retry_on_failure::randomization_factor", -0.1, "randomization_factor"},
		{"retry_on_failure::randomization_factor", 1.1, "randomization_factor"},
		{"sending_queue", nil, "enabled"},
		{"sending_queue", false, "configured mapping"},
		{"sending_queue", map[string]any{}, "enabled"},
		{"sending_queue::enabled", true, "enabled"},
		{"sending_queue::enabled", nil, "enabled"},
		{"sending_queue::enabled", "true", "enabled"},
	}
	for _, exporter := range []string{"stsk8slogs/promtail", "otlp_http/otel_native"} {
		for _, tt := range tests {
			t.Run(exporter+"/"+tt.path+"/"+tt.want, func(t *testing.T) {
				values := fixtureMap(t, pipelineFixture)
				fixtureSet(t, values, "exporters::"+exporter+"::"+tt.path, tt.value)
				assertPipelineRejected(t, values, tt.want)
			})
		}
		for _, field := range []string{
			"timeout", "retry_on_failure", "retry_on_failure::enabled",
			"retry_on_failure::initial_interval", "retry_on_failure::max_interval",
			"retry_on_failure::max_elapsed_time", "sending_queue", "sending_queue::enabled",
		} {
			t.Run(exporter+"/missing/"+field, func(t *testing.T) {
				values := fixtureMap(t, pipelineFixture)
				fixtureDelete(t, values, "exporters::"+exporter+"::"+field)
				assertPipelineRejected(t, values, "")
			})
		}
	}
}

func TestValidatePipelineConfigRejectsGraphChanges(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		value any
		want  string
	}{
		{"second route", "connectors::stslogsroute/extra", nil, "exactly one route"},
		{"same destinations", "connectors::stslogsroute/logs::native_pipeline", "logs/promtail", "distinct logs"},
		{"missing destination", "connectors::stslogsroute/logs::native_pipeline", "logs/missing", "one logs input"},
		{"wrong signal", "connectors::stslogsroute/logs::native_pipeline", "traces/otel_native", "distinct logs"},
		{"wrong capability", "connectors::stslogsroute/logs::capability_extension", "health_check", "stslogscapability"},
		{"missing capability", "connectors::stslogsroute/logs::capability_extension", "stslogscapability/absent", "enabled"},
		{"disabled capability", "service::extensions", []any{"file_storage/logs"}, "enabled"},
		{"disabled storage", "service::extensions", []any{"stslogscapability/logs"}, "enabled file_storage"},
		{"duplicate extension", "service::extensions", []string{"file_storage/logs", "stslogscapability/logs", "file_storage/logs"}, "distinct"},
		{"extra logs bypass", "service::pipelines::logs/bypass", map[string]any{
			"receivers": []string{"filelog/pods"}, "exporters": []string{"stsk8slogs/promtail"},
		}, "exactly three"},
		{"uninspected signal graph", "service::pipelines::metrics/extra", nil, "exactly three"},
		{"input fanout", "service::pipelines::logs/input::exporters", []string{"stslogsroute/logs", "stsk8slogs/promtail"}, "only to"},
		{"input bypass", "service::pipelines::logs/input::exporters", []string{"stsk8slogs/promtail"}, "only to"},
		{"second file reader", "service::pipelines::logs/input::receivers", []string{"filelog/pods", "filelog/other"}, "exactly one filelog"},
		{"wrong input", "service::pipelines::logs/input::receivers", []string{"otlp"}, "exactly one filelog"},
		{"receiver scalar", "service::pipelines::logs/input::receivers", "filelog/pods", "exactly one filelog"},
		{"terminal fanout", "service::pipelines::logs/otel_native::exporters", []string{"otlp_http/otel_native", "debug"}, "exactly one exporter"},
		{"terminal direct receiver", "service::pipelines::logs/promtail::receivers", []string{"filelog/pods"}, "only the route"},
		{"wrong legacy", "service::pipelines::logs/promtail::exporters", []string{"debug"}, "stsk8slogs"},
		{"wrong native", "service::pipelines::logs/otel_native::exporters", []string{"stsk8slogs/promtail"}, "OTLP exporter"},
		{"native cycle", "service::pipelines::logs/otel_native::exporters", []string{"stslogsroute/logs"}, "OTLP exporter"},
		{"missing exporter", "service::pipelines::logs/otel_native::exporters", []string{"otlp/absent"}, "configured mapping"},
		{"terminal processor", "service::pipelines::logs/otel_native::processors", []string{"batch"}, "must be empty"},
		{"input batch", "service::pipelines::logs/input::processors", []string{"batch/logs"}, "asynchronous processor"},
		{"unknown processor", "service::pipelines::logs/input::processors", []string{"custom/buffer"}, "asynchronous processor"},
		{"missing processor", "service::pipelines::logs/input::processors", []string{"transform/missing"}, "configured mapping"},
		{"duplicate processor", "service::pipelines::logs/input::processors", []string{"memory_limiter", "memory_limiter"}, "distinct"},
		{"nonstring processor", "service::pipelines::logs/input::processors", []any{12}, "list of component IDs"},
		{"receiver collision", "receivers::stslogsroute/logs", nil, "also be defined as a receiver"},
		{"exporter collision", "exporters::stslogsroute/logs", nil, "also be defined as an exporter"},
		{"receiver retries", "receivers::filelog/pods::retry_on_failure::enabled", true, "explicitly false"},
		{"receiver default retries", "receivers::filelog/pods::retry_on_failure", nil, "explicitly false"},
		{"storage recreation", "extensions::file_storage/logs::recreate", true, "explicitly false"},
		{"storage recreation scalar", "extensions::file_storage/logs::recreate", "false", "explicitly false"},
		{"wrong checkpoint storage", "receivers::filelog/pods::storage", "memory_storage", "enabled file_storage"},
		{"file concurrency", "receivers::filelog/pods::max_concurrent_files", 7, "max_concurrent_files + 2"},
		{"concurrency overflow", "receivers::filelog/pods::max_concurrent_files", int64(math.MaxInt64), "max_concurrent_files + 2"},
		{"zero concurrency", "receivers::filelog/pods::max_concurrent_files", 0, "positive integer"},
		{"small admission", "connectors::stslogsroute/logs::max_concurrent_calls", 5, "max_concurrent_files + 2"},
		{"zero record limit", "connectors::stslogsroute/logs::max_record_bytes", 0, "positive integer"},
		{"small request limit", "connectors::stslogsroute/logs::max_request_bytes", 10, "must not exceed"},
		{"short lifetime", "connectors::stslogsroute/logs::export_lifetime", "54.999999999s", "plus 20s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := fixtureMap(t, pipelineFixture)
			fixtureSet(t, values, tt.path, tt.value)
			assertPipelineRejected(t, values, tt.want)
		})
	}
	for _, path := range []string{
		"connectors", "service", "service::pipelines::logs/otel_native", "receivers::filelog/pods",
		"extensions::stslogscapability/logs", "extensions::file_storage/logs",
		"extensions::file_storage/logs::recreate", "receivers::filelog/pods::storage",
		"receivers::filelog/pods::retry_on_failure", "receivers::filelog/pods::max_concurrent_files",
		"connectors::stslogsroute/logs::max_concurrent_calls",
		"connectors::stslogsroute/logs::max_record_bytes", "connectors::stslogsroute/logs::max_request_bytes",
		"connectors::stslogsroute/logs::export_lifetime",
	} {
		t.Run("missing/"+path, func(t *testing.T) {
			values := fixtureMap(t, pipelineFixture)
			fixtureDelete(t, values, path)
			assertPipelineRejected(t, values, "")
		})
	}
}

func TestValidatePipelineConfigDurationScalars(t *testing.T) {
	for _, path := range []string{
		"connectors::stslogsroute/logs::export_lifetime",
		"exporters::stsk8slogs/promtail::timeout",
		"exporters::otlp_http/otel_native::retry_on_failure::initial_interval",
		"exporters::otlp_http/otel_native::retry_on_failure::max_interval",
		"exporters::otlp_http/otel_native::retry_on_failure::max_elapsed_time",
	} {
		t.Run(path, func(t *testing.T) {
			for _, value := range []any{
				nil, "", "0s", "-1ns", "9223372036854775808ns", "1e100s", "NaN",
				int64(5e9), uint64(math.MaxUint64), 5.0, math.NaN(), math.Inf(1), true,
				map[string]any{}, []any{"5s"}, time.Duration(-1),
			} {
				values := fixtureMap(t, pipelineFixture)
				fixtureSet(t, values, path, value)
				assertPipelineRejected(t, values, "positive duration")
			}
		})
	}
}

func TestValidatePipelineConfigOverflow(t *testing.T) {
	tests := []struct {
		name    string
		timeout string
		elapsed string
		want    string
	}{
		{"addition", "9223372036854775807ns", "30s", "sum overflows"},
		{"overhead", "1ns", "9223372036854775806ns", "plus 20s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := fixtureMap(t, pipelineFixture)
			fixtureSet(t, values, "connectors::stslogsroute/logs::export_lifetime", "9223372036854775807ns")
			fixtureSet(t, values, "exporters::otlp_http/otel_native::timeout", tt.timeout)
			fixtureSet(t, values, "exporters::otlp_http/otel_native::retry_on_failure::max_elapsed_time", tt.elapsed)
			assertPipelineRejected(t, values, tt.want)
		})
	}
}

func TestValidatePipelineConfigErrorsDoNotEchoValues(t *testing.T) {
	const marker = "untrusted-configuration-value"
	for _, path := range []string{
		"connectors::stslogsroute/logs::export_lifetime",
		"connectors::stslogsroute/logs::capability_extension",
		"exporters::stsk8slogs/promtail::sending_queue",
		"exporters::otlp_http/otel_native::retry_on_failure::multiplier",
		"receivers::filelog/pods::storage",
	} {
		values := fixtureMap(t, pipelineFixture)
		fixtureSet(t, values, path, marker)
		message := assertPipelineRejected(t, values, "")
		if strings.Contains(message, marker) {
			t.Fatal("validation error echoed a configuration value")
		}
	}
}

func TestValidatePipelineConfigEmpty(t *testing.T) {
	for _, conf := range []*confmap.Conf{nil, confmap.New()} {
		if result, err := logsagent.ValidatePipelineConfig(conf); err == nil || result != (logsagent.PipelineConfig{}) {
			t.Fatal("empty configuration must fail without returning partial bounds")
		}
	}
}

func TestOTELNativeExporterNamesAndDisabledQueues(t *testing.T) {
	for _, kind := range []string{"otlp_http", "otlphttp", "otlp_grpc", "otlp"} {
		for _, effective := range []bool{false, true} {
			nativeID := kind + "/otel_native"
			values := fixtureMap(t, strings.ReplaceAll(pipelineFixture, "otlp_http/otel_native", nativeID))
			validate := logsagent.ValidatePipelineConfig
			if effective {
				validate = logsagent.ValidateEffectivePipelineConfig
				for _, id := range []string{"stsk8slogs/promtail", nativeID} {
					fixtureSet(t, values, "exporters::"+id+"::sending_queue", nil)
				}
			}
			conf := confmap.NewFromStringMap(values)
			before := snapshotConfig(t, conf)
			got, err := validate(conf)
			if err != nil || got.RetryBound != 35*time.Second {
				t.Fatalf("%s effective=%t: %+v, %v", kind, effective, got, err)
			}
			if before != snapshotConfig(t, conf) {
				t.Fatal("validation mutated configuration")
			}
		}
	}
}

func TestEffectiveConfigOmittedRecreate(t *testing.T) {
	values := fixtureMap(t, pipelineFixture)
	fixtureDelete(t, values, "extensions::file_storage/logs::recreate")
	fixtureSet(t, values, "exporters::stsk8slogs/promtail::sending_queue", nil)
	fixtureSet(t, values, "exporters::otlp_http/otel_native::sending_queue", nil)
	conf := confmap.NewFromStringMap(values)
	before := snapshotConfig(t, conf)
	got, err := logsagent.ValidateEffectivePipelineConfig(conf)
	if err != nil || got.RetryBound != 35*time.Second {
		t.Fatalf("effective bounds: %+v, %v", got, err)
	}
	if before != snapshotConfig(t, conf) {
		t.Fatal("effective validation mutated the configuration")
	}
	if _, err := logsagent.ValidatePipelineConfig(conf); err == nil {
		t.Fatal("authored validation accepted omitted recreate")
	}
}

func TestEffectiveConfigRejectsUnsafeValues(t *testing.T) {
	for _, tc := range []struct {
		path  string
		value any
	}{
		{"extensions::file_storage/logs::recreate", true},
		{"extensions::file_storage/logs::recreate", nil},
		{"extensions::file_storage/logs::recreate", "false"},
		{"exporters::otlp_http/otel_native::sending_queue::num_consumers", 1},
		{"exporters::stsk8slogs/promtail::sending_queue", map[string]any{}},
		{"exporters::otlp_http/otel_native::sending_queue", map[string]any{"enabled": true}},
		{"exporters::otlp_http/otel_native::sending_queue", map[string]any{}},
		{"exporters::otlp_http/otel_native::sending_queue::sizer", map[string]any{}},
		{"exporters::otlp_http/otel_native::sending_queue::sizer", "bytes"},
		{"exporters::otlp_http/otel_native::sending_queue::sizer", "items"},
		{"exporters::stsk8slogs/promtail::sending_queue::batch", map[string]any{}},
		{"receivers::filelog/pods::retry_on_failure::enabled", true},
		{"receivers::filelog/pods::preserve_trailing_whitespaces", false},
	} {
		t.Run(tc.path, func(t *testing.T) {
			values := fixtureMap(t, pipelineFixture)
			fixtureSet(t, values, tc.path, tc.value)
			if _, err := logsagent.ValidateEffectivePipelineConfig(confmap.NewFromStringMap(values)); err == nil {
				t.Fatal("effective validation accepted an unsafe value")
			}
		})
	}
}

func assertPipelineRejected(t *testing.T, values map[string]any, want string) string {
	t.Helper()
	conf := confmap.NewFromStringMap(values)
	before := snapshotConfig(t, conf)
	result, err := logsagent.ValidatePipelineConfig(conf)
	if err == nil {
		t.Fatal("invalid configuration was accepted")
	}
	if result != (logsagent.PipelineConfig{}) {
		t.Fatal("failed validation returned partial bounds")
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not contain %q", err, want)
	}
	if before != snapshotConfig(t, conf) {
		t.Fatal("failed validation mutated the configuration")
	}
	return err.Error()
}

func snapshotConfig(t *testing.T, conf *confmap.Conf) string {
	t.Helper()
	encoded, err := yaml.Marshal(conf.ToStringMap())
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
