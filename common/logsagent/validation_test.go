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
  stslogsagent/logs:
    health_endpoint: 127.0.0.1:13133
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
exporters:
  stsk8slogs/promtail:
    delivery:
      controller_extension: stslogsagent/logs
      max_concurrent_calls: 8
      max_record_bytes: 262144
      max_request_bytes: 1048576
      export_lifetime: 55s
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
service:
  extensions: [file_storage/logs, stslogsagent/logs]
  pipelines:
    logs/input:
      receivers: [filelog/pods]
      processors: [memory_limiter, transform/static_pod, k8sattributes, transform/cluster]
      exporters: [stsk8slogs/promtail]
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
		ExtensionID:        "stslogsagent/logs",
		PromtailExporterID: "stsk8slogs/promtail",
		ExportLifetime:     55 * time.Second,
		RetryBound:         35 * time.Second,
	}
	tests := []struct {
		name    string
		fixture string
		changes map[string]any
		want    logsagent.PipelineConfig
	}{
		{name: "spec graph", want: base},
		{
			name:    "canonical component IDs",
			fixture: strings.NewReplacer("filelog/", "file_log/", "k8sattributes", "k8s_attributes").Replace(pipelineFixture),
			want:    base,
		},
		{
			name: "configurable component and pipeline IDs",
			fixture: strings.NewReplacer(
				"stslogsagent/logs", "stslogsagent/custom",

				"logs/input", "logs/source",
				"filelog/pods", "filelog/custom",
				"file_storage/logs", "file_storage/custom",
				"stsk8slogs/promtail", "stsk8slogs/custom",
			).Replace(pipelineFixture),
			want: logsagent.PipelineConfig{
				ExtensionID: "stslogsagent/custom", PromtailExporterID: "stsk8slogs/custom",
				RetryBound: base.RetryBound, ExportLifetime: base.ExportLifetime,
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
			name: "slower Promtail exporter determines bound",
			changes: map[string]any{
				"exporters::stsk8slogs/promtail::retry_on_failure::max_elapsed_time": "31s",
				"exporters::stsk8slogs/promtail::delivery::export_lifetime":          "92s",
			},
			want: logsagent.PipelineConfig{
				ExtensionID: base.ExtensionID, PromtailExporterID: base.PromtailExporterID,
				RetryBound: 36 * time.Second, ExportLifetime: 92 * time.Second,
			},
		},
		{
			name: "explicit retry tuning",
			changes: map[string]any{
				"exporters::stsk8slogs/promtail::retry_on_failure::randomization_factor": 0,
				"exporters::stsk8slogs/promtail::retry_on_failure::multiplier":           1.0,
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
	for _, exporter := range []string{"stsk8slogs/promtail"} {
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
		{"connector graph", "connectors", map[string]any{"forward/logs": nil}, "must not configure connectors"},
		{"malformed connectors", "connectors", "invalid", "must not configure connectors"},
		{"wrong signal", "service::pipelines", map[string]any{"traces/input": nil}, "logs input pipeline"},
		{"wrong controller", "exporters::stsk8slogs/promtail::delivery::controller_extension", "health_check", "stslogsagent"},
		{"missing controller", "exporters::stsk8slogs/promtail::delivery::controller_extension", "stslogsagent/absent", "enabled"},
		{"disabled controller", "service::extensions", []any{"file_storage/logs"}, "enabled"},
		{"disabled storage", "service::extensions", []any{"stslogsagent/logs"}, "enabled file_storage"},
		{"duplicate extension", "service::extensions", []string{"file_storage/logs", "stslogsagent/logs", "file_storage/logs"}, "distinct"},
		{"extra logs bypass", "service::pipelines::logs/bypass", map[string]any{
			"receivers": []string{"filelog/pods"}, "exporters": []string{"stsk8slogs/promtail"},
		}, "exactly one"},
		{"uninspected signal graph", "service::pipelines::metrics/extra", nil, "exactly one"},
		{"input fanout", "service::pipelines::logs/input::exporters", []string{"debug/extra", "stsk8slogs/promtail"}, "only to"},
		{"input bypass", "service::pipelines::logs/input::exporters", []string{"debug"}, "only to"},
		{"second file reader", "service::pipelines::logs/input::receivers", []string{"filelog/pods", "filelog/other"}, "exactly one filelog"},
		{"wrong input", "service::pipelines::logs/input::receivers", []string{"otlp"}, "exactly one filelog"},
		{"receiver scalar", "service::pipelines::logs/input::receivers", "filelog/pods", "exactly one filelog"},
		{"missing exporter", "service::pipelines::logs/input::exporters", []string{"stsk8slogs/absent"}, "configured mapping"},
		{"input batch", "service::pipelines::logs/input::processors", []string{"batch/logs"}, "asynchronous processor"},
		{"unknown processor", "service::pipelines::logs/input::processors", []string{"custom/buffer"}, "asynchronous processor"},
		{"missing processor", "service::pipelines::logs/input::processors", []string{"transform/missing"}, "configured mapping"},
		{"duplicate processor", "service::pipelines::logs/input::processors", []string{"memory_limiter", "memory_limiter"}, "distinct"},
		{"nonstring processor", "service::pipelines::logs/input::processors", []any{12}, "list of component IDs"},
		{"receiver retries", "receivers::filelog/pods::retry_on_failure::enabled", true, "explicitly false"},
		{"receiver default retries", "receivers::filelog/pods::retry_on_failure", nil, "explicitly false"},
		{"storage recreation", "extensions::file_storage/logs::recreate", true, "explicitly false"},
		{"storage recreation scalar", "extensions::file_storage/logs::recreate", "false", "explicitly false"},
		{"wrong checkpoint storage", "receivers::filelog/pods::storage", "memory_storage", "enabled file_storage"},
		{"file concurrency", "receivers::filelog/pods::max_concurrent_files", 7, "max_concurrent_files + 2"},
		{"concurrency overflow", "receivers::filelog/pods::max_concurrent_files", int64(math.MaxInt64), "max_concurrent_files + 2"},
		{"zero concurrency", "receivers::filelog/pods::max_concurrent_files", 0, "positive integer"},
		{"small admission", "exporters::stsk8slogs/promtail::delivery::max_concurrent_calls", 5, "max_concurrent_files + 2"},
		{"zero record limit", "exporters::stsk8slogs/promtail::delivery::max_record_bytes", 0, "positive integer"},
		{"small request limit", "exporters::stsk8slogs/promtail::delivery::max_request_bytes", 10, "must not exceed"},
		{"short lifetime", "exporters::stsk8slogs/promtail::delivery::export_lifetime", "54.999999999s", "plus 20s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := fixtureMap(t, pipelineFixture)
			fixtureSet(t, values, tt.path, tt.value)
			assertPipelineRejected(t, values, tt.want)
		})
	}
	for _, path := range []string{
		"exporters::stsk8slogs/promtail::delivery", "service", "service::pipelines::logs/input", "receivers::filelog/pods",
		"extensions::stslogsagent/logs", "extensions::file_storage/logs",
		"extensions::file_storage/logs::recreate", "receivers::filelog/pods::storage",
		"receivers::filelog/pods::retry_on_failure", "receivers::filelog/pods::max_concurrent_files",
		"exporters::stsk8slogs/promtail::delivery::max_concurrent_calls",
		"exporters::stsk8slogs/promtail::delivery::max_record_bytes", "exporters::stsk8slogs/promtail::delivery::max_request_bytes",
		"exporters::stsk8slogs/promtail::delivery::export_lifetime",
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
		"exporters::stsk8slogs/promtail::delivery::export_lifetime",
		"exporters::stsk8slogs/promtail::timeout",
		"exporters::stsk8slogs/promtail::retry_on_failure::initial_interval",
		"exporters::stsk8slogs/promtail::retry_on_failure::max_interval",
		"exporters::stsk8slogs/promtail::retry_on_failure::max_elapsed_time",
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
			fixtureSet(t, values, "exporters::stsk8slogs/promtail::delivery::export_lifetime", "9223372036854775807ns")
			fixtureSet(t, values, "exporters::stsk8slogs/promtail::timeout", tt.timeout)
			fixtureSet(t, values, "exporters::stsk8slogs/promtail::retry_on_failure::max_elapsed_time", tt.elapsed)
			assertPipelineRejected(t, values, tt.want)
		})
	}
}

func TestValidatePipelineConfigErrorsDoNotEchoValues(t *testing.T) {
	const marker = "untrusted-configuration-value"
	for _, path := range []string{
		"exporters::stsk8slogs/promtail::delivery::export_lifetime",
		"exporters::stsk8slogs/promtail::delivery::controller_extension",
		"exporters::stsk8slogs/promtail::sending_queue",
		"exporters::stsk8slogs/promtail::retry_on_failure::multiplier",
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

func TestEffectiveConfigOmittedRecreate(t *testing.T) {
	values := fixtureMap(t, pipelineFixture)
	fixtureDelete(t, values, "extensions::file_storage/logs::recreate")
	fixtureSet(t, values, "exporters::stsk8slogs/promtail::sending_queue", nil)
	fixtureSet(t, values, "exporters::stsk8slogs/promtail::sending_queue", nil)
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
		{"exporters::stsk8slogs/promtail::sending_queue::num_consumers", 1},
		{"exporters::stsk8slogs/promtail::sending_queue", map[string]any{}},
		{"exporters::stsk8slogs/promtail::sending_queue", map[string]any{"enabled": true}},
		{"exporters::stsk8slogs/promtail::sending_queue", map[string]any{}},
		{"exporters::stsk8slogs/promtail::sending_queue::sizer", map[string]any{}},
		{"exporters::stsk8slogs/promtail::sending_queue::sizer", "bytes"},
		{"exporters::stsk8slogs/promtail::sending_queue::sizer", "items"},
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
