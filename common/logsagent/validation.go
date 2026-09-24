package logsagent

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/collector/confmap"
)

type PipelineConfig struct {
	ExtensionID        string
	PromtailExporterID string
	ExportLifetime     time.Duration
	RetryBound         time.Duration
}

// ValidatePipelineConfig requires explicitly authored bounds for configuration fixtures.
// Component-local validation, the emitter feature gate and Pod grace are separate checks.
func ValidatePipelineConfig(conf *confmap.Conf) (PipelineConfig, error) {
	if conf == nil {
		return PipelineConfig{}, errors.New("logs agent configuration is required")
	}
	var err error
	root := configFields{values: conf.ToStringMap(), path: "config", err: &err}
	if raw := root.values["connectors"]; raw != nil {
		if connectors, ok := raw.(map[string]any); !ok || len(connectors) != 0 {
			return PipelineConfig{}, errors.New("direct logs delivery must not configure connectors")
		}
	}
	service := root.object("service")
	pipelines := service.object("pipelines")
	if len(pipelines.values) != 1 {
		return PipelineConfig{}, errors.New("logs agent requires exactly one pipeline")
	}
	var inputID string
	for id := range pipelines.values {
		inputID = id
	}
	if !componentType(inputID, "logs") {
		return PipelineConfig{}, errors.New("logs agent requires a logs input pipeline")
	}
	input := pipelines.namedObject(inputID, "input pipeline")
	outputs := input.strings("exporters", false)
	if len(outputs) != 1 || !componentType(outputs[0], "stsk8slogs") {
		return PipelineConfig{}, errors.New("input pipeline must export only to one stsk8slogs exporter")
	}
	exporters := root.object("exporters")
	promtail := exporters.namedObject(outputs[0], "Promtail-compatible exporter")
	delivery := promtail.object("delivery")
	cfg := PipelineConfig{
		ExtensionID:        delivery.text("controller_extension"),
		PromtailExporterID: outputs[0],
		ExportLifetime:     delivery.duration("export_lifetime"),
	}
	calls := delivery.positiveInt("max_concurrent_calls")
	recordBytes := delivery.positiveInt("max_record_bytes")
	requestBytes := delivery.positiveInt("max_request_bytes")
	if err != nil {
		return PipelineConfig{}, err
	}
	if recordBytes > requestBytes {
		return PipelineConfig{}, errors.New("delivery max_record_bytes must not exceed max_request_bytes")
	}
	if !componentType(cfg.ExtensionID, "stslogsagent") {
		return PipelineConfig{}, errors.New("delivery controller_extension must reference stslogsagent")
	}
	enabledExtensions := service.strings("extensions", false)
	extensions := root.object("extensions")
	extensions.namedObject(cfg.ExtensionID, "controller extension")
	if !slices.Contains(enabledExtensions, cfg.ExtensionID) {
		return PipelineConfig{}, errors.New("controller extension must be enabled in service.extensions")
	}
	inputReceivers := input.strings("receivers", false)
	if len(inputReceivers) != 1 || !componentType(inputReceivers[0], "filelog", "file_log") {
		return PipelineConfig{}, errors.New("input pipeline must receive from exactly one filelog receiver")
	}
	processors := input.strings("processors", true)
	for _, id := range processors {
		// Unknown processors cannot be assumed to preserve synchronous completion.
		if !componentType(id, "memory_limiter", "transform", "k8sattributes", "k8s_attributes") {
			return PipelineConfig{}, errors.New("input pipeline contains an unsupported or asynchronous processor")
		}
		root.object("processors").namedObject(id, "input processor")
	}

	receivers := root.object("receivers")
	filelog := receivers.namedObject(inputReceivers[0], "filelog receiver")
	files := filelog.positiveInt("max_concurrent_files")
	filelog.boolean("preserve_trailing_whitespaces", true)
	filelog.object("retry_on_failure").boolean("enabled", false)
	storageID := filelog.text("storage")
	if !componentType(storageID, "file_storage") || !slices.Contains(enabledExtensions, storageID) {
		return PipelineConfig{}, errors.New("filelog storage must reference an enabled file_storage extension")
	}
	extensions.namedObject(storageID, "file storage extension").boolean("recreate", false)
	if err != nil {
		return PipelineConfig{}, err
	}
	if files > math.MaxInt64-2 || calls < files+2 {
		return PipelineConfig{}, errors.New("max_concurrent_calls must be at least max_concurrent_files + 2")
	}

	promtailBound := exporterBound(exporters.namedObject(cfg.PromtailExporterID, "Promtail-compatible exporter"))
	if err != nil {
		return PipelineConfig{}, err
	}
	cfg.RetryBound = promtailBound
	const overhead = 20 * time.Second
	if cfg.RetryBound > time.Duration(math.MaxInt64)-overhead ||
		cfg.ExportLifetime < cfg.RetryBound+overhead {
		return PipelineConfig{}, errors.New("export_lifetime must cover retry/timeout bounds plus 20s")
	}
	return cfg, nil
}

// ValidateEffectivePipelineConfig checks the Collector's defaulted, marshaled configuration.
func ValidateEffectivePipelineConfig(conf *confmap.Conf) (PipelineConfig, error) {
	if conf == nil {
		return ValidatePipelineConfig(nil)
	}
	values := conf.ToStringMap()
	extensions, _ := values["extensions"].(map[string]any)
	for id, raw := range extensions {
		if !componentType(id, "file_storage") {
			continue
		}
		fields, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		// FileStorage marks recreate as omitempty; an omitted effective value is false.
		if _, exists := fields["recreate"]; !exists {
			fields["recreate"] = false
		}
	}
	exporters, _ := values["exporters"].(map[string]any)
	for id, raw := range exporters {
		if !componentType(id, "stsk8slogs") {
			continue
		}
		fields, _ := raw.(map[string]any)
		// Only a present null in effective config proves configoptional disabled the queue.
		if queue, exists := fields["sending_queue"]; exists && queue == nil {
			fields["sending_queue"] = map[string]any{"enabled": false}
		}
	}
	return ValidatePipelineConfig(confmap.NewFromStringMap(values))
}

func exporterBound(exporter configFields) time.Duration {
	timeout := exporter.duration("timeout")
	retry := exporter.object("retry_on_failure")
	retry.boolean("enabled", true)
	initial := retry.duration("initial_interval")
	interval := retry.duration("max_interval")
	elapsed := retry.duration("max_elapsed_time")
	retry.optionalNumber("multiplier", 1, math.MaxFloat64)
	retry.optionalNumber("randomization_factor", 0, 1)
	queue := exporter.object("sending_queue")
	queue.boolean("enabled", false)
	if len(queue.values) != 1 {
		queue.fail("", "must contain only enabled: false")
	}
	if *exporter.err != nil {
		return 0
	}
	if initial > interval || interval > elapsed {
		retry.fail("", "requires initial_interval <= max_interval <= max_elapsed_time")
		return 0
	}
	if elapsed > time.Duration(math.MaxInt64)-timeout {
		exporter.fail("", "retry and timeout duration sum overflows")
		return 0
	}
	return elapsed + timeout
}

func componentType(id string, allowed ...string) bool {
	kind, name, named := strings.Cut(id, "/")
	return slices.Contains(allowed, kind) && (!named || strings.TrimSpace(name) != "")
}

// Paths contain only validator-owned labels; errors never include configuration values.
type configFields struct {
	values map[string]any
	path   string
	err    *error
}

func (c configFields) fail(key, message string) {
	if *c.err == nil {
		path := c.path
		if key != "" {
			path += "." + key
		}
		*c.err = fmt.Errorf("%s %s", path, message)
	}
}

func (c configFields) object(key string) configFields {
	return c.namedObject(key, c.path+"."+key)
}

func (c configFields) namedObject(key, label string) configFields {
	value, exists := c.values[key]
	fields, ok := value.(map[string]any)
	if !exists || (value != nil && !ok) {
		result := configFields{path: label, err: c.err}
		result.fail("", "must be a configured mapping")
		return result
	}
	return configFields{values: fields, path: label, err: c.err}
}

func (c configFields) text(key string) string {
	value, ok := c.values[key].(string)
	if !ok || strings.TrimSpace(value) == "" {
		c.fail(key, "must be an explicit nonempty string")
	}
	return value
}

func (c configFields) boolean(key string, want bool) {
	value, ok := c.values[key].(bool)
	if !ok || value != want {
		c.fail(key, fmt.Sprintf("must be explicitly %t", want))
	}
}

func (c configFields) positiveInt(key string) int64 {
	value := reflect.ValueOf(c.values[key])
	var result int64
	if value.IsValid() {
		switch value.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			result = value.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			unsigned := value.Uint()
			if unsigned <= math.MaxInt64 {
				result = int64(unsigned)
			}
		default:
			c.fail(key, "must be an explicit positive integer")
		}
	}
	if result <= 0 || result > int64(math.MaxInt) {
		c.fail(key, "must be an explicit positive integer within the platform int range")
		return 0
	}
	return result
}

func (c configFields) duration(key string) time.Duration {
	var result time.Duration
	switch value := c.values[key].(type) {
	case time.Duration:
		result = value
	case string:
		var err error
		result, err = time.ParseDuration(value)
		if err != nil {
			result = 0
		}
	}
	if result <= 0 {
		c.fail(key, "must be an explicit positive duration within the time.Duration range")
	}
	return result
}

func (c configFields) strings(key string, optional bool) []string {
	value, exists := c.values[key]
	if optional && (!exists || value == nil) {
		return nil
	}
	var result []string
	switch list := value.(type) {
	case []string:
		result = list
	case []any:
		for _, item := range list {
			text, ok := item.(string)
			if !ok {
				c.fail(key, "must be a list of component IDs")
				return nil
			}
			result = append(result, text)
		}
	default:
		c.fail(key, "must be a list of component IDs")
	}
	seen := make(map[string]bool, len(result))
	for _, id := range result {
		if strings.TrimSpace(id) == "" || seen[id] {
			c.fail(key, "must contain nonempty, distinct component IDs")
		}
		seen[id] = true
	}
	return result
}

func (c configFields) optionalNumber(key string, minimum, maximum float64) {
	raw, exists := c.values[key]
	if !exists {
		return
	}
	value := reflect.ValueOf(raw)
	number := math.NaN()
	if value.IsValid() {
		switch value.Kind() {
		case reflect.Float32, reflect.Float64:
			number = value.Float()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			number = float64(value.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			number = float64(value.Uint())
		default:
			c.fail(key, "must be a finite number within the supported retry range")
		}
	}
	if math.IsNaN(number) || math.IsInf(number, 0) || number < minimum || number > maximum {
		c.fail(key, "must be a finite number within the supported retry range")
	}
}
