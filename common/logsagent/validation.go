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
	ExtensionID          string
	PromtailExporterID   string
	OTELNativeExporterID string
	ExportLifetime       time.Duration
	RetryBound           time.Duration
}

// ValidatePipelineConfig requires explicitly authored bounds for configuration fixtures.
// Component-local validation, the emitter feature gate and Pod grace are separate checks.
func ValidatePipelineConfig(conf *confmap.Conf) (PipelineConfig, error) {
	if conf == nil {
		return PipelineConfig{}, errors.New("logs agent configuration is required")
	}
	var err error
	root := configFields{values: conf.ToStringMap(), path: "config", err: &err}
	cfg, input, delivery, err := deliveryPipelines(root)
	if err != nil {
		return PipelineConfig{}, err
	}
	service := root.object("service")
	exporters := root.object("exporters")
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
	var nativeBound time.Duration
	if cfg.OTELNativeExporterID != "" {
		nativeBound = exporterBound(exporters.namedObject(cfg.OTELNativeExporterID, "OTELNative exporter"))
	}
	if err != nil {
		return PipelineConfig{}, err
	}
	cfg.RetryBound = max(promtailBound, nativeBound)
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
		if !componentType(id, "stsk8slogs", "otlp_http", "otlphttp", "otlp_grpc", "otlp") {
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

func deliveryPipelines(root configFields) (PipelineConfig, configFields, configFields, error) {
	var cfg PipelineConfig
	var input, delivery configFields
	service := root.object("service")
	pipelines := service.object("pipelines")
	exporters := root.object("exporters")
	connectors, ok := root.values["connectors"].(map[string]any)
	if root.values["connectors"] != nil && !ok {
		return cfg, input, delivery, errors.New("connectors must be a configured mapping")
	}
	if len(connectors) == 0 {
		if len(pipelines.values) != 1 {
			return cfg, input, delivery, errors.New("logs agent requires exactly one pipeline")
		}
		var inputID string
		for id := range pipelines.values {
			inputID = id
		}
		if !componentType(inputID, "logs") {
			return cfg, input, delivery, errors.New("logs agent requires a logs input pipeline")
		}
		input = pipelines.namedObject(inputID, "input pipeline")
		outputs := input.strings("exporters", false)
		if len(outputs) != 1 || !componentType(outputs[0], "stsk8slogs") {
			return cfg, input, delivery, errors.New("input pipeline must export only to one stsk8slogs exporter")
		}
		cfg.PromtailExporterID = outputs[0]
		delivery = exporters.namedObject(outputs[0], "Promtail-compatible exporter").object("delivery")
		cfg.ExtensionID = delivery.text("controller_extension")
		cfg.ExportLifetime = delivery.duration("export_lifetime")
		controller := deliveryController(root, cfg.ExtensionID)
		if *root.err != nil {
			return cfg, input, delivery, *root.err
		}
		if discoveryEnabled(controller) {
			return cfg, input, delivery, errors.New("direct logs delivery requires discovery_enabled to be false")
		}
		return cfg, input, delivery, *root.err
	}
	if len(connectors) != 1 {
		return cfg, input, delivery, errors.New("logs agent requires exactly one route connector")
	}
	var routeID string
	for id := range connectors {
		routeID = id
	}
	if !componentType(routeID, "stslogsroute") {
		return cfg, input, delivery, errors.New("logs agent connector must be stslogsroute")
	}
	delivery = root.object("connectors").namedObject(routeID, "route connector")
	cfg.ExtensionID = delivery.text("controller_extension")
	cfg.ExportLifetime = delivery.duration("export_lifetime")
	promtailID := delivery.text("promtail_pipeline")
	nativeID, _ := delivery.values["native_pipeline"].(string)
	if !componentType(promtailID, "logs") ||
		(nativeID != "" && (promtailID == nativeID || !componentType(nativeID, "logs"))) {
		return cfg, input, delivery, errors.New("route must reference two distinct logs pipelines")
	}
	controller := deliveryController(root, cfg.ExtensionID)
	if *root.err != nil {
		return cfg, input, delivery, *root.err
	}
	discovery := discoveryEnabled(controller)
	if discovery != (nativeID != "") {
		return cfg, input, delivery, errors.New("native_pipeline must be configured exactly when discovery_enabled is true")
	}
	pipelineCount, countName := 2, "two"
	if discovery {
		pipelineCount, countName = 3, "three"
	}
	if len(pipelines.values) != pipelineCount {
		return cfg, input, delivery, fmt.Errorf("logs agent requires exactly %s pipelines", countName)
	}
	var inputID string
	for id := range pipelines.values {
		if id != promtailID && id != nativeID {
			if inputID != "" || !componentType(id, "logs") {
				return cfg, input, delivery, errors.New("logs agent requires one logs input pipeline")
			}
			inputID = id
		}
	}
	input = pipelines.namedObject(inputID, "input pipeline")
	cfg.PromtailExporterID = terminalExporter(pipelines.namedObject(promtailID, "Promtail pipeline"), routeID)
	if discovery {
		cfg.OTELNativeExporterID = terminalExporter(pipelines.namedObject(nativeID, "OTELNative pipeline"), routeID)
	}
	if *root.err != nil {
		return cfg, input, delivery, *root.err
	}
	if !componentType(cfg.PromtailExporterID, "stsk8slogs") {
		return cfg, input, delivery, errors.New("the Promtail pipeline must export to stsk8slogs")
	}
	if discovery && !componentType(cfg.OTELNativeExporterID, "otlp_http", "otlphttp", "otlp_grpc", "otlp") {
		return cfg, input, delivery, errors.New("OTELNative pipeline requires an OTLP exporter")
	}
	if outputs := input.strings("exporters", false); len(outputs) != 1 || outputs[0] != routeID {
		return cfg, input, delivery, errors.New("input pipeline must export only to the route connector")
	}
	if _, exists := root.object("receivers").values[routeID]; exists {
		return cfg, input, delivery, errors.New("route connector must not also be defined as a receiver")
	}
	if _, exists := exporters.values[routeID]; exists {
		return cfg, input, delivery, errors.New("route connector must not also be defined as an exporter")
	}
	for _, id := range []string{cfg.PromtailExporterID, cfg.OTELNativeExporterID} {
		if id == "" {
			continue
		}
		exporter := exporters.namedObject(id, "routed terminal exporter")
		if exporter.values["delivery"] != nil {
			return cfg, input, delivery, errors.New("routed terminal exporters must not configure delivery")
		}
	}
	return cfg, input, delivery, *root.err
}

func deliveryController(root configFields, id string) configFields {
	if !componentType(id, "stslogsagent") {
		root.fail("controller_extension", "must reference stslogsagent")
	}
	if !slices.Contains(root.object("service").strings("extensions", false), id) {
		root.fail("controller_extension", "must be enabled in service.extensions")
	}
	return root.object("extensions").namedObject(id, "controller extension")
}

func discoveryEnabled(controller configFields) bool {
	value, exists := controller.values["discovery_enabled"]
	enabled, ok := value.(bool)
	if exists && !ok {
		controller.fail("discovery_enabled", "must be a boolean")
	}
	return enabled
}

func terminalExporter(pipeline configFields, routeID string) string {
	receivers := pipeline.strings("receivers", false)
	if len(receivers) != 1 || receivers[0] != routeID {
		pipeline.fail("receivers", "must contain only the route connector")
	}
	if len(pipeline.strings("processors", true)) != 0 {
		pipeline.fail("processors", "must be empty in terminal pipelines")
	}
	exporters := pipeline.strings("exporters", false)
	if len(exporters) != 1 {
		pipeline.fail("exporters", "must contain exactly one exporter")
		return ""
	}
	return exporters[0]
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
