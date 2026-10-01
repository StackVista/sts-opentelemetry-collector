package logsagent_test

import (
	"bytes"
	_ "embed"
	"testing"
	"text/template"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/confmap"
	"go.yaml.in/yaml/v3"
)

//go:embed testdata/promtail-agent.yaml.tmpl
var fixedConfigTemplate string

func fixedConfig(t *testing.T, s settings) map[string]any {
	t.Helper()
	tmpl, err := template.New("fixed-agent").Parse(fixedConfigTemplate)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, s); err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := yaml.Unmarshal(buf.Bytes(), &config); err != nil {
		t.Fatal(err)
	}
	return config
}

func TestFixedGraphCompatibility(t *testing.T) {
	config := fixedConfig(t, defaultSettings())
	bounds, err := logsagent.ValidatePipelineConfig(confmap.NewFromStringMap(config))
	if err != nil || bounds.OTELNativeExporterID != "" {
		t.Fatalf("fixed graph rejected: %+v, %v", bounds, err)
	}
	section(config, "extensions", "stslogsagent/logs")["discovery_enabled"] = true
	if _, err := logsagent.ValidatePipelineConfig(confmap.NewFromStringMap(config)); err == nil {
		t.Fatal("discovery accepted without a native route")
	}
	config = renderConfig(t, defaultSettings())
	delete(section(config, "extensions", "stslogsagent/logs"), "discovery_enabled")
	if _, err := logsagent.ValidatePipelineConfig(confmap.NewFromStringMap(config)); err == nil {
		t.Fatal("native graph accepted without explicit discovery")
	}
}

func TestFixedProcessCompatibility(t *testing.T) {
	f := newFixture(t, "native")
	f.configure = func(config map[string]any) {
		clear(config)
		for key, value := range fixedConfig(t, f.settings) {
			config[key] = value
		}
	}
	p := f.start(nil, true)
	p.ready()
	expected := f.appendRecords(0, 0, 1)
	f.backend.waitBodies("promtail", expected, true)
	p.waitExport("acknowledged")
	p.signal()
	p.wait(5*time.Second, true)
	p.assertDrain("completed")
	f.backend.assertOnly("promtail")
	f.backend.assertRecords("promtail", expected, true)
	if f.backend.polls() != 0 {
		t.Fatal("fixed configuration made feature requests")
	}
}
