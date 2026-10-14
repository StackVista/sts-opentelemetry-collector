package stsk8seventsexporter //nolint:testpackage // Validates unexported defaults.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func validConfig() *Config {
	cfg, _ := createDefaultConfig().(*Config)
	cfg.Endpoint = "https://receiver.example" + intakeSuffix
	cfg.APIKey = "key"
	cfg.ClusterName = "cluster"
	return cfg
}

func TestConfigValidate(t *testing.T) {
	require.NoError(t, validConfig().Validate())
	for name, mutate := range map[string]func(*Config){
		"endpoint without intake path": func(c *Config) { c.Endpoint = "https://receiver.example" },
		"missing api key":              func(c *Config) { c.APIKey = " " },
		"api key with control char":    func(c *Config) { c.APIKey = "key\n" },
		"missing cluster name":         func(c *Config) { c.ClusterName = "" },
		"unknown cluster type":         func(c *Config) { c.ClusterType = "eks" },
		"unknown auth type":            func(c *Config) { c.AuthType = "token" },
		"unknown event category":       func(c *Config) { c.EventCategories = map[string]string{"DarkMystery": "V0ID"} },
		"zero timeout":                 func(c *Config) { c.TimeoutSettings.Timeout = 0 },
		"zero interval":                func(c *Config) { c.Interval = 0 },
		"zero max events per request":  func(c *Config) { c.MaxEventsPerRequest = 0 },
		"zero max attempts":            func(c *Config) { c.MaxAttempts = 0 },
		"buffer smaller than request":  func(c *Config) { c.MaxBufferedEvents = c.MaxEventsPerRequest - 1 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mutate(cfg)
			assert.Error(t, cfg.Validate())
		})
	}
}

func TestConfigUnmarshal(t *testing.T) {
	cfg, _ := createDefaultConfig().(*Config)
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"endpoint":         "https://receiver.example" + intakeSuffix,
		"api_key":          "key",
		"cluster_name":     "cluster",
		"cluster_type":     "openshift",
		"default_host":     "node-cluster",
		"event_categories": map[string]any{"MysteryChange": "Changes"},
		"timeout":          "5s",
	}).Unmarshal(cfg))
	require.NoError(t, cfg.Validate())
	assert.Equal(t, "openshift", cfg.ClusterType)
	assert.Equal(t, "node-cluster", cfg.DefaultHost)
	assert.Equal(t, "Changes", string(cfg.categoryOverrides()["MysteryChange"]))
	assert.Equal(t, "5s", cfg.TimeoutSettings.Timeout.String())
}

func TestFactoryCreatesLogsExporter(t *testing.T) {
	factory := NewFactory()
	cfg := validConfig()
	cfg.InternalHostname = "collector"
	cfg.AuthType = authTypeNone
	exp, err := factory.CreateLogs(context.Background(), exportertest.NewNopSettings(factory.Type()), cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, exp.Shutdown(context.Background()))
}

func TestFactoryRejectsInvalidConfig(t *testing.T) {
	factory := NewFactory()
	_, err := factory.CreateLogs(context.Background(), exportertest.NewNopSettings(factory.Type()), factory.CreateDefaultConfig())
	assert.Error(t, err)
}
