package stsksmreceiver //nolint:testpackage // Validates unexported defaults.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

func validConfig() *Config {
	cfg, _ := createDefaultConfig().(*Config)
	cfg.ClusterName = "cluster"
	return cfg
}

func TestDefaultConfig(t *testing.T) {
	cfg, ok := createDefaultConfig().(*Config)
	require.True(t, ok)
	assert.Equal(t, authTypeServiceAccount, cfg.AuthType)
	assert.Equal(t, 40*time.Second, cfg.CollectionInterval)
	assert.Equal(t, 300*time.Second, cfg.ResyncPeriod)
	assert.Equal(t, defaultCollectors(), cfg.Collectors)
	assert.Contains(t, cfg.Collectors, "endpoints")
	assert.Nil(t, cfg.K8sLeaderElector)
	assert.Error(t, cfg.Validate(), "cluster_name has no default")
}

func TestConfigValidate(t *testing.T) {
	require.NoError(t, validConfig().Validate())
	for name, mutate := range map[string]func(*Config){
		"unknown auth type":    func(c *Config) { c.AuthType = "none" },
		"missing cluster name": func(c *Config) { c.ClusterName = " " },
		"zero interval":        func(c *Config) { c.CollectionInterval = 0 },
		"sub-second resync":    func(c *Config) { c.ResyncPeriod = 500 * time.Millisecond },
		"no collectors":        func(c *Config) { c.Collectors = nil },
		"empty collector":      func(c *Config) { c.Collectors = []string{defaultCollectors()[0], ""} },
		"namespace label wildcard": func(c *Config) {
			c.LabelsAsTags = map[string]map[string]string{namespaceKind: {"*": "ns_%%label%%"}}
		},
		"namespace annotation wildcard": func(c *Config) {
			c.AnnotationsAsTags = map[string]map[string]string{namespaceKind: {"*": "ns_%%annotation%%"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mutate(cfg)
			assert.Error(t, cfg.Validate())
		})
	}
}

func TestConfigAcceptsSupportedTagMappings(t *testing.T) {
	cfg := validConfig()
	cfg.LabelsAsTags = map[string]map[string]string{
		namespaceKind: {"tier": "namespace_tier"},
		"statefulset": {"*": "sts_%%label%%"},
	}
	cfg.AnnotationsAsTags = map[string]map[string]string{namespaceKind: {"owner.example.com/contact": "namespace_contact"}}
	assert.NoError(t, cfg.Validate())
}

func TestConfigUnmarshal(t *testing.T) {
	collectors := defaultCollectors()[:2]
	cfg, _ := createDefaultConfig().(*Config)
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"auth_type":           "kubeConfig",
		"cluster_name":        "Prod_EU",
		"collection_interval": "15s",
		"resync_period":       "60s",
		"collectors":          []any{collectors[0], collectors[1]},
		"labels_as_tags":      map[string]any{"pod": map[string]any{"app.kubernetes.io/name": "app"}},
		"annotations_as_tags": map[string]any{"deployment": map[string]any{"owner": "team"}},
		"k8s_leader_elector":  "k8s_leader_elector",
	}).Unmarshal(cfg))
	require.NoError(t, cfg.Validate())
	assert.Equal(t, authTypeKubeConfig, cfg.AuthType)
	assert.Equal(t, "prod_eu", cfg.clusterName())
	assert.Equal(t, 15*time.Second, cfg.CollectionInterval)
	assert.Equal(t, time.Minute, cfg.ResyncPeriod)
	assert.Equal(t, collectors, cfg.Collectors)
	assert.Equal(t, map[string]map[string]string{"pod": {"app.kubernetes.io/name": "app"}}, cfg.LabelsAsTags)
	assert.Equal(t, map[string]map[string]string{"deployment": {"owner": "team"}}, cfg.AnnotationsAsTags)
	assert.Equal(t, component.MustNewID("k8s_leader_elector"), *cfg.K8sLeaderElector)
}
