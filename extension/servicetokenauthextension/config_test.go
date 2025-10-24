package servicetokenauthextension_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/extension/servicetokenauthextension"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		id          component.ID
		expected    component.Config
		expectedErr bool
	}{
		{
			id:          component.NewID(servicetokenauthextension.Type),
			expectedErr: true,
		},
		{
			id:          component.NewIDWithName(servicetokenauthextension.Type, "empty_endpoint"),
			expectedErr: true,
		},
		{
			id:          component.NewIDWithName(servicetokenauthextension.Type, "empty_url"),
			expectedErr: true,
		},
		{
			id: component.NewIDWithName(servicetokenauthextension.Type, "missing_cache"),
			expected: &servicetokenauthextension.Config{
				Endpoint: &servicetokenauthextension.EndpointSettings{
					URL: "http://localhost:8091/authorize",
				},
				Cache: &servicetokenauthextension.CacheSettings{
					ValidSize:   100,
					ValidTTL:    5 * time.Minute,
					InvalidSize: 100,
				},
				Schema: "StackState",
			},
		},
		{
			id: component.NewIDWithName(servicetokenauthextension.Type, "empty_cache"),
			expected: &servicetokenauthextension.Config{
				Endpoint: &servicetokenauthextension.EndpointSettings{
					URL: "http://localhost:8091/authorize",
				},
				Cache: &servicetokenauthextension.CacheSettings{
					ValidSize:   100,
					ValidTTL:    5 * time.Minute,
					InvalidSize: 100,
				},
				Schema: "StackState",
			},
		},
		{
			id: component.NewIDWithName(servicetokenauthextension.Type, "valid"),
			expected: &servicetokenauthextension.Config{
				Endpoint: &servicetokenauthextension.EndpointSettings{
					URL: "http://localhost:8091/authorize",
				},
				Cache: &servicetokenauthextension.CacheSettings{
					ValidSize:   10,
					ValidTTL:    20 * time.Second,
					InvalidSize: 30,
				},
				Schema: "StackStack2",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
			require.NoError(t, err)
			factory := servicetokenauthextension.NewFactory()
			cfg := factory.CreateDefaultConfig()
			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))
			if tt.expectedErr {
				assert.Error(t, xconfmap.Validate(cfg))
				return
			}
			assert.NoError(t, xconfmap.Validate(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
