package ingestionapikeyauthextension

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		id          component.ID
		expected    component.Config
		expectedErr bool
	}{
		{
			id:          component.NewID(Type),
			expectedErr: true,
		},
		{
			id:          component.NewIDWithName(Type, "empty_endpoint"),
			expectedErr: true,
		},
		{
			id:          component.NewIDWithName(Type, "empty_url"),
			expectedErr: true,
		},
		{
			id: component.NewIDWithName(Type, "missing_cache"),
			expected: &Config{
				Endpoint: &EndpointSettings{
					Url: "http://localhost:8091/authorize",
				},
				Cache: &CacheSettings{
					ValidSize:   100,
					ValidTtl:    5 * time.Minute,
					InvalidSize: 100,
				},
				Schema: "StackState",
			},
		},
		{
			id: component.NewIDWithName(Type, "empty_cache"),
			expected: &Config{
				Endpoint: &EndpointSettings{
					Url: "http://localhost:8091/authorize",
				},
				Cache: &CacheSettings{
					ValidSize:   100,
					ValidTtl:    5 * time.Minute,
					InvalidSize: 100,
				},
				Schema: "StackState",
			},
		},
		{
			id: component.NewIDWithName(Type, "valid"),
			expected: &Config{
				Endpoint: &EndpointSettings{
					Url: "http://localhost:8091/authorize",
				},
				Cache: &CacheSettings{
					ValidSize:   10,
					ValidTtl:    20 * time.Second,
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
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()
			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, component.UnmarshalConfig(sub, cfg))
			if tt.expectedErr {
				assert.Error(t, component.ValidateConfig(cfg))
				return
			}
			assert.NoError(t, component.ValidateConfig(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
