package stsauthenticationextension

import (
	"path/filepath"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/extension/stsauthenticationextension/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		id          component.ID
		expected    component.Config
		expectedErr bool
	}{
		{
			id:          component.NewID(metadata.Type),
			expectedErr: true,
		},
		{
			id: component.NewIDWithName(metadata.Type, "sometoken"),
			expected: &Config{
				Tenants: map[configopaque.String]string{"sometoken": "tenant"},
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
