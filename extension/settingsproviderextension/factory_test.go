package settingsproviderextension

import (
	"context"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension"
	"testing"
	"time"
)

func TestFactory_CreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig()
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
	assert.NotNil(t, cfg)
}

func TestFactory_NewFactory(t *testing.T) {
	f := NewFactory()
	assert.NotNil(t, f)
	assert.Equal(t, f.Type(), Type)
}

func TestFactory_CreateExtension(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *stsSettingsConfig.Config
		expectError bool
	}{
		{
			name: "Valid File Config",
			cfg: &stsSettingsConfig.Config{
				File: &stsSettingsConfig.FileSettingsProviderConfig{
					Path:           "../provider/file/testdata/settings.yaml",
					UpdateInterval: 30 * time.Second,
				},
			},
			expectError: false,
		},
		{
			name: "Valid Kafka Config",
			cfg: &stsSettingsConfig.Config{
				Kafka: &stsSettingsConfig.KafkaSettingsProviderConfig{
					Brokers:    []string{"localhost:9092"},
					Topic:      "sts-internal-settings",
					BufferSize: 1000,
				},
			},
			expectError: false,
		},
		{
			name:        "Invalid Config (no source)",
			cfg:         &stsSettingsConfig.Config{},
			expectError: true,
		},
		{
			name: "Invalid Config (both sources)",
			cfg: &stsSettingsConfig.Config{
				File: &stsSettingsConfig.FileSettingsProviderConfig{
					Path: "/path/to/testdata/mappings.yaml",
				},
				Kafka: &stsSettingsConfig.KafkaSettingsProviderConfig{
					Brokers: []string{"localhost:9092"},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			set := extension.CreateSettings{}
			ctx := context.Background()
			ext, err := createExtension(ctx, set, tt.cfg)

			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, ext)
			} else {
				require.NoError(t, err)
				require.NotNil(t, ext)
			}
		})
	}
}
