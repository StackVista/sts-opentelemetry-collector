package settingsproviderextension

import (
	"context"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension"
	"testing"
	"time"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig()
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
	assert.NotNil(t, cfg)
}

func TestNewFactory(t *testing.T) {
	f := NewFactory()
	assert.NotNil(t, f)
	assert.Equal(t, f.Type(), Type)
}

func TestCreateExtension(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *stsSettingsConfig.Config
		expectError bool
	}{
		{
			name: "Valid File Config",
			cfg: &stsSettingsConfig.Config{
				File: &stsSettingsConfig.FileSourceConfig{
					Path:           "./testdata/otel_mappings.yaml",
					UpdateInterval: 30 * time.Second,
				},
			},
			expectError: false,
		},
		//{
		//	name: "Valid Kafka Config",
		//	cfg: &stsSettingsConfig.Config{
		//		Kafka: &stsSettingsConfig.KafkaSourceConfig{
		//			Brokers: []string{"localhost:9092"},
		//			Topic:   "sts_internal_settings",
		//		},
		//	},
		//	expectError: false,
		//},
		{
			name:        "Invalid Config (no source)",
			cfg:         &stsSettingsConfig.Config{},
			expectError: true,
		},
		{
			name: "Invalid Config (both sources)",
			cfg: &stsSettingsConfig.Config{
				File: &stsSettingsConfig.FileSourceConfig{
					Path: "/path/to/testdata/mappings.yaml",
				},
				Kafka: &stsSettingsConfig.KafkaSourceConfig{
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
