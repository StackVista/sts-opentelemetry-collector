package config_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/config"
	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *config.Config
		wantErr error
	}{
		{
			name:    "Valid File Config",
			cfg:     &config.Config{File: &config.FileSettingsProviderConfig{Path: "/some/path"}},
			wantErr: nil,
		},
		{
			name:    "Valid Kafka Config",
			cfg:     &config.Config{Kafka: &config.KafkaSettingsProviderConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"}},
			wantErr: nil,
		},
		{
			name:    "Missing Config",
			cfg:     &config.Config{},
			wantErr: errors.New("must specify either 'file' or 'kafka' configuration"),
		},
		{
			name:    "Both Configs Specified",
			cfg:     &config.Config{File: &config.FileSettingsProviderConfig{Path: "/some/path"}, Kafka: &config.KafkaSettingsProviderConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic"}},
			wantErr: errors.New("cannot specify both 'file' and 'kafka' configuration"),
		},
		{
			name:    "File Config Missing Path",
			cfg:     &config.Config{File: &config.FileSettingsProviderConfig{}},
			wantErr: errors.New("'path' must be specified when using file source"),
		},
		{
			name:    "Kafka Config Missing Brokers",
			cfg:     &config.Config{Kafka: &config.KafkaSettingsProviderConfig{Topic: "test-topic"}},
			wantErr: errors.New("at least one kafka broker must be specified"),
		},
		{
			name:    "Kafka Config Missing Topic",
			cfg:     &config.Config{Kafka: &config.KafkaSettingsProviderConfig{Brokers: []string{"localhost:9092"}}},
			wantErr: errors.New("'topic' must be specified when using kafka source"),
		},
		{
			name:    "File Config with Zero UpdateInterval (should be defaulted)",
			cfg:     &config.Config{File: &config.FileSettingsProviderConfig{Path: "/some/path", UpdateInterval: 0}},
			wantErr: nil,
		},
		{
			name:    "Kafka Config with Zero BufferSize (should be defaulted)",
			cfg:     &config.Config{Kafka: &config.KafkaSettingsProviderConfig{Brokers: []string{"localhost:9092"}, Topic: "test-topic", BufferSize: 0}},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.NoError(t, err)
				// Check for defaults if no error is expected
				if tt.cfg.File != nil && tt.cfg.File.UpdateInterval == 0 {
					assert.Equal(t, 30*time.Second, tt.cfg.File.UpdateInterval)
				}
				if tt.cfg.Kafka != nil && tt.cfg.Kafka.BufferSize == 0 {
					assert.Equal(t, 1000, tt.cfg.Kafka.BufferSize)
				}
			}
		})
	}
}
