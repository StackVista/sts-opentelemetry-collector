package settingsproviderextension

import (
	"context"
	"fmt"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal"
	stsSettingsSource "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/source"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

var (
	Type = component.MustNewType("settings_provider")
)

func NewFactory() extension.Factory {
	return extension.NewFactory(
		Type,
		createDefaultConfig,
		createExtension,
		// TODO: eventually this needs to become StabilityLevelStable
		component.StabilityLevelDevelopment,
	)
}

func createDefaultConfig() component.Config {
	return &stsSettingsConfig.Config{
		File: &stsSettingsConfig.FileSettingsProviderConfig{
			Path: "./testdata/otel_mappings.yaml",
		},
	}
}

func createExtension(_ context.Context, set extension.CreateSettings, cfg component.Config) (extension.Extension, error) {
	topoCfg := cfg.(*stsSettingsConfig.Config)
	logger := set.Logger

	if topoCfg.File != nil {
		fileProvider, err := stsSettingsSource.NewFileSettingsProvider(topoCfg.File, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create file provider: %w", err)
		}
		return fileProvider, nil
	}

	if topoCfg.Kafka != nil {
		kafkaProvider, err := stsSettingsSource.NewKafkaSettingsProvider(topoCfg.Kafka, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka provider: %w", err)
		}
		return kafkaProvider, nil
	}

	return nil, fmt.Errorf("configuration must specify either 'file' or 'kafka'")
}
