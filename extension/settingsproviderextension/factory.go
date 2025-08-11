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
		File: &stsSettingsConfig.FileSourceConfig{
			Path: "location", // TODO
		},
	}
}

func createExtension(_ context.Context, set extension.CreateSettings, cfg component.Config) (extension.Extension, error) {
	topoCfg := cfg.(*stsSettingsConfig.Config)
	logger := set.Logger

	if topoCfg.File != nil {
		fileProvider, err := stsSettingsSource.NewFileProvider(topoCfg.File, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create file provider: %w", err)
		}
		return fileProvider, nil
	}

	if topoCfg.Kafka != nil {
		// TODO
		return nil, nil
	}

	return nil, fmt.Errorf("configuration must specify either 'file' or 'kafka'")
}
