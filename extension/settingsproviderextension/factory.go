package settingsproviderextension

import (
	"context"
	"errors"
	"fmt"

	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/config"
	stsFileSettingsSource "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal/provider/file"
	stsKafkaSettingsSource "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal/provider/kafka"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

//nolint:gochecknoglobals
var (
	Type = component.MustNewType("sts_settings_provider")
)

func NewFactory() extension.Factory {
	return extension.NewFactory(
		Type,
		CreateDefaultConfig,
		CreateExtension,
		// TODO: eventually this needs to become StabilityLevelStable
		component.StabilityLevelDevelopment,
	)
}

// createDefaultConfig builds the base config for the extension
func CreateDefaultConfig() component.Config {
	return &stsSettingsConfig.Config{}
}

func CreateExtension(
	ctx context.Context,
	set extension.Settings,
	cfg component.Config,
) (extension.Extension, error) {
	topoCfg, ok := cfg.(*stsSettingsConfig.Config)
	if !ok {
		return nil, errors.New("unable to cast config")
	}

	logger := set.Logger

	if topoCfg.File != nil {
		fileProvider, err := stsFileSettingsSource.NewFileSettingsProvider(topoCfg.File, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create file provider: %w", err)
		}
		return fileProvider, nil
	}

	if topoCfg.Kafka != nil {
		kafkaProvider, err := stsKafkaSettingsSource.NewKafkaSettingsProvider(
			ctx,
			topoCfg.Kafka,
			set.TelemetrySettings,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka provider: %w", err)
		}
		return kafkaProvider, nil
	}

	return nil, fmt.Errorf("configuration must specify either 'file' or 'kafka'")
}
