package stslogsagentextension

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

func NewFactory() extension.Factory {
	return extension.NewFactory(component.MustNewType("stslogsagent"), createDefaultConfig,
		createExtension, component.StabilityLevelDevelopment)
}

func createDefaultConfig() component.Config {
	return &Config{HealthEndpoint: "0.0.0.0:13133"}
}

func createExtension(_ context.Context, set extension.Settings, config component.Config) (extension.Extension, error) {
	cfg, ok := config.(*Config)
	if !ok {
		return nil, errors.New("invalid logs agent configuration")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newController(*cfg, set)
}
