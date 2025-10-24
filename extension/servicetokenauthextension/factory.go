package servicetokenauthextension

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

var (
	//nolint:gochecknoglobals
	Type = component.MustNewType("service_token_auth")
)

func NewFactory() extension.Factory {
	return extension.NewFactory(
		Type,
		createDefaultConfig,
		CreateExtension,
		component.StabilityLevelAlpha,
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Cache: &CacheSettings{
			ValidSize:   100,
			ValidTTL:    5 * time.Minute,
			InvalidSize: 100,
		},
		Schema: "StackState",
	}
}

func CreateExtension(_ context.Context, _ extension.Settings, cfg component.Config) (extension.Extension, error) {
	config, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("error casting the configuration")
	}

	return NewServiceTokenAuth(config)
}
