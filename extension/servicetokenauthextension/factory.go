package servicetokenauthextension

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

var (
	Type = component.MustNewType("service_token_auth")
)

func NewFactory() extension.Factory {
	return extension.NewFactory(
		Type,
		createDefaultConfig,
		createExtension,
		component.StabilityLevelAlpha,
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Cache: &CacheSettings{
			ValidSize:   100,
			ValidTtl:    5 * time.Minute,
			InvalidSize: 100,
		},
		Schema: "StackState",
	}
}

func createExtension(_ context.Context, _ extension.CreateSettings, cfg component.Config) (extension.Extension, error) {
	return newServerAuthExtension(cfg.(*Config))
}
