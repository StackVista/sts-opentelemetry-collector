package ingestionapikeyauthextension

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

var (
	Type = component.MustNewType("ingestion_api_key_auth")
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
	}
}

func createExtension(_ context.Context, _ extension.CreateSettings, cfg component.Config) (extension.Extension, error) {
	return newServerAuthExtension(cfg.(*Config))
}
