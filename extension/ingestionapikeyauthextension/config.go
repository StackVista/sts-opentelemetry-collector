package ingestionapikeyauthextension

import (
	"fmt"
	"time"
)

type EndpointSettings struct {
	Url string `mapstructure:"url"`
}

type CacheSettings struct {
	ValidSize   int           `mapstructure:"valid_size"`
	ValidTtl    time.Duration `mapstructure:"valid_ttl"`
	InvalidSize int           `mapstructure:"invalid_size"`
}

type Config struct {
	Endpoint *EndpointSettings `mapstructure:"endpoint,omitempty"`
	Cache    *CacheSettings    `mapstructure:"cache,omitempty"`
}

func (cfg *Config) Validate() error {
	if cfg.Endpoint == nil {
		return fmt.Errorf("required endpoint paramater")
	}
	if len(cfg.Endpoint.Url) == 0 {
		return fmt.Errorf("required endpoint.url paramater")
	}

	if cfg.Cache == nil {
		return fmt.Errorf("required cache paramater")
	}
	if cfg.Cache.ValidSize <= 0 {
		return fmt.Errorf("paramater cache.valid_size must be a postive value")
	}
	if cfg.Cache.ValidTtl <= 0 {
		return fmt.Errorf("paramater cache.valid_ttl_seconds must be a postive value")
	}
	if cfg.Cache.InvalidSize <= 0 {
		return fmt.Errorf("paramater cache.invalid_size must be a postive value")
	}
	return nil
}
