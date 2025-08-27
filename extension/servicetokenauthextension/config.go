package servicetokenauthextension

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
	Schema   string            `mapstructure:"schema"`
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
	if cfg.Cache.ValidTtl <= time.Duration(100) {
		// It must be greater than 100 because the caching library being used, uses this value as the default number of
		// expiry buckets. This number of expiry buckets is used to calculate the cache entry expiry goroutine (ticker) tick's interval.
		// This means: a value smaller than 100 results in 0 due to integer division (ttl / 100).
		// Subsequently, it results in time.NewTicker() panicking because the interval is non-positive.
		return fmt.Errorf("paramater cache.valid_ttl must be a positive value greater than 100")
	}
	if cfg.Cache.InvalidSize <= 0 {
		return fmt.Errorf("paramater cache.invalid_size must be a postive value")
	}

	if len(cfg.Schema) == 0 {
		return fmt.Errorf("required schema paramater")
	}
	return nil
}
