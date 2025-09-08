package tracetotopoconnector

import (
	"fmt"
	"time"
)

const (
	defaultCacheSize = 1000
	maxCacheSize     = 20000
	defaultCacheTTL  = 30 * time.Minute
)

// Config defines the configuration options for tracetotopoconnector.
type Config struct {
	ExpressionCacheSettings ExpressionCacheSettings `mapstructure:"expression_cache_settings"`
}

type ExpressionCacheSettings struct {
	Size int           `mapstructure:"size"`
	TTL  time.Duration `mapstructure:"ttl"`
}

func (cfg *Config) Validate() error {
	if cfg.ExpressionCacheSettings.Size <= 0 {
		cfg.ExpressionCacheSettings.Size = defaultCacheSize
	} else if cfg.ExpressionCacheSettings.Size > maxCacheSize {
		return fmt.Errorf("expression_cache.size exceeds max allowed (%d)", maxCacheSize)
	}

	if cfg.ExpressionCacheSettings.TTL <= 0 {
		// no value provided, using the default
		cfg.ExpressionCacheSettings.TTL = defaultCacheTTL
	} else if cfg.ExpressionCacheSettings.TTL <= time.Duration(100) {
		// Guard against hashicorp/golang-lru bug with expiry buckets (ttl/100 == 0 result in a panic in the Ticker).
		// Must be strictly greater than 100ns.
		return fmt.Errorf("parameter expression_cache.ttl must be a positive value greater than 100ns")
	}

	return nil
}
