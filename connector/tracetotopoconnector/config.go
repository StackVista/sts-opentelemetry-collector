package tracetotopoconnector

import (
	"fmt"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/metrics"
	"go.opentelemetry.io/collector/component"
)

const (
	defaultCacheSize = 5000
	maxCacheSize     = 1000_000
	minValidTTL      = 100 * time.Nanosecond // to avoid hashicorp/golang-lru ticker panic
)

// Config defines the configuration options for tracetotopoconnector.
type Config struct {
	ExpressionCacheSettings  CacheSettings `mapstructure:"expression_cache_settings"`
	TagRegexCacheSettings    CacheSettings `mapstructure:"tag_regex_cache_settings"`
	TagTemplateCacheSettings CacheSettings `mapstructure:"tag_template_cache_settings"`
}

type CacheSettings struct {
	EnableMetrics bool          `mapstructure:"enable_metrics"`
	Size          int           `mapstructure:"size"`
	TTL           time.Duration `mapstructure:"ttl"`
}

func (c *CacheSettings) ToMetered(
	name string,
	telemetrySettings component.TelemetrySettings,
) metrics.MeteredCacheSettings {
	return metrics.MeteredCacheSettings{
		Name:              name,
		EnableMetrics:     c.EnableMetrics,
		Size:              c.Size,
		TTL:               c.TTL,
		TelemetrySettings: telemetrySettings,
	}
}

func (c *CacheSettings) Validate(cacheName string) error {
	if c.Size <= 0 {
		c.Size = defaultCacheSize
	} else if c.Size > maxCacheSize {
		return fmt.Errorf("%s.size: must not exceed %d", cacheName, maxCacheSize)
	}

	// TTL = 0 means "no expiration", so allow it
	if c.TTL < 0 {
		return fmt.Errorf("%s.ttl: must not be negative (0 disables expiration)", cacheName)
	} else if c.TTL > 0 && c.TTL <= minValidTTL {
		return fmt.Errorf("%s.ttl: must be greater than %s to avoid internal timer issues", cacheName, minValidTTL)
	}

	return nil
}

func (cfg *Config) Validate() error {
	if err := cfg.ExpressionCacheSettings.Validate("expression_cache"); err != nil {
		return err
	}
	if err := cfg.TagRegexCacheSettings.Validate("tag_regex_cache"); err != nil {
		return err
	}
	if err := cfg.TagTemplateCacheSettings.Validate("tag_template_cache"); err != nil {
		return err
	}
	return nil
}
