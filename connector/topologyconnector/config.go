package topologyconnector

import (
	"fmt"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"go.opentelemetry.io/collector/component"
)

const (
	// cache defaults
	defaultCacheSize = 5000
	maxCacheSize     = 1000_000
	minValidTTL      = 100 * time.Nanosecond // to avoid hashicorp/golang-lru ticker panic
)

// Config defines the configuration options for topologyconnector.
type Config struct {
	ExpressionCache  CacheSettings         `mapstructure:"expression_cache"`
	TagRegexCache    CacheSettings         `mapstructure:"tag_regex_cache"`
	TagTemplateCache CacheSettings         `mapstructure:"tag_template_cache"`
	Deduplication    DeduplicationSettings `mapstructure:"deduplication"`
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

// DeduplicationSettings controls pre-mapping CacheTTL-aware deduplication.
type DeduplicationSettings struct {
	// Enabled toggles deduplication entirely.
	Enabled bool `mapstructure:"enabled"`
	// RefreshFraction controls how often a mapping is re-emitted relative to its
	// configured expiration window (ExpireAfterMs).
	//
	// After a mapping has been sent, subsequent invocations are suppressed until
	// at least (RefreshFraction * ExpireAfterMs) has elapsed since the last send.
	//
	// For example, a value of 0.5 means the mapping may be re-sent halfway through
	// its expiration window; invocations before that are skipped.
	//
	// Valid range: 0.1 .. 0.9. A value of 0 is treated as the default (0.5).
	RefreshFraction float64       `mapstructure:"refresh_fraction"`
	Cache           CacheSettings `mapstructure:"cache"`
}

func (c *CacheSettings) Validate(cacheName string) error {
	if c.Size <= 0 {
		c.Size = defaultCacheSize
	} else if c.Size > maxCacheSize {
		return fmt.Errorf("%s.size: must not exceed %d", cacheName, maxCacheSize)
	}

	// CacheTTL = 0 means "no expiration", so allow it
	if c.TTL < 0 {
		return fmt.Errorf("%s.ttl: must not be negative (0 disables expiration)", cacheName)
	} else if c.TTL > 0 && c.TTL <= minValidTTL {
		return fmt.Errorf("%s.ttl: must be greater than %s to avoid internal timer issues", cacheName, minValidTTL)
	}

	return nil
}

func (d *DeduplicationSettings) Validate() error {
	// Normalise deduplication refresh fraction
	if d.RefreshFraction <= 0 {
		d.RefreshFraction = 0.5
	} else if d.RefreshFraction > 1 {
		d.RefreshFraction = 1
	}

	if err := d.Cache.Validate("deduplication_cache"); err != nil {
		return err
	}

	return nil
}

func (cfg *Config) Validate() error {
	if err := cfg.ExpressionCache.Validate("expression_cache"); err != nil {
		return err
	}
	if err := cfg.TagRegexCache.Validate("tag_regex_cache"); err != nil {
		return err
	}
	if err := cfg.TagTemplateCache.Validate("tag_template_cache"); err != nil {
		return err
	}
	if err := cfg.Deduplication.Validate(); err != nil {
		return err
	}
	return nil
}
