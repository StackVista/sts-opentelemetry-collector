package k8scrdreceiver

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"k8s.io/client-go/dynamic"
)

type DiscoveryMode string

const (
	DiscoveryModeAPIGroups DiscoveryMode = "api_groups"
	DiscoveryModeAll       DiscoveryMode = "all"
)

// Config defines configuration for the k8scrd receiver.
// The receiver watches CRDs/CRs via informers and uses a consolidated increment loop
// to detect changes and emit snapshots.
type Config struct {
	APIConfig APIConfig `mapstructure:",squash"`

	// IncrementInterval controls how often the process cache is compared against
	// the informer cache to detect and emit changes. Default: 10s, min: 1s.
	IncrementInterval time.Duration `mapstructure:"increment_interval"`

	// SnapshotInterval controls how often a full snapshot is emitted from the informer cache.
	// Snapshots emit all current resources as ADDED (for downstream TTL freshness) and
	// emit DELETED for any resources that were previously cached but are no longer present.
	// Default: 5m, min: 1m.
	SnapshotInterval time.Duration `mapstructure:"snapshot_interval"`

	// IncludeInitialState emits all existing CRDs/CRs on startup before the first interval tick.
	// When false, the first increment populates the process cache without emitting.
	IncludeInitialState bool `mapstructure:"include_initial_state"`

	// ClusterName identifies the observed cluster. Added to log records as k8s.cluster.name
	// and used to namespace the Valkey cache key in multi-cluster deployments.
	ClusterName string `mapstructure:"cluster_name"`

	// DiscoveryMode controls how CRDs are discovered: "api_groups" (filtered) or "all".
	DiscoveryMode DiscoveryMode `mapstructure:"discovery_mode"`

	// APIGroupFilters defines inclusion/exclusion patterns for API groups.
	// Only used when DiscoveryMode is "api_groups".
	APIGroupFilters *APIGroupFilters `mapstructure:"api_group_filters"`

	// ValkeyEndpoint is the address of a Valkey instance for cache persistence.
	// When set, the resource cache is persisted across restarts and leader failovers.
	// Format: "host:port" (e.g. "valkey:6379"). When empty, no external cache is used.
	ValkeyEndpoint string `mapstructure:"valkey_endpoint"`

	// K8sLeaderElector is the component ID of a k8sleaderelector extension.
	// When set, only the leader replica actively watches CRDs/CRs.
	K8sLeaderElector *component.ID `mapstructure:"k8s_leader_elector"`
}

// APIGroupFilters defines inclusion and exclusion patterns for API groups
type APIGroupFilters struct {
	// Include defines patterns to include (glob). Default: ["*"] (all groups)
	Include []string `mapstructure:"include"`

	// Exclude defines patterns to exclude (glob). Applied after include.
	Exclude []string `mapstructure:"exclude"`

	// Compiled regex caches (not exposed in config)
	includeRegexes []*regexp.Regexp
	excludeRegexes []*regexp.Regexp
}

func (c *Config) Validate() error {
	if c.IncrementInterval == 0 {
		c.IncrementInterval = 10 * time.Second
	}
	if c.IncrementInterval < 1*time.Second {
		return errors.New("increment_interval must be at least 1 second")
	}

	if c.SnapshotInterval == 0 {
		c.SnapshotInterval = 5 * time.Minute
	}
	if c.SnapshotInterval < 1*time.Minute {
		return errors.New("snapshot_interval must be at least 1 minute")
	}

	if c.SnapshotInterval < c.IncrementInterval {
		return errors.New("snapshot_interval must be greater than or equal to increment_interval")
	}

	if c.DiscoveryMode == "" {
		c.DiscoveryMode = DiscoveryModeAPIGroups
	}

	switch c.DiscoveryMode {
	case DiscoveryModeAPIGroups, DiscoveryModeAll:
		// Valid
	default:
		return fmt.Errorf("invalid discovery_mode %q: must be 'api_groups' or 'all'", c.DiscoveryMode)
	}

	if c.DiscoveryMode == DiscoveryModeAPIGroups {
		if c.APIGroupFilters == nil {
			c.APIGroupFilters = &APIGroupFilters{
				Include: []string{"*"},
				Exclude: []string{},
			}
		}

		if len(c.APIGroupFilters.Include) == 0 {
			return errors.New("api_group_filters.include cannot be empty when discovery_mode is 'api_groups'")
		}

		// Pre-compile and cache include patterns for performance
		c.APIGroupFilters.includeRegexes = make([]*regexp.Regexp, 0, len(c.APIGroupFilters.Include))
		for _, pattern := range c.APIGroupFilters.Include {
			regex, err := compilePattern(pattern)
			if err != nil {
				return fmt.Errorf("invalid include pattern %q: %w", pattern, err)
			}
			c.APIGroupFilters.includeRegexes = append(c.APIGroupFilters.includeRegexes, regex)
		}

		// Pre-compile and cache exclude patterns for performance
		c.APIGroupFilters.excludeRegexes = make([]*regexp.Regexp, 0, len(c.APIGroupFilters.Exclude))
		for _, pattern := range c.APIGroupFilters.Exclude {
			regex, err := compilePattern(pattern)
			if err != nil {
				return fmt.Errorf("invalid exclude pattern %q: %w", pattern, err)
			}
			c.APIGroupFilters.excludeRegexes = append(c.APIGroupFilters.excludeRegexes, regex)
		}
	}

	return nil
}

func (c *Config) getDynamicClient() (dynamic.Interface, error) {
	return MakeDynamicClient(c.APIConfig)
}

// shouldWatchAPIGroup determines if a CRD's API group should be watched based on filters
func (c *Config) shouldWatchAPIGroup(apiGroup string) bool {
	if c.DiscoveryMode == DiscoveryModeAll {
		return true
	}

	// Check include patterns using cached compiled regexes
	included := false
	for _, regex := range c.APIGroupFilters.includeRegexes {
		if regex.MatchString(apiGroup) {
			included = true
			break
		}
	}

	if !included {
		return false
	}

	// Check exclude patterns using cached compiled regexes
	for _, regex := range c.APIGroupFilters.excludeRegexes {
		if regex.MatchString(apiGroup) {
			return false
		}
	}

	return true
}

// compilePattern converts a glob-style pattern to a regex
func compilePattern(pattern string) (*regexp.Regexp, error) {
	// Escape regex special chars except *
	escaped := regexp.QuoteMeta(pattern)

	// Replace escaped \* with .*
	regexPattern := "^" + strings.ReplaceAll(escaped, `\*`, `.*`) + "$"

	return regexp.Compile(regexPattern)
}
