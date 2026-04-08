package k8scrdreceiver

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"k8s.io/client-go/dynamic"
)

type DiscoveryMode string

const (
	DiscoveryModeAPIGroups DiscoveryMode = "api_groups"
	DiscoveryModeAll       DiscoveryMode = "all"
)

// APIGroupFilters defines inclusion and exclusion patterns for API groups
type APIGroupFilters struct {
	// Include defines patterns to include (regex). Default: ["*"] (all groups)
	Include []string `mapstructure:"include"`

	// Exclude defines patterns to exclude (regex). Applied after include.
	Exclude []string `mapstructure:"exclude"`

	// Compiled regex caches (not exposed in config)
	includeRegexes []*regexp.Regexp
	excludeRegexes []*regexp.Regexp
}

// Config defines configuration for the k8scrd receiver
type Config struct {
	APIConfig           APIConfig        `mapstructure:",squash"`
	DiscoveryMode       DiscoveryMode    `mapstructure:"discovery_mode"`
	APIGroupFilters     *APIGroupFilters `mapstructure:"api_group_filters"`
	IncludeInitialState bool             `mapstructure:"include_initial_state"`
}

func (c *Config) Validate() error {
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

// shouldWatch determines if a CRD's API group should be watched based on filters
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

// matchesPattern checks if a string matches a glob-style pattern
// Supports:
//   - "*" matches anything
//   - "*.example.com" matches "foo.example.com" but not "example.com"
//   - Exact match: "policies.kubewarden.io"
//
// Note: This function compiles patterns on every call and is only used for testing.
// Production code uses pre-compiled regexes cached in Config.APIGroupFilters.
func matchesPattern(pattern, str string) bool {
	// Simple glob support
	if pattern == "*" {
		return true
	}

	regex, err := compilePattern(pattern)
	if err != nil {
		return false
	}

	return regex.MatchString(str)
}

// compilePattern converts a glob-style pattern to a regex
func compilePattern(pattern string) (*regexp.Regexp, error) {
	// Escape regex special chars except *
	escaped := regexp.QuoteMeta(pattern)

	// Replace escaped \* with .*
	regexPattern := "^" + strings.ReplaceAll(escaped, `\*`, `.*`) + "$"

	return regexp.Compile(regexPattern)
}
