//nolint:testpackage // Tests require access to internal functions
package k8scrdreceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config with api_groups mode",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				APIGroupFilters: &APIGroupFilters{
					Include: []string{"*"},
					Exclude: []string{},
				},
			},
			wantErr: false,
		},
		{
			name: "valid config with all mode",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
			},
			wantErr: false,
		},
		{
			name: "valid config with specific API groups",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				APIGroupFilters: &APIGroupFilters{
					Include: []string{"policies.kubewarden.io", "*.suse.com"},
					Exclude: []string{"internal.suse.com"},
				},
			},
			wantErr: false,
		},
		{
			name: "empty include patterns",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				APIGroupFilters: &APIGroupFilters{
					Include: []string{},
					Exclude: []string{},
				},
			},
			wantErr: true,
			errMsg:  "api_group_filters.include cannot be empty",
		},
		{
			name: "invalid discovery mode",
			config: &Config{
				DiscoveryMode: "invalid",
			},
			wantErr: true,
			errMsg:  "invalid discovery_mode",
		},
		// Note: glob patterns with special chars are escaped by QuoteMeta,
		// so they become literal matches and won't fail validation.
		// Regex compilation only fails on invalid constructed patterns.
		{
			name: "empty discovery mode defaults to api_groups",
			config: &Config{
				DiscoveryMode: "",
			},
			wantErr: false,
		},
		{
			name: "nil api_group_filters gets default",
			config: &Config{
				DiscoveryMode:   DiscoveryModeAPIGroups,
				APIGroupFilters: nil,
			},
			wantErr: false,
		},
		{
			name: "zero increment_interval gets default",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
			},
			wantErr: false,
		},
		{
			name: "increment_interval below 1s rejected",
			config: &Config{
				DiscoveryMode:     DiscoveryModeAll,
				IncrementInterval: 500 * time.Millisecond,
				SnapshotInterval:  5 * time.Minute,
			},
			wantErr: true,
			errMsg:  "increment_interval must be at least 1 second",
		},
		{
			name: "snapshot_interval below 1m rejected",
			config: &Config{
				DiscoveryMode:     DiscoveryModeAll,
				IncrementInterval: 10 * time.Second,
				SnapshotInterval:  30 * time.Second,
			},
			wantErr: true,
			errMsg:  "snapshot_interval must be at least 1 minute",
		},
		{
			name: "snapshot_interval below increment_interval rejected",
			config: &Config{
				DiscoveryMode:     DiscoveryModeAll,
				IncrementInterval: 10 * time.Minute,
				SnapshotInterval:  5 * time.Minute,
			},
			wantErr: true,
			errMsg:  "snapshot_interval must be greater than or equal to increment_interval",
		},
		{
			name: "snapshot_interval equal to increment_interval accepted",
			config: &Config{
				DiscoveryMode:     DiscoveryModeAll,
				IncrementInterval: 1 * time.Minute,
				SnapshotInterval:  1 * time.Minute,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestCompiledPatternMatches verifies that the glob-to-regex translation done by compilePattern
// produces the expected match/non-match behaviour. The test exercises compilePattern directly
// (instead of going through Config.shouldWatchAPIGroup) so each glob feature can be pinned down
// in isolation.
func TestCompiledPatternMatches(t *testing.T) {
	tests := []struct {
		pattern string
		str     string
		want    bool
	}{
		// Wildcard tests
		{"*", "anything", true},
		{"*", "example.com", true},
		{"*", "", true},

		// Subdomain wildcard tests
		{"*.example.com", "foo.example.com", true},
		{"*.example.com", "bar.example.com", true},
		{"*.example.com", "nested.foo.example.com", true},
		{"*.example.com", "example.com", false}, // Should NOT match bare domain

		// Exact match tests
		{"exact.match.io", "exact.match.io", true},
		{"exact.match.io", "not.exact.match.io", false},
		{"exact.match.io", "exact.match", false},

		// Multiple wildcard tests
		{"foo.*.bar", "foo.baz.bar", true},
		{"foo.*.bar", "foo.anything.bar", true},
		{"foo.*.bar", "foo.nested.path.bar", true},
		{"foo.*.bar", "foo.bar", false}, // * requires at least one char

		// Complex patterns
		{"policies.*.io", "policies.kubewarden.io", true},
		{"policies.*.io", "policies.example.io", true},
		{"policies.*.io", "policies.io", false},

		// Special characters in domain (should be escaped, not interpreted as regex)
		{"example.com", "exampleXcom", false}, // . should not match any char
		{"test-domain.io", "test-domain.io", true},
		{"test_domain.io", "test_domain.io", true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+" matches "+tt.str, func(t *testing.T) {
			regex, err := compilePattern(tt.pattern)
			require.NoError(t, err)
			got := regex.MatchString(tt.str)
			assert.Equal(t, tt.want, got, "pattern=%q str=%q", tt.pattern, tt.str)
		})
	}
}

func TestCompilePattern(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		wantErr bool
	}{
		{"wildcard", "*", false},
		{"subdomain wildcard", "*.example.com", false},
		{"exact match", "exact.match", false},
		{"multiple wildcards", "foo.*.bar.*", false},
		{"special chars", "test-domain_name.io", false},
		{"empty pattern", "", false},
		// Note: All glob patterns should compile successfully
		// Invalid regex would only come from malformed brackets, which QuoteMeta handles
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			regex, err := compilePattern(tt.pattern)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, regex)
			}
		})
	}
}

func TestShouldWatchAPIGroup(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		apiGroup string
		want     bool
	}{
		{
			name: "all mode watches everything",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
			},
			apiGroup: "anything.io",
			want:     true,
		},
		{
			name: "wildcard includes everything",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				APIGroupFilters: &APIGroupFilters{
					Include: []string{"*"},
				},
			},
			apiGroup: "example.com",
			want:     true,
		},
		{
			name: "exact match included",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				APIGroupFilters: &APIGroupFilters{
					Include: []string{"policies.kubewarden.io"},
				},
			},
			apiGroup: "policies.kubewarden.io",
			want:     true,
		},
		{
			name: "exact match not included",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				APIGroupFilters: &APIGroupFilters{
					Include: []string{"policies.kubewarden.io"},
				},
			},
			apiGroup: "other.io",
			want:     false,
		},
		{
			name: "subdomain wildcard match",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				APIGroupFilters: &APIGroupFilters{
					Include: []string{"*.suse.com"},
				},
			},
			apiGroup: "rancher.suse.com",
			want:     true,
		},
		{
			name: "excluded group not watched",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				APIGroupFilters: &APIGroupFilters{
					Include: []string{"*"},
					Exclude: []string{"internal.example.com"},
				},
			},
			apiGroup: "internal.example.com",
			want:     false,
		},
		{
			name: "excluded wildcard pattern",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				APIGroupFilters: &APIGroupFilters{
					Include: []string{"*.example.com"},
					Exclude: []string{"test.*.example.com"},
				},
			},
			apiGroup: "test.foo.example.com",
			want:     false,
		},
		{
			name: "multiple include patterns",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				APIGroupFilters: &APIGroupFilters{
					Include: []string{
						"policies.kubewarden.io",
						"longhorn.io",
						"*.suse.com",
					},
				},
			},
			apiGroup: "rancher.suse.com",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call Validate to compile and cache regex patterns
			err := tt.config.Validate()
			require.NoError(t, err)

			got := tt.config.shouldWatchAPIGroup(tt.apiGroup)
			assert.Equal(t, tt.want, got)
		})
	}
}
