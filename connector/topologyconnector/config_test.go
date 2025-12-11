//nolint:testpackage
package topologyconnector

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name       string
		cfg        *Config
		wantErr    string
		wantSize   int
		wantTTLMin time.Duration
	}{
		{
			name: "valid config",
			cfg: &Config{
				ExpressionCache: CacheSettings{
					Size: 1000,
					TTL:  10 * time.Minute,
				},
			},
		},
		{
			name: "apply default size if not set",
			cfg: &Config{
				ExpressionCache: CacheSettings{
					Size: 0,
					TTL:  10 * time.Minute,
				},
			},
			wantSize: defaultCacheSize,
		},
		{
			name: "return error if size exceeds max",
			cfg: &Config{
				ExpressionCache: CacheSettings{
					Size: maxCacheSize + 1,
					TTL:  10 * time.Minute,
				},
			},
			wantErr: fmt.Sprintf("expression_cache.size: must not exceed %d", maxCacheSize),
		},
		{
			name: "allow ttl = 0 (no expiration)",
			cfg: &Config{
				ExpressionCache: CacheSettings{
					Size: 100,
					TTL:  0,
				},
			},
		},
		{
			name: "return error if ttl is too small",
			cfg: &Config{
				ExpressionCache: CacheSettings{
					Size: 100,
					TTL:  50 * time.Nanosecond, // <= minValidTTL
				},
			},
			wantErr: fmt.Sprintf("expression_cache.ttl: must be greater than %s to avoid internal timer issues", minValidTTL),
		},
		{
			name: "return error if ttl is negative",
			cfg: &Config{
				ExpressionCache: CacheSettings{
					Size: 100,
					TTL:  -1 * time.Second,
				},
			},
			wantErr: "expression_cache.ttl: must not be negative (0 disables expiration)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.EqualError(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)

			if tt.wantSize > 0 {
				assert.Equal(t, tt.wantSize, tt.cfg.ExpressionCache.Size)
			}
			if tt.wantTTLMin > 0 {
				assert.Equal(t, tt.wantTTLMin, tt.cfg.ExpressionCache.TTL)
			}
		})
	}
}

func TestDeduplicationSettings_Validate(t *testing.T) {
	tests := []struct {
		name                string
		dedup               DeduplicationSettings
		wantErr             string
		wantRefreshFraction float64
		wantCacheSize       int
	}{
		{
			name: "valid deduplication settings",
			dedup: DeduplicationSettings{
				Enabled:         true,
				RefreshFraction: 0.4,
				Cache: CacheSettings{
					Size: 100,
					TTL:  5 * time.Minute,
				},
			},
			wantRefreshFraction: 0.4,
		},
		{
			name: "refresh_fraction < 0 defaults to 0.5",
			dedup: DeduplicationSettings{
				RefreshFraction: -1,
				Cache: CacheSettings{
					Size: 100,
					TTL:  1 * time.Minute,
				},
			},
			wantRefreshFraction: 0.5,
		},
		{
			name: "refresh_fraction == 0 disables refresh",
			dedup: DeduplicationSettings{
				RefreshFraction: 0,
				Cache: CacheSettings{
					Size: 100,
					TTL:  1 * time.Minute,
				},
			},
			wantRefreshFraction: 0,
		},
		{
			name: "refresh_fraction > 1 clamps to 1",
			dedup: DeduplicationSettings{
				RefreshFraction: 1.5,
				Cache: CacheSettings{
					Size: 100,
					TTL:  1 * time.Minute,
				},
			},
			wantRefreshFraction: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.dedup.Validate()

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.EqualError(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)

			if tt.wantRefreshFraction > 0 {
				assert.Equal(t, tt.wantRefreshFraction, tt.dedup.RefreshFraction)
			}
			if tt.wantCacheSize > 0 {
				assert.Equal(t, tt.wantCacheSize, tt.dedup.Cache.Size)
			}
		})
	}
}
