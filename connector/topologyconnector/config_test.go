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
				ExpressionCacheSettings: CacheSettings{
					Size: 1000,
					TTL:  10 * time.Minute,
				},
			},
		},
		{
			name: "apply default size if not set",
			cfg: &Config{
				ExpressionCacheSettings: CacheSettings{
					Size: 0,
					TTL:  10 * time.Minute,
				},
			},
			wantSize: defaultCacheSize,
		},
		{
			name: "return error if size exceeds max",
			cfg: &Config{
				ExpressionCacheSettings: CacheSettings{
					Size: maxCacheSize + 1,
					TTL:  10 * time.Minute,
				},
			},
			wantErr: fmt.Sprintf("expression_cache.size: must not exceed %d", maxCacheSize),
		},
		{
			name: "allow ttl = 0 (no expiration)",
			cfg: &Config{
				ExpressionCacheSettings: CacheSettings{
					Size: 100,
					TTL:  0,
				},
			},
		},
		{
			name: "return error if ttl is too small",
			cfg: &Config{
				ExpressionCacheSettings: CacheSettings{
					Size: 100,
					TTL:  50 * time.Nanosecond, // <= minValidTTL
				},
			},
			wantErr: fmt.Sprintf("expression_cache.ttl: must be greater than %s to avoid internal timer issues", minValidTTL),
		},
		{
			name: "return error if ttl is negative",
			cfg: &Config{
				ExpressionCacheSettings: CacheSettings{
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
				assert.Equal(t, tt.wantSize, tt.cfg.ExpressionCacheSettings.Size)
			}
			if tt.wantTTLMin > 0 {
				assert.Equal(t, tt.wantTTLMin, tt.cfg.ExpressionCacheSettings.TTL)
			}
		})
	}
}
