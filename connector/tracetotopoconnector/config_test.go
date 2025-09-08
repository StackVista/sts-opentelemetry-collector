package tracetotopoconnector

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

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
				ExpressionCacheSettings: ExpressionCacheSettings{
					Size: 1000,
					TTL:  10 * time.Minute,
				},
			},
			wantErr: "",
		},
		{
			name: "apply default size if not set",
			cfg: &Config{
				ExpressionCacheSettings: ExpressionCacheSettings{
					Size: 0,
					TTL:  10 * time.Minute,
				},
			},
			wantErr:  "",
			wantSize: defaultCacheSize,
		},
		{
			name: "return error if size exceeds max",
			cfg: &Config{
				ExpressionCacheSettings: ExpressionCacheSettings{
					Size: maxCacheSize + 1,
					TTL:  10 * time.Minute,
				},
			},
			wantErr: fmt.Sprintf("expression_cache.size exceeds max allowed (%d)", maxCacheSize),
		},
		{
			name: "apply default ttl if not set",
			cfg: &Config{
				ExpressionCacheSettings: ExpressionCacheSettings{
					Size: 100,
					TTL:  0,
				},
			},
			wantErr:    "",
			wantTTLMin: defaultCacheTTL,
		},
		{
			name: "return error if ttl is too small",
			cfg: &Config{
				ExpressionCacheSettings: ExpressionCacheSettings{
					Size: 100,
					TTL:  50, // <= 100ns
				},
			},
			wantErr: "parameter expression_cache.ttl must be a positive value greater than 100ns",
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

			// Check size normalization
			if tt.wantSize > 0 {
				assert.Equal(t, tt.wantSize, tt.cfg.ExpressionCacheSettings.Size)
			}

			// Check ttl normalization
			if tt.wantTTLMin > 0 {
				assert.Equal(t, tt.wantTTLMin, tt.cfg.ExpressionCacheSettings.TTL)
			}
		})
	}
}
