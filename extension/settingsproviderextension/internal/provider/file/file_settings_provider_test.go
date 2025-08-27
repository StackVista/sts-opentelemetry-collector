package file

import (
	"context"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/config"
	stsSettingsCommon "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"path/filepath"
	"testing"
	"time"
)

func TestFileSettingsProvider_NewFileSettingsProvider(t *testing.T) {
	// Test case 1: Successful initialization.
	t.Run("success", func(t *testing.T) {
		cfg := &stsSettingsConfig.FileSettingsProviderConfig{
			Path: filepath.Join("./testdata", "settings.yaml"),
		}
		provider, err := NewFileSettingsProvider(cfg, zaptest.NewLogger(t))
		require.NoError(t, err)
		assert.NotNil(t, provider)
		settings, err := provider.UnsafeGetCurrentSettingsByType(stsSettingsModel.SettingTypeOtelComponentMapping)
		require.NoError(t, err)
		assert.Equal(t, 1, len(settings))
	})

	// Test case 2: File not found error.
	t.Run("file_not_found", func(t *testing.T) {
		cfg := &stsSettingsConfig.FileSettingsProviderConfig{
			Path: "/non-existent/file",
		}
		provider, err := NewFileSettingsProvider(cfg, zaptest.NewLogger(t))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no such file or directory")
		assert.Nil(t, provider)
	})

	// Test case 3: Malformed YAML.
	t.Run("malformed_yaml", func(t *testing.T) {
		cfg := &stsSettingsConfig.FileSettingsProviderConfig{
			Path: filepath.Join("./testdata", "settings_malformed.yaml"),
		}
		provider, err := NewFileSettingsProvider(cfg, zaptest.NewLogger(t))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "yaml: line 3")
		assert.Nil(t, provider)
	})
}

// TestParseSettings focuses on the YAML parsing and transformation logic.
func TestFileSettingsProvider_ParseSettings(t *testing.T) {
	provider := &SettingsProvider{
		cfg:    &stsSettingsConfig.FileSettingsProviderConfig{Path: "/dev/null"},
		logger: zaptest.NewLogger(t),
	}

	// Define test cases
	testCases := []struct {
		name                 string
		fileContent          []byte
		expectParseError     bool
		expectCombinedError  bool
		expectedMappingCount int
		expectedID           string
	}{
		{
			name: "Valid YAML with one OtelComponentMapping",
			fileContent: []byte(`
- createdTimeStamp: 1723485600
  id: "1122334455"
  name: "Host Component"
  shard: 0
  type: "OtelComponentMapping"
  output:
    domainIdentifier:
      expression: "host"
    domainName:
      expression: "my-custom-domain"
    identifier:
      expression: "${input.attributes['host.name']}"
    layerName:
      expression: "Infrastructure"
    name:
      expression: "${input.attributes['service.name']}"
    typeName:
      expression: "host-component-type"
`),
			expectParseError:     false,
			expectedMappingCount: 1,
			expectedID:           "1122334455",
		},
		{
			name: "Malformed YAML",
			fileContent: []byte(`- id: 
"bad""
`),
			expectParseError: true,
		},
		{
			name:             "Empty file content",
			fileContent:      []byte(""),
			expectParseError: true,
		},
		{
			name: "Setting with unknown type",
			fileContent: []byte(`
- id: "def-456"
  type: "SomeUnknownType"
`),
			expectCombinedError:  true, // Expect an error because the type is unknown
			expectedMappingCount: 0,    // The setting should be logged and skipped, not cause a parse error, but an error nonetheless because no settings were parsed
		},
		{
			name: "Partial success with one valid and one invalid setting",
			fileContent: []byte(`
- createdTimeStamp: 1723485600
  id: "1122334455"
  name: "Host Component"
  shard: 0
  type: "OtelComponentMapping"
  output:
    domainIdentifier:
      expression: "host"
    domainName:
      expression: "my-custom-domain"
    identifier:
      expression: "${input.attributes['host.name']}"
    layerName:
      expression: "Infrastructure"
    name:
      expression: "${input.attributes['service.name']}"
    typeName:
      expression: "host-component-type"
- id: "bad-one"
  type: "OtelComponentMapping"
  output: 123 # This is an invalid type for the 'output' field
`),
			expectCombinedError:  true, // Expect an error from the collection
			expectedMappingCount: 1,    // But still expect the valid one to be parsed
			expectedID:           "1122334455",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			settingsMap, err := provider.parseSettings(tc.fileContent)

			if tc.expectParseError || tc.expectCombinedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// This block runs for all cases except a total parse failure
			if !tc.expectParseError {
				otelMappings, ok := settingsMap[stsSettingsModel.SettingTypeOtelComponentMapping]
				assert.True(t, ok || tc.expectedMappingCount == 0)
				assert.Len(t, otelMappings, tc.expectedMappingCount)

				if tc.expectedMappingCount > 0 {
					require.Len(t, otelMappings, 1)
					settingId, err := stsSettingsCommon.GetSettingId(otelMappings[0].Raw)
					require.NoError(t, err)
					assert.Equal(t, tc.expectedID, settingId)
				}
			}
		})
	}
}

func TestFileSettingsProvider_Shutdown(t *testing.T) {
	logger := zaptest.NewLogger(t)
	provider := &SettingsProvider{
		cfg: &stsSettingsConfig.FileSettingsProviderConfig{
			Path:           "/dev/null",
			UpdateInterval: 10 * time.Millisecond,
		},
		logger: logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start provider (spins up goroutine with ticker)
	err := provider.Start(ctx, nil)
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond) // settle time

	// Shutdown with a timeout context
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()

	err = provider.Shutdown(shutdownCtx)
	require.NoError(t, err, "expected clean shutdown without timeout")
}
