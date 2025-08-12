package source

import (
	"context"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"go.opentelemetry.io/collector/component/componenttest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal"
)

// TestNewFileProvider verifies the constructor's behavior.
func TestNewFileProvider(t *testing.T) {
	// Test case 1: Successful initialization.
	t.Run("success", func(t *testing.T) {
		cfg := &stsSettingsConfig.FileSourceConfig{
			Path: filepath.Join("../testdata", "otel_mappings.yaml"),
		}
		provider, err := NewFileProvider(cfg, zap.NewNop())
		require.NoError(t, err)
		assert.NotNil(t, provider)
		assert.Equal(t, 1, len(provider.GetCurrentSettings()))
	})

	// Test case 2: File not found error.
	t.Run("file_not_found", func(t *testing.T) {
		cfg := &stsSettingsConfig.FileSourceConfig{
			Path: "/non-existent/file",
		}
		provider, err := NewFileProvider(cfg, zap.NewNop())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no such file or directory")
		assert.Nil(t, provider)
	})

	// Test case 3: Malformed YAML.
	t.Run("malformed_yaml", func(t *testing.T) {
		cfg := &stsSettingsConfig.FileSourceConfig{
			Path: filepath.Join("../testdata", "otel_mappings_malformed.yaml"),
		}
		provider, err := NewFileProvider(cfg, zap.NewNop())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "yaml: line 3")
		assert.Nil(t, provider)
	})
}

// TestFileProvider_StartAndShutdown verifies the lifecycle and change detection.
func TestFileProvider_StartAndShutdown(t *testing.T) {
	originalFilePath := filepath.Join("../testdata", "otel_mappings.yaml")
	tempDir := t.TempDir()

	// Create a temporary file by copying the original test data file's content.
	tempFilePath := filepath.Join(tempDir, "temp_mappings.yaml")
	content, err := os.ReadFile(originalFilePath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(tempFilePath, content, 0644))

	cfg := &stsSettingsConfig.FileSourceConfig{
		Path:           tempFilePath,
		UpdateInterval: 100 * time.Millisecond,
	}
	provider, err := NewFileProvider(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the provider.
	require.NoError(t, provider.Start(ctx, componenttest.NewNopHost()))

	// Test initial settings.
	assert.Equal(t, 1, len(provider.GetCurrentSettings()))

	// Listen for updates.
	updates := provider.RegisterForUpdates()

	// Wait for an update after a change.
	var settingsUpdateSignalReceived bool
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-updates:
			t.Log("Update received.")
			settingsUpdateSignalReceived = true
		case <-time.After(500 * time.Millisecond):
			t.Error("Timed out waiting for update.")
		}
	}()

	// Change the file content.
	newMapping := stsSettingsModel.OtelComponentMapping{Id: "new-mapping", CreatedTimeStamp: 2}
	var existingMappings []stsSettingsModel.OtelComponentMapping
	err = yaml.Unmarshal(content, &existingMappings)
	require.NoError(t, err)
	existingMappings = append(existingMappings, newMapping)
	updatedContent, err := yaml.Marshal(existingMappings)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(tempFilePath, updatedContent, 0644))

	wg.Wait()
	cancel()

	// Verify the new settings.
	assert.True(t, settingsUpdateSignalReceived)
	assert.Equal(t, 2, len(provider.GetCurrentSettings()))
	_, found := provider.GetCurrentSettings()["new-mapping"]
	assert.True(t, found)
}

// TestMapsAreEqual verifies the deep comparison logic.
func TestMapsAreEqual(t *testing.T) {
	mapA := map[stsSettingsModel.SettingId]stsSettingsModel.OtelComponentMapping{
		"1": {Id: "1", Name: "test", CreatedTimeStamp: 1},
	}
	mapB := map[stsSettingsModel.SettingId]stsSettingsModel.OtelComponentMapping{
		"1": {Id: "1", Name: "test", CreatedTimeStamp: 1},
	}
	mapC := map[stsSettingsModel.SettingId]stsSettingsModel.OtelComponentMapping{
		"1": {Id: "1", Name: "diff", CreatedTimeStamp: 2},
	}
	mapD := map[stsSettingsModel.SettingId]stsSettingsModel.OtelComponentMapping{
		"2": {Id: "2", Name: "test", CreatedTimeStamp: 1},
	}

	assert.True(t, mapsAreEqual(mapA, mapB))
	assert.False(t, mapsAreEqual(mapA, mapC))
	assert.False(t, mapsAreEqual(mapA, mapD))
}
