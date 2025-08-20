//go:build integration

package file_test

import (
	"context"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettings "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/config"
	stsSettingsFile "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/provider/file"
	"go.opentelemetry.io/collector/component/componenttest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// TestFileSettingsProvider_StartAndShutdown verifies the lifecycle and change detection.
func TestFileSettingsProvider_StartAndShutdown(t *testing.T) {
	originalFilePath := filepath.Join("./testdata", "settings.yaml")
	tempDir := t.TempDir()
	tempFilePath := filepath.Join(tempDir, "temp_settings.yaml")

	content, err := os.ReadFile(originalFilePath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(tempFilePath, content, 0644))

	cfg := &stsSettingsConfig.FileSettingsProviderConfig{
		Path:           tempFilePath,
		UpdateInterval: 100 * time.Millisecond,
	}
	logger, _ := zap.NewDevelopment()
	provider, err := stsSettingsFile.NewFileSettingsProvider(cfg, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Start and Initial Verification ---
	t.Run("loads initial settings on start", func(t *testing.T) {
		require.NoError(t, provider.Start(ctx, componenttest.NewNopHost()))

		currentSettings, err := stsSettings.GetSettingsAs[stsSettingsModel.OtelComponentMapping](provider, stsSettingsModel.SettingTypeOtelComponentMapping)
		require.NoError(t, err)
		assert.Len(t, currentSettings, 1, "should have one OtelComponentMapping")
	})

	// --- File Update Verification ---
	t.Run("detects and applies file changes", func(t *testing.T) {
		updates := provider.RegisterForUpdates()

		// Modify the file to trigger an update
		newMapping := newOtelComponentMapping("111")
		addMappingToFile(t, tempFilePath, content, newMapping)

		// Wait for the provider to signal an update
		select {
		case <-updates:
			t.Log("Update signal received.")
		case <-time.After(1 * time.Second): // Generous timeout
			t.Fatal("Timed out waiting for settings update signal.")
		}

		// Verify the state after the update
		currentSettings, err := stsSettings.GetSettingsAs[stsSettingsModel.OtelComponentMapping](provider, stsSettingsModel.SettingTypeOtelComponentMapping)
		require.NoError(t, err)
		assert.Len(t, currentSettings, 2, "should have two mappings after update")
		assertSettingExists(t, currentSettings, "111")
	})
}

func addMappingToFile(t *testing.T, filePath string, originalContent []byte, newMapping stsSettingsModel.OtelComponentMapping) {
	t.Helper()

	var existingMappings []stsSettingsModel.OtelComponentMapping
	err := yaml.Unmarshal(originalContent, &existingMappings)
	require.NoError(t, err, "failed to unmarshal existing mappings")

	updatedMappings := append(existingMappings, newMapping)

	updatedContent, err := yaml.Marshal(updatedMappings)
	require.NoError(t, err, "failed to marshal updated mappings")
	require.NoError(t, os.WriteFile(filePath, updatedContent, 0644), "failed to write updated settings file")
}

func assertSettingExists(t *testing.T, settings []stsSettingsModel.OtelComponentMapping, expectedId string) {
	t.Helper()

	found := false
	for _, setting := range settings {
		if expectedId == setting.Id {
			found = true
			break
		}
	}
	assert.True(t, found, "setting with id '%s' should exist but was not found", expectedId)
}

func newOtelComponentMapping(id string) stsSettingsModel.OtelComponentMapping {
	return stsSettingsModel.OtelComponentMapping{
		Id:               id,
		CreatedTimeStamp: 2,
		Shard:            0,
		Type:             "OtelComponentMapping",
		Output: stsSettingsModel.OtelComponentMappingOutput{
			DomainIdentifier: newOtelStringExpression("host"),
			DomainName:       *newOtelStringExpression("domain"),
			Identifier:       *newOtelStringExpression("${input.attributes['host.name']}"),
			LayerName:        *newOtelStringExpression("Infrastructure"),
			Name:             *newOtelStringExpression("${input.attributes['service.name']}"),
			TypeName:         *newOtelStringExpression("host-component-type"),
		},
	}
}

func newOtelStringExpression(expr string) *stsSettingsModel.OtelStringExpression {
	return &stsSettingsModel.OtelStringExpression{
		Expression: expr,
	}
}
