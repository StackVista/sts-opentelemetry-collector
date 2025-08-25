//go:build integration

package file_test

import (
	"context"
	"fmt"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettings "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/config"
	stsSettingsFile "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal/provider/file"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap/zaptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestFileSettingsProvider_LoadsInitialSettings(t *testing.T) {
	_, cancel, provider, _ := setupFileProvider(t, 100*time.Millisecond)
	defer cancel()

	currentSettings, err := stsSettings.GetSettingsAs[stsSettingsModel.OtelComponentMapping](provider, stsSettingsModel.SettingTypeOtelComponentMapping)
	require.NoError(t, err)
	assert.Len(t, currentSettings, 1, "should have one OtelComponentMapping")
}

func TestFileSettingsProvider_DetectsFileChanges(t *testing.T) {
	_, cancel, provider, tempFilePath := setupFileProvider(t, 50*time.Millisecond)
	defer cancel()

	// Modify the file to trigger an update
	content, err := os.ReadFile(tempFilePath)
	require.NoError(t, err)
	newMapping := newOtelComponentMapping("111")
	addMappingToFile(t, tempFilePath, content, newMapping)

	// Verify the state after the update
	// The file-based provider polls the settings file at a fixed interval.
	// Even though we write the file atomically, the provider may not see the update immediately.
	// require.Eventually waits until the provider reads the updated file and updates its cache.
	require.Eventually(t, func() bool {
		current, err := stsSettings.GetSettingsAs[stsSettingsModel.OtelComponentMapping](
			provider, stsSettingsModel.SettingTypeOtelComponentMapping)
		if err != nil {
			return false
		}
		return len(current) == 2 && settingExists(current, "111")
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func setupFileProvider(t *testing.T, updateInterval time.Duration) (ctx context.Context, cancel context.CancelFunc, provider *stsSettingsFile.SettingsProvider, tempFilePath string) {
	t.Helper()

	originalFilePath := filepath.Join("./testdata", "settings.yaml")

	tempDir, err := os.MkdirTemp("", "file_provider_test")
	require.NoError(t, err)

	tempFilePath = filepath.Join(tempDir, fmt.Sprintf("temp_settings_%d.yaml", time.Now().UnixNano()))

	content, err := os.ReadFile(originalFilePath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(tempFilePath, content, 0644))

	cfg := &stsSettingsConfig.FileSettingsProviderConfig{
		Path:           tempFilePath,
		UpdateInterval: updateInterval,
	}
	logger := zaptest.NewLogger(t)
	provider, err = stsSettingsFile.NewFileSettingsProvider(cfg, logger)
	require.NoError(t, err)

	ctx, cancel = context.WithCancel(context.Background())
	require.NoError(t, provider.Start(ctx, componenttest.NewNopHost()))
	time.Sleep(100 * time.Millisecond)

	return ctx, cancel, provider, tempFilePath
}

func addMappingToFile(t *testing.T, filePath string, originalContent []byte, newMapping stsSettingsModel.OtelComponentMapping) {
	t.Helper()

	var existingMappings []stsSettingsModel.OtelComponentMapping
	err := yaml.Unmarshal(originalContent, &existingMappings)
	require.NoError(t, err, "failed to unmarshal existing mappings")

	updatedMappings := append(existingMappings, newMapping)
	updatedContent, err := yaml.Marshal(updatedMappings)
	require.NoError(t, err, "failed to marshal updated mappings")
	// write content to file
	require.NoError(t, os.WriteFile(filePath, updatedContent, 0644), "failed to write updated settings file")
	t.Log("file at path", filePath, "updated with new mapping")
}

func settingExists(settings []stsSettingsModel.OtelComponentMapping, expectedId string) bool {
	found := false
	for _, setting := range settings {
		if expectedId == setting.Id {
			found = true
			break
		}
	}
	return found
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
