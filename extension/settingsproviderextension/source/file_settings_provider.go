package source

import (
	"context"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal"
	"os"
	"reflect"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
)

type fileSettingsProvider struct {
	cfg    *stsSettingsConfig.FileSettingsProviderConfig
	logger *zap.Logger

	// The current state of the stsSettingsModel. Using a map for potential multiple mappings.
	settingsLock    sync.RWMutex
	currentSettings map[stsSettingsModel.SettingId]stsSettingsModel.OtelComponentMapping

	// Channel to notify clients of updates.
	updateChannel chan struct{}
}

func NewFileSettingsProvider(cfg *stsSettingsConfig.FileSettingsProviderConfig, logger *zap.Logger) (*fileSettingsProvider, error) {
	provider := &fileSettingsProvider{
		cfg:             cfg,
		logger:          logger,
		updateChannel:   make(chan struct{}, 1),
		currentSettings: make(map[stsSettingsModel.SettingId]stsSettingsModel.OtelComponentMapping),
	}

	// Perform an initial load of the configuration file.
	if err := provider.loadSettings(); err != nil {
		return nil, err
	}

	return provider, nil
}

// Start initiates the file watching goroutine.
func (f *fileSettingsProvider) Start(ctx context.Context, host component.Host) error {
	f.logger.Info("Starting file-based settings provider.", zap.String("path", f.cfg.Path))

	go func() {
		ticker := time.NewTicker(f.cfg.UpdateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				f.logger.Info("File provider context done, shutting down.")
				return
			case <-ticker.C:
				f.checkAndUpdateSettings()
			}
		}
	}()
	return nil
}

// Shutdown stops the provider.
func (f *fileSettingsProvider) Shutdown(ctx context.Context) error {
	f.logger.Info("Shutting down file-based settings provider.")
	return nil
}

// RegisterForUpdates returns a channel for receiving change notifications.
func (f *fileSettingsProvider) RegisterForUpdates() <-chan struct{} {
	return f.updateChannel
}

func (f *fileSettingsProvider) loadSettings() error {
	mappingMap, err := f.readAndParseMappings()
	if err != nil {
		return err
	}

	f.settingsLock.Lock()
	f.currentSettings = mappingMap
	f.settingsLock.Unlock()

	return nil
}

// checkAndUpdateSettings checks for file changes and updates the state.
func (f *fileSettingsProvider) checkAndUpdateSettings() {
	f.logger.Debug("Checking for settings file updates.")

	newMappingMap, err := f.readAndParseMappings()
	if err != nil {
		f.logger.Error("Failed to read or parse settings file.", zap.Error(err))
		return
	}

	f.settingsLock.RLock()
	isDifferent := len(newMappingMap) != len(f.currentSettings) || !mapsAreEqual(newMappingMap, f.currentSettings)
	f.logger.Debug("Current settings size", zap.Int("size", len(f.currentSettings)))
	if isDifferent {
		f.logger.Debug("New mappings detected, updating in-memory cache")
	}
	f.settingsLock.RUnlock()

	if isDifferent {
		f.settingsLock.Lock()
		f.currentSettings = newMappingMap
		f.settingsLock.Unlock()

		select {
		case f.updateChannel <- struct{}{}:
			f.logger.Info("New settings loaded and change signal sent.")
		default:
		}
	}
}

// readAndParseMappings is a private helper method to read the file and parse the content.
func (f *fileSettingsProvider) readAndParseMappings() (map[stsSettingsModel.SettingId]stsSettingsModel.OtelComponentMapping, error) {
	fileContent, err := os.ReadFile(f.cfg.Path)
	if err != nil {
		return nil, err
	}

	var newMappings []stsSettingsModel.OtelComponentMapping
	if err := yaml.Unmarshal(fileContent, &newMappings); err != nil {
		return nil, err
	}

	mappingMap := make(map[stsSettingsModel.SettingId]stsSettingsModel.OtelComponentMapping)
	for _, mapping := range newMappings {
		mappingMap[mapping.Id] = mapping
	}
	return mappingMap, nil
}

func mapsAreEqual(a, b map[stsSettingsModel.SettingId]stsSettingsModel.OtelComponentMapping) bool {
	if len(a) != len(b) {
		return false
	}

	return reflect.DeepEqual(a, b)
}

// GetCurrentSettings provides a thread-safe way to access the latest settings.
func (f *fileSettingsProvider) GetCurrentSettings() map[stsSettingsModel.SettingId]stsSettingsModel.OtelComponentMapping {
	f.settingsLock.RLock()
	defer f.settingsLock.RUnlock()
	// Return a copy to prevent external modification of the internal state.
	copiedSettings := make(map[stsSettingsModel.SettingId]stsSettingsModel.OtelComponentMapping, len(f.currentSettings))
	for k, v := range f.currentSettings {
		copiedSettings[k] = v
	}
	return copiedSettings
}
