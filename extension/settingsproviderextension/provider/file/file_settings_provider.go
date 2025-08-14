package file

import (
	"context"
	"encoding/json"
	"errors"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal"
	stsProviderCommon "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/provider/common"
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
	currentSettings map[stsSettingsModel.SettingType][]stsSettingsModel.Setting

	// Channel to notify clients of updates.
	updateChannel chan struct{}
}

func NewFileSettingsProvider(cfg *stsSettingsConfig.FileSettingsProviderConfig, logger *zap.Logger) (*fileSettingsProvider, error) {
	provider := &fileSettingsProvider{
		cfg:             cfg,
		logger:          logger,
		updateChannel:   make(chan struct{}, 1),
		currentSettings: make(map[stsSettingsModel.SettingType][]stsSettingsModel.Setting),
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

// GetCurrentSettings provides a thread-safe way to access the latest settings.
func (f *fileSettingsProvider) GetCurrentSettings() map[stsSettingsModel.SettingType][]stsSettingsModel.Setting {
	f.settingsLock.RLock()
	defer f.settingsLock.RUnlock()
	// Return a deep copy to prevent external modification of the internal state.
	copiedSettings := make(map[stsSettingsModel.SettingType][]stsSettingsModel.Setting, len(f.currentSettings))
	for key, valueSlice := range f.currentSettings {
		copiedSlice := make([]stsSettingsModel.Setting, len(valueSlice))
		copy(copiedSlice, valueSlice)
		copiedSettings[key] = copiedSlice
	}
	return copiedSettings

}

func (f *fileSettingsProvider) loadSettings() error {
	fileContent, err := f.readSettingsFile()
	if err != nil {
		return err
	}
	settingsMap, err := f.parseSettings(fileContent)
	if err != nil {
		return err
	}

	f.settingsLock.Lock()
	f.currentSettings = settingsMap
	f.settingsLock.Unlock()

	return nil
}

// checkAndUpdateSettings checks for file changes and updates the state.
func (f *fileSettingsProvider) checkAndUpdateSettings() {
	f.logger.Debug("Checking for settings file updates.")

	fileContent, err := f.readSettingsFile()
	if err != nil {
		f.logger.Error("Failed to read settings file.", zap.Error(err))
		return
	}

	newSettingsMap, err := f.parseSettings(fileContent)
	if err != nil {
		f.logger.Error("Failed to read or parse settings file.", zap.Error(err))
		return
	}

	f.settingsLock.RLock()
	isDifferent := len(newSettingsMap) != len(f.currentSettings) || !mapsAreEqual(newSettingsMap, f.currentSettings)
	f.logger.Debug("Current settings size", zap.Int("size", len(f.currentSettings)))
	if isDifferent {
		f.logger.Debug("New settings detected, updating in-memory cache")
	}
	f.settingsLock.RUnlock()

	if isDifferent {
		f.settingsLock.Lock()
		f.currentSettings = newSettingsMap
		f.settingsLock.Unlock()

		select {
		case f.updateChannel <- struct{}{}:
			f.logger.Info("New settings loaded and change signal sent.")
		default:
		}
	}
}

// parseSettings is a private helper method to parse the file content.
func (f *fileSettingsProvider) parseSettings(fileContent []byte) (map[stsSettingsModel.SettingType][]stsSettingsModel.Setting, error) {
	if len(fileContent) == 0 {
		return nil, errors.New("file content is empty")
	}

	var rawSettings []map[string]interface{}
	if err := yaml.Unmarshal(fileContent, &rawSettings); err != nil {
		return nil, err
	}

	var errs []error
	settingsMap := make(map[stsSettingsModel.SettingType][]stsSettingsModel.Setting)
	for _, setting := range rawSettings {
		jsonBytes, err := json.Marshal(setting)
		if err != nil {
			errs = append(errs, err)
			f.logger.Error("Failed to re-marshal setting to JSON during processing, skipping.",
				zap.Error(err),
				zap.Any("raw_setting", setting))
			continue
		}

		var settingModel stsSettingsModel.Setting
		if err := settingModel.UnmarshalJSON(jsonBytes); err != nil {
			errs = append(errs, err)
			f.logger.Error("Failed to parse setting from file content.",
				zap.Error(err),
				zap.ByteString("json_bytes", jsonBytes))
			continue
		}

		settingType, err := stsProviderCommon.GetSettingType(settingModel)
		if err != nil {
			errs = append(errs, err)
			f.logger.Error("Failed to get setting type.",
				zap.Error(err),
				zap.Any("setting", settingModel))
			continue
		}

		settingsMap[settingType] = append(settingsMap[settingType], settingModel)
	}
	return settingsMap, errors.Join(errs...)

}

// readSettingsFile is a private helper method to read the file content from disk.
func (f *fileSettingsProvider) readSettingsFile() ([]byte, error) {
	return os.ReadFile(f.cfg.Path)
}

func mapsAreEqual(a, b map[stsSettingsModel.SettingType][]stsSettingsModel.Setting) bool {
	return reflect.DeepEqual(a, b)
}
