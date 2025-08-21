package file

import (
	"context"
	"encoding/json"
	"errors"
	stsSettingsCommon "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/common"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/config"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"go.yaml.in/yaml/v3"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"

	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
)

type SettingsProvider struct {
	cfg    *stsSettingsConfig.FileSettingsProviderConfig
	logger *zap.Logger

	settingsCache *stsSettingsCommon.SettingsCache

	providerCancelFunc context.CancelFunc
	providerCancelWg   sync.WaitGroup
}

func NewFileSettingsProvider(cfg *stsSettingsConfig.FileSettingsProviderConfig, logger *zap.Logger) (*SettingsProvider, error) {
	provider := &SettingsProvider{
		cfg:           cfg,
		logger:        logger,
		settingsCache: stsSettingsCommon.NewSettingsCache(logger),
	}

	// Perform an initial load of the configuration file.
	if err := provider.loadSettings(); err != nil {
		return nil, err
	}

	return provider, nil
}

// Start initiates the file watching goroutine.
func (f *SettingsProvider) Start(ctx context.Context, host component.Host) error {
	f.logger.Info("Starting file-based settings provider.", zap.String("path", f.cfg.Path))

	ctx, f.providerCancelFunc = context.WithCancel(ctx)

	f.providerCancelWg.Add(1)
	go func() {
		defer f.providerCancelWg.Done()

		ticker := time.NewTicker(f.cfg.UpdateInterval)
		defer ticker.Stop()

		// The ticker doesn't tick immediately, so we do an initial run
		f.checkAndUpdateSettings()

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
func (f *SettingsProvider) Shutdown(ctx context.Context) error {
	f.logger.Info("Shutting down file-based settings provider.")
	if f.providerCancelFunc != nil {
		f.providerCancelFunc()
	}

	done := make(chan struct{})
	go func() {
		f.providerCancelWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		f.logger.Info("File provider shutdown complete.")
	case <-ctx.Done():
		return ctx.Err() // timed out waiting for goroutine
	}

	if f.settingsCache != nil {
		f.settingsCache.Shutdown()
	}

	return nil
}

func (f *SettingsProvider) RegisterForUpdates(types ...stsSettingsModel.SettingType) <-chan stsSettingsEvents.UpdateSettingsEvent {
	return f.settingsCache.RegisterForUpdates(types...)
}

func (f *SettingsProvider) Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool {
	return f.settingsCache.Unregister(ch)
}

func (f *SettingsProvider) GetCurrentSettingsByType(settingType stsSettingsModel.SettingType) ([]any, error) {
	return f.settingsCache.GetConcreteSettingsByType(settingType)
}

func (f *SettingsProvider) loadSettings() error {
	fileContent, err := f.readSettingsFile()
	if err != nil {
		return err
	}
	settingsMap, err := f.parseSettings(fileContent)
	if err != nil {
		return err
	}

	f.settingsCache.Update(settingsMap)

	return nil
}

// checkAndUpdateSettings checks for file changes and updates the state.
func (f *SettingsProvider) checkAndUpdateSettings() {
	f.logger.Debug("Checking for settings file updates.")

	fileContent, err := f.readSettingsFile()
	if err != nil {
		f.logger.Error("Failed to read settings file.", zap.Error(err))
		return
	}

	newSettingsByType, err := f.parseSettings(fileContent)
	if err != nil {
		f.logger.Error("Failed to parse settings file.", zap.Error(err))
		return
	}

	// Note: since the file-based settings provider is not intended for production use, we're updating the entire cache.
	f.settingsCache.Update(newSettingsByType)
}

// parseSettings is a private helper method to parse the file content.
func (f *SettingsProvider) parseSettings(fileContent []byte) (stsSettingsCommon.SettingsByType, error) {
	if len(fileContent) == 0 {
		return nil, errors.New("file content is empty")
	}

	var rawSettings []map[string]interface{}
	if err := yaml.Unmarshal(fileContent, &rawSettings); err != nil {
		return nil, err
	}

	var errs []error
	settingsByType := make(stsSettingsCommon.SettingsByType)
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

		settingType, err := stsSettingsCommon.GetSettingType(settingModel)
		if err != nil {
			errs = append(errs, err)
			f.logger.Error("Failed to get setting type.",
				zap.Error(err),
				zap.Any("setting", settingModel))
			continue
		}

		settingEntry := stsSettingsCommon.NewSettingEntry(settingModel)
		settingsByType[settingType] = append(settingsByType[settingType], settingEntry)
	}
	return settingsByType, errors.Join(errs...)
}

// readSettingsFile is a private helper method to read the file content from disk.
func (f *SettingsProvider) readSettingsFile() ([]byte, error) {
	return os.ReadFile(f.cfg.Path)
}
