package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	stsProviderCommon "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/common"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/config"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/subscribers"
	yaml "go.yaml.in/yaml/v3"
	"os"
	"reflect"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"

	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
)

type cachedSetting struct {
	raw                 stsSettingsModel.Setting
	concreteSettingType any // holds typed/concrete struct, lazy populated
}

type SettingsProvider struct {
	cfg    *stsSettingsConfig.FileSettingsProviderConfig
	logger *zap.Logger

	subscriberHub *subscribers.SubscriberHub

	// Mutex for concurrent access to currentSettings
	settingsLock sync.RWMutex
	// A map where the key is the SettingType and the value is a slice of all currently active settings of that type.
	currentSettings map[stsSettingsModel.SettingType][]cachedSetting

	providerCancelFunc context.CancelFunc
	providerCancelWg   sync.WaitGroup
}

func NewFileSettingsProvider(cfg *stsSettingsConfig.FileSettingsProviderConfig, logger *zap.Logger) (*SettingsProvider, error) {
	provider := &SettingsProvider{
		cfg:             cfg,
		logger:          logger,
		subscriberHub:   subscribers.NewSubscriberHub(),
		currentSettings: make(map[stsSettingsModel.SettingType][]cachedSetting),
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
		f.subscriberHub.Shutdown()
		f.logger.Info("File provider shutdown complete.")
		return nil
	case <-ctx.Done():
		return ctx.Err() // timed out waiting for goroutine
	}
}

func (f *SettingsProvider) RegisterForUpdates(types ...stsSettingsModel.SettingType) <-chan stsSettingsEvents.UpdateSettingsEvent {
	return f.subscriberHub.Register(types...)
}

func (f *SettingsProvider) Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool {
	return f.subscriberHub.Unregister(ch)
}

func (f *SettingsProvider) GetCurrentSettingsByType(settingType stsSettingsModel.SettingType) (any, error) {
	f.settingsLock.RLock()
	defer f.settingsLock.RUnlock()

	cachedSetting, ok := f.currentSettings[settingType]
	if !ok {
		return nil, fmt.Errorf("no settings for type %s", settingType)
	}

	converterFor, ok := stsProviderCommon.ConverterFor(settingType)
	if !ok {
		return nil, fmt.Errorf("no converter registered for type %s", settingType)
	}

	// hydrate cache on demand
	out := make([]any, 0, len(cachedSetting))
	for i := range cachedSetting {
		if cachedSetting[i].concreteSettingType == nil {
			val, err := converterFor(cachedSetting[i].raw)
			if err != nil {
				return nil, err
			}
			cachedSetting[i].concreteSettingType = val
		}
		out = append(out, cachedSetting[i].concreteSettingType)
	}

	return out, nil
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

	f.settingsLock.Lock()
	f.currentSettings = settingsMap
	f.settingsLock.Unlock()

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

	newSettingsMap, err := f.parseSettings(fileContent)
	if err != nil {
		f.logger.Error("Failed to parse settings file.", zap.Error(err))
		return
	}

	f.settingsLock.RLock()
	oldSettings := f.currentSettings
	f.settingsLock.RUnlock()

	changedTypes := diffSettingsMaps(oldSettings, newSettingsMap)
	if len(changedTypes) == 0 {
		f.logger.Debug("No settings changes detected")
		return
	}

	f.logger.Debug("Settings changed", zap.Int("changedTypes", len(changedTypes)))

	// Update cache
	f.settingsLock.Lock()
	f.currentSettings = newSettingsMap
	f.settingsLock.Unlock()

	// Notify only for changed types
	for _, settingType := range changedTypes {
		f.subscriberHub.Notify(stsSettingsEvents.UpdateSettingsEvent{
			Type: settingType,
		})
	}
}

// parseSettings is a private helper method to parse the file content.
func (f *SettingsProvider) parseSettings(fileContent []byte) (map[stsSettingsModel.SettingType][]cachedSetting, error) {
	if len(fileContent) == 0 {
		return nil, errors.New("file content is empty")
	}

	var rawSettings []map[string]interface{}
	if err := yaml.Unmarshal(fileContent, &rawSettings); err != nil {
		return nil, err
	}

	var errs []error
	settingsMap := make(map[stsSettingsModel.SettingType][]cachedSetting)
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

		cachedSetting := cachedSetting{
			raw:                 settingModel,
			concreteSettingType: nil,
		}
		settingsMap[settingType] = append(settingsMap[settingType], cachedSetting)
	}
	return settingsMap, errors.Join(errs...)
}

// readSettingsFile is a private helper method to read the file content from disk.
func (f *SettingsProvider) readSettingsFile() ([]byte, error) {
	return os.ReadFile(f.cfg.Path)
}

func diffSettingsMaps(oldMap, newMap map[stsSettingsModel.SettingType][]cachedSetting) []stsSettingsModel.SettingType {
	var diff []stsSettingsModel.SettingType

	// Detect added or changed
	for k, newVals := range newMap {
		oldVals, ok := oldMap[k]
		if !ok || !equalCachedSettings(oldVals, newVals) {
			diff = append(diff, k)
		}
	}

	// Detect removed
	for k := range oldMap {
		if _, ok := newMap[k]; !ok {
			diff = append(diff, k)
		}
	}

	return diff
}

func equalCachedSettings(a, b []cachedSetting) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !reflect.DeepEqual(a[i].raw, b[i].raw) {
			return false
		}
	}
	return true
}
