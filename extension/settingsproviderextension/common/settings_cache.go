package common

import (
	"fmt"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"sync"
)

type SettingEntry struct {
	Raw      stsSettingsModel.Setting
	Concrete any // holds the typed/Concrete struct, lazy populated
}

func NewSettingEntry(raw stsSettingsModel.Setting) SettingEntry {
	return SettingEntry{
		Raw: raw,
	}
}

type SettingsByType map[stsSettingsModel.SettingType][]SettingEntry

type SettingsCache struct {
	// Mutex for concurrent access to settingsByType
	settingsLock sync.RWMutex
	// A map where the key is the SettingType and the value is a slice of all currently active settings of that type.
	settingsByType SettingsByType
}

func NewSettingsCache() *SettingsCache {
	return &SettingsCache{
		settingsByType: make(SettingsByType),
	}
}

func (c *SettingsCache) Update(settingsByType SettingsByType) {
	c.settingsLock.Lock()
	defer c.settingsLock.Unlock()
	c.settingsByType = settingsByType
}

func (c *SettingsCache) UpdateSettingsForType(settingType stsSettingsModel.SettingType, settingEntries []SettingEntry) {
	c.settingsLock.Lock()
	defer c.settingsLock.Unlock()
	c.settingsByType[settingType] = settingEntries
}

func (c *SettingsCache) GetAvailableSettingTypes() []stsSettingsModel.SettingType {
	c.settingsLock.RLock()
	defer c.settingsLock.RUnlock()
	types := make([]stsSettingsModel.SettingType, 0, len(c.settingsByType))
	for t := range c.settingsByType {
		types = append(types, t)
	}
	return types
}

func (c *SettingsCache) GetSettingsByType(settingType stsSettingsModel.SettingType) ([]any, error) {
	// read lock to fetch cached entries
	c.settingsLock.RLock()
	cached, ok := c.settingsByType[settingType]
	c.settingsLock.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no settings for type %s", settingType)
	}

	// Get converter (assume ConverterFor is thread-safe)
	converter, ok := ConverterFor(settingType)
	if !ok {
		return nil, fmt.Errorf("no converter registered for type %s", settingType)
	}

	concreteSettings := make([]any, len(cached))

	for i := range cached {
		// Fast path: already hydrated
		if cached[i].Concrete != nil {
			concreteSettings[i] = cached[i].Concrete
			continue
		}

		// Hydrate lazily with write lock
		c.settingsLock.Lock()
		if cached[i].Concrete == nil { // double-check
			val, err := converter(cached[i].Raw)
			if err != nil {
				c.settingsLock.Unlock()
				return nil, err
			}
			cached[i].Concrete = val
		}
		concreteSettings[i] = cached[i].Concrete
		c.settingsLock.Unlock()
	}

	return concreteSettings, nil
}
