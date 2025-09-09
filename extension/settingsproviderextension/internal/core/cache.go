package core

import (
	"fmt"
	"github.com/mohae/deepcopy"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.uber.org/zap"
	"reflect"
	"sync"
)

type SettingEntry struct {
	Raw stsSettingsModel.Setting
	// Concrete is the typed representation of Raw, eagerly materialized when the
	// cache is updated. This avoids repeated JSON (un)marshaling when converting
	// from the generic Setting into its concrete type (e.g. when retrieving a setting (by type) from the cache).
	Concrete any
}

func NewSettingEntry(raw stsSettingsModel.Setting) SettingEntry {
	return SettingEntry{
		Raw: raw,
	}
}

type SettingsByType map[stsSettingsModel.SettingType][]SettingEntry

func (s *SettingsByType) getTypes() []stsSettingsModel.SettingType {
	types := make([]stsSettingsModel.SettingType, 0, len(*s))
	for t := range *s {
		types = append(types, t)
	}
	return types
}

//nolint:unparam
func (s *SettingsByType) getConcreteSettings(settingType stsSettingsModel.SettingType) ([]any, error) {
	entries := (*s)[settingType] // returns nil if missing
	concreteSettings := make([]any, len(entries))
	for i := range entries {
		concreteSettings[i] = deepcopy.Copy(entries[i].Concrete)
	}
	return concreteSettings, nil
}

type SettingsCache interface {
	RegisterForUpdates(types ...stsSettingsModel.SettingType) (<-chan stsSettingsEvents.UpdateSettingsEvent, error)
	Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool
	GetAvailableSettingTypes() []stsSettingsModel.SettingType
	GetConcreteSettingsByType(settingType stsSettingsModel.SettingType) ([]any, error)
	Update(settingsByType SettingsByType)
	UpdateSettingsForType(settingType stsSettingsModel.SettingType, newEntries []SettingEntry)
	Shutdown()
}

type DefaultSettingsCache struct {
	logger              *zap.Logger
	subscriptionService Subscriber

	// Mutex for concurrent access to settingsByType
	settingsLock sync.RWMutex
	// A map where the key is the SettingType and the value is a slice of all currently active settings of that type.
	settingsByType SettingsByType
}

func NewDefaultSettingsCache(logger *zap.Logger) *DefaultSettingsCache {
	return &DefaultSettingsCache{
		subscriptionService: NewSubscriberHub(logger),
		settingsByType:      make(SettingsByType),
	}
}

func (s *DefaultSettingsCache) RegisterForUpdates(
	types ...stsSettingsModel.SettingType,
) (<-chan stsSettingsEvents.UpdateSettingsEvent, error) {
	return s.subscriptionService.Register(types...)
}

func (s *DefaultSettingsCache) Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool {
	return s.subscriptionService.Unregister(ch)
}

func (s *DefaultSettingsCache) Shutdown() {
	if s.subscriptionService != nil {
		s.subscriptionService.Shutdown()
	}
}

func (s *DefaultSettingsCache) GetAvailableSettingTypes() []stsSettingsModel.SettingType {
	s.settingsLock.RLock()
	defer s.settingsLock.RUnlock()

	return s.settingsByType.getTypes()
}

func (s *DefaultSettingsCache) GetConcreteSettingsByType(settingType stsSettingsModel.SettingType) ([]any, error) {
	s.settingsLock.RLock()
	cachedConcreteEntries, err := s.settingsByType.getConcreteSettings(settingType)
	s.settingsLock.RUnlock()

	return cachedConcreteEntries, err
}

func (s *DefaultSettingsCache) Update(settingsByType SettingsByType) {
	// Iterate new snapshot and apply type-by-type
	for t, newEntries := range settingsByType {
		s.UpdateSettingsForType(t, newEntries)
	}

	// Clean up: detect if some types disappeared in the new snapshot
	var deletedTypes []stsSettingsModel.SettingType

	s.settingsLock.Lock()
	for t := range s.settingsByType {
		if _, exists := settingsByType[t]; !exists {
			// Track type for deletion
			deletedTypes = append(deletedTypes, t)
			delete(s.settingsByType, t)
		}
	}
	s.settingsLock.Unlock()

	s.subscriptionService.Notify(deletedTypes...)
}

func (s *DefaultSettingsCache) UpdateSettingsForType(
	settingType stsSettingsModel.SettingType,
	newEntries []SettingEntry,
) {
	var validEntries []SettingEntry

	// Convert all new entries to concrete type first
	if converter, ok := ConverterFor(settingType); ok {
		for _, entry := range newEntries {
			//nolint:gosec
			_, err := s.toConcreteTypeIfNeeded(&entry, converter)
			if err != nil {
				s.logger.Warn(
					"skipping setting entry due to conversion failure",
					zap.Error(err),
					zap.String("type", string(settingType)),
				)
				continue
			}
			validEntries = append(validEntries, entry)
		}
	} else {
		validEntries = newEntries
	}

	s.settingsLock.Lock()
	oldEntries := s.settingsByType[settingType]

	// Compare old vs new (only valid entries)
	changed := concreteSettingsChanged(oldEntries, validEntries)

	// Store only the valid entries
	s.settingsByType[settingType] = validEntries
	s.settingsLock.Unlock()

	// Only notify if we have valid changes
	if changed && len(validEntries) > 0 {
		s.subscriptionService.Notify(settingType)
	}
}

func concreteSettingsChanged(oldEntries, newEntries []SettingEntry) bool {
	if len(oldEntries) != len(newEntries) {
		return true
	}

	for i := range oldEntries {
		if !reflect.DeepEqual(oldEntries[i].Concrete, newEntries[i].Concrete) {
			return true
		}
	}

	return false
}

func (s *DefaultSettingsCache) toConcreteTypeIfNeeded(entry *SettingEntry, converter ConverterFunc) (any, error) {
	if entry.Concrete != nil {
		return entry.Concrete, nil
	}

	val, err := converter(entry.Raw)
	if err != nil {
		return nil, fmt.Errorf("failed to convert setting: %w", err)
	}

	entry.Concrete = val
	return val, nil
}
