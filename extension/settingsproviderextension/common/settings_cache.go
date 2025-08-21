package common

import (
	"fmt"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	stsSettingsSubscribers "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/subscribers"
	"go.uber.org/zap"
	"reflect"
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

func (s *SettingsByType) GetTypes() []stsSettingsModel.SettingType {
	types := make([]stsSettingsModel.SettingType, 0, len(*s))
	for t := range *s {
		types = append(types, t)
	}
	return types
}

func (s *SettingsByType) GetConcreteSettings(settingType stsSettingsModel.SettingType) ([]any, error) {
	entries := (*s)[settingType] // returns nil if missing
	concreteSettings := make([]any, len(entries))
	for i := range entries {
		// TODO: we could do a deep copy here?
		concreteSettings[i] = entries[i].Concrete
	}
	return concreteSettings, nil
}

type SettingsCache struct {
	logger              *zap.Logger
	subscriptionService stsSettingsSubscribers.SubscriptionService

	// Mutex for concurrent access to settingsByType
	settingsLock sync.RWMutex
	// A map where the key is the SettingType and the value is a slice of all currently active settings of that type.
	settingsByType SettingsByType
}

func NewSettingsCache(logger *zap.Logger) *SettingsCache {
	return &SettingsCache{
		subscriptionService: stsSettingsSubscribers.NewSubscriberHub(logger),
		settingsByType:      make(SettingsByType),
	}
}

func (s *SettingsCache) RegisterForUpdates(types ...stsSettingsModel.SettingType) <-chan stsSettingsEvents.UpdateSettingsEvent {
	return s.subscriptionService.Register(types...)
}

func (s *SettingsCache) Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool {
	return s.subscriptionService.Unregister(ch)
}

func (s *SettingsCache) Shutdown() {
	if s.subscriptionService != nil {
		s.subscriptionService.Shutdown()
	}
}

func (s *SettingsCache) GetAvailableSettingTypes() []stsSettingsModel.SettingType {
	s.settingsLock.RLock()
	defer s.settingsLock.RUnlock()

	return s.settingsByType.GetTypes()
}

func (s *SettingsCache) GetConcreteSettingsByType(settingType stsSettingsModel.SettingType) ([]any, error) {
	s.settingsLock.RLock()
	cachedConcreteEntries, err := s.settingsByType.GetConcreteSettings(settingType)
	s.settingsLock.RUnlock()

	return cachedConcreteEntries, err
}

func (s *SettingsCache) Update(settingsByType SettingsByType) {
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

func (s *SettingsCache) UpdateSettingsForType(settingType stsSettingsModel.SettingType, newEntries []SettingEntry) {
	var validEntries []SettingEntry
	var hasConversionFailures bool

	// Convert all new entries to concrete type first
	if converter, ok := ConverterFor(settingType); ok {
		for _, entry := range newEntries {
			_, err := s.toConcreteTypeIfNeeded(&entry, converter)
			if err != nil {
				s.logger.Warn("skipping setting entry due to conversion failure", zap.Error(err), zap.String("type", string(settingType)))
				hasConversionFailures = true
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

	// Optional: track conversion failures in metrics
	if hasConversionFailures {
		// TODO: increment conversion failure metric
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

func (s *SettingsCache) toConcreteTypeIfNeeded(entry *SettingEntry, converter ConverterFunc) (any, error) {
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

//func (s *SettingsCache) GetConcreteSettingsByType(settingType stsSettingsModel.SettingType) ([]any, error) {
//	// read lock to fetch cached entries
//	s.settingsLock.RLock()
//	cached, ok := s.settingsByType[settingType]
//	s.settingsLock.RUnlock()
//
//	if !ok {
//		return nil, fmt.Errorf("no settings for type %s", settingType)
//	}
//
//	// GetConcreteSettings converter (assume ConverterFor is thread-safe)
//	converter, ok := ConverterFor(settingType)
//	if !ok {
//		return nil, fmt.Errorf("no converter registered for type %s", settingType)
//	}
//
//	concreteSettings := make([]any, len(cached))
//
//	for i := range cached {
//		// Fast path: already hydrated
//		if cached[i].Concrete != nil {
//			concreteSettings[i] = cached[i].Concrete
//			continue
//		}
//
//		// Hydrate lazily with write lock
//		s.settingsLock.Lock()
//		if cached[i].Concrete == nil { // double-check
//			val, err := converter(cached[i].Raw)
//			if err != nil {
//				s.settingsLock.Unlock()
//				return nil, err
//			}
//			cached[i].Concrete = val
//		}
//		concreteSettings[i] = cached[i].Concrete
//		s.settingsLock.Unlock()
//	}
//
//	return concreteSettings, nil
//}
