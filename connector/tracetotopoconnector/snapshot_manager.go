package tracetotopoconnector

import (
	"sync"

	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.uber.org/zap"
)

type SnapshotChange struct {
	AddedComponents   []stsSettingsModel.OtelComponentMapping
	RemovedComponents []stsSettingsModel.OtelComponentMapping

	AddedRelations   []stsSettingsModel.OtelRelationMapping
	RemovedRelations []stsSettingsModel.OtelRelationMapping
}

type SnapshotManager struct {
	logger *zap.Logger
	mu     sync.RWMutex

	components []stsSettingsModel.OtelComponentMapping
	relations  []stsSettingsModel.OtelRelationMapping
}

func NewSnapshotManager(logger *zap.Logger) *SnapshotManager {
	return &SnapshotManager{
		logger:     logger,
		components: []stsSettingsModel.OtelComponentMapping{},
		relations:  []stsSettingsModel.OtelRelationMapping{},
	}
}

// Update compares the provided component and relation mappings against the
// current snapshot, computes the differences, and updates the internal state.
func (s *SnapshotManager) Update(
	newComponents []stsSettingsModel.OtelComponentMapping,
	newRelations []stsSettingsModel.OtelRelationMapping,
) SnapshotChange {
	s.mu.Lock()
	defer s.mu.Unlock()

	change := SnapshotChange{
		AddedComponents:   DiffSettings(newComponents, s.components),
		RemovedComponents: DiffSettings(s.components, newComponents),
		AddedRelations:    DiffSettings(newRelations, s.relations),
		RemovedRelations:  DiffSettings(s.relations, newRelations),
	}

	// Update the internal state
	s.components = newComponents
	s.relations = newRelations

	return change
}

func (s *SnapshotManager) Current() (
	[]stsSettingsModel.OtelComponentMapping,
	[]stsSettingsModel.OtelRelationMapping,
) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]stsSettingsModel.OtelComponentMapping(nil), s.components...),
		append([]stsSettingsModel.OtelRelationMapping(nil), s.relations...)
}

// DiffSettings returns all settings that exist in `primary` but not in `comparison`.
// Two settings are considered the same if their GetIdentifier() values match.
//
// Example usage:
//
//	added := DiffSettings(newSettings, oldSettings)
//	removed := DiffSettings(oldSettings, newSettings)
func DiffSettings[T stsSettingsModel.SettingExtension](primary, comparison []T) []T {
	var diff []T
	for _, candidate := range primary {
		found := false
		for _, existing := range comparison {
			if candidate.GetIdentifier() == existing.GetIdentifier() {
				found = true
				break
			}
		}
		if !found {
			diff = append(diff, candidate)
		}
	}
	return diff
}
