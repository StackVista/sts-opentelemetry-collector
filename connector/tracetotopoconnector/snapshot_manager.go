package tracetotopoconnector

import (
	"context"
	"slices"
	"sync"

	stsSettingsApi "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.uber.org/zap"
)

type SnapshotChange struct {
	RemovedComponentMappings []stsSettingsModel.OtelComponentMapping
	RemovedRelationMappings  []stsSettingsModel.OtelRelationMapping
}

type SnapshotManager struct {
	logger  *zap.Logger
	mu      sync.RWMutex
	cancel  context.CancelFunc
	signal  stsSettingsModel.OtelInputSignal
	stopped chan struct{}

	componentMappings []stsSettingsModel.OtelComponentMapping
	relationMappings  []stsSettingsModel.OtelRelationMapping
}

type onRemovals = func(
	ctx context.Context,
	componentMappings []stsSettingsModel.OtelComponentMapping,
	relationMappings []stsSettingsModel.OtelRelationMapping,
)

func NewSnapshotManager(logger *zap.Logger, signal stsSettingsModel.OtelInputSignal) *SnapshotManager {
	return &SnapshotManager{
		logger:            logger,
		componentMappings: []stsSettingsModel.OtelComponentMapping{},
		relationMappings:  []stsSettingsModel.OtelRelationMapping{},
		signal:            signal,
	}
}

func (s *SnapshotManager) Start(
	ctx context.Context,
	settingsProvider stsSettingsApi.StsSettingsProvider,
	onRemovals onRemovals,
) error {
	settingUpdatesCh, err := settingsProvider.RegisterForUpdates(
		stsSettingsModel.SettingTypeOtelComponentMapping,
		stsSettingsModel.SettingTypeOtelRelationMapping,
	)
	if err != nil {
		return err
	}

	s.logger.Info("SnapshotManager subscribed to setting updates")
	s.stopped = make(chan struct{})
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	go func() {
		defer close(s.stopped)
		for {
			select {
			case <-ctx.Done():
				return
			case <-settingUpdatesCh:
				s.logger.Info("Settings update received, updating mapping snapshots")
				s.GetAndUpdateSettingSnapshots(ctx, settingsProvider, onRemovals)
			}
		}
	}()

	// Initial load
	s.GetAndUpdateSettingSnapshots(ctx, settingsProvider, onRemovals)
	return nil
}

func (s *SnapshotManager) GetAndUpdateSettingSnapshots(
	ctx context.Context,
	provider stsSettingsApi.StsSettingsProvider,
	onRemovals onRemovals,
) {
	newComponentMappings, err := stsSettingsApi.GetSettingsAs[stsSettingsModel.OtelComponentMapping](provider)
	if err != nil {
		s.logger.Error("failed to get component mappings", zap.Error(err))
		return
	}

	newRelationMappings, err := stsSettingsApi.GetSettingsAs[stsSettingsModel.OtelRelationMapping](provider)
	if err != nil {
		s.logger.Error("failed to get relation mappings", zap.Error(err))
		return
	}

	s.Update(ctx, newComponentMappings, newRelationMappings, onRemovals)
}

// Filter mappings for the selected input signal and trigger onRemovals
// when mappings have been removed
func (s *SnapshotManager) Update(
	ctx context.Context,
	newComponentMappings []stsSettingsModel.OtelComponentMapping,
	newRelationMappings []stsSettingsModel.OtelRelationMapping,
	onRemovals onRemovals,
) {
	applicableComponentMappings := []stsSettingsModel.OtelComponentMapping{}
	for _, mapping := range newComponentMappings {
		if slices.Contains(mapping.InputSignals, s.signal) {
			applicableComponentMappings = append(applicableComponentMappings, mapping)
		}
	}

	applicableRelationMappings := []stsSettingsModel.OtelRelationMapping{}
	for _, mapping := range newRelationMappings {
		if slices.Contains(mapping.InputSignals, s.signal) {
			applicableRelationMappings = append(applicableRelationMappings, mapping)
		}
	}

	change := s.updateInteral(applicableComponentMappings, applicableRelationMappings)
	if len(change.RemovedComponentMappings) > 0 || len(change.RemovedRelationMappings) > 0 {
		onRemovals(ctx, change.RemovedComponentMappings, change.RemovedRelationMappings)
	}
}

// Update compares the provided component and relation mappings against the
// current snapshot, computes the differences, and updates the internal state.
func (s *SnapshotManager) updateInteral(
	newComponents []stsSettingsModel.OtelComponentMapping,
	newRelations []stsSettingsModel.OtelRelationMapping,
) SnapshotChange {
	s.mu.Lock()
	defer s.mu.Unlock()

	change := SnapshotChange{
		RemovedComponentMappings: DiffSettings(s.componentMappings, newComponents),
		RemovedRelationMappings:  DiffSettings(s.relationMappings, newRelations),
	}

	// Update the internal state
	s.componentMappings = newComponents
	s.relationMappings = newRelations

	return change
}

func (s *SnapshotManager) Current() (
	[]stsSettingsModel.OtelComponentMapping,
	[]stsSettingsModel.OtelRelationMapping,
) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]stsSettingsModel.OtelComponentMapping(nil), s.componentMappings...),
		append([]stsSettingsModel.OtelRelationMapping(nil), s.relationMappings...)
}

func (s *SnapshotManager) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.stopped != nil {
		<-s.stopped
	}

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
