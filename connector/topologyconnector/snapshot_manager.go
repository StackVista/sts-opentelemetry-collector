package topologyconnector

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
	stopped chan struct{}

	supportedSignals []stsSettingsModel.OtelInputSignal

	componentMappings map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelComponentMapping
	relationMappings  map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelRelationMapping
}

type onRemovals = func(
	ctx context.Context,
	componentMappings []stsSettingsModel.OtelComponentMapping,
	relationMappings []stsSettingsModel.OtelRelationMapping,
)

func NewSnapshotManager(logger *zap.Logger, supportedSignals []stsSettingsModel.OtelInputSignal) *SnapshotManager {
	return &SnapshotManager{
		logger:            logger,
		supportedSignals:  supportedSignals,
		componentMappings: make(map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelComponentMapping),
		relationMappings:  make(map[stsSettingsModel.OtelInputSignal][]stsSettingsModel.OtelRelationMapping),
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

// Update Filter mappings for the selected input signal and trigger onRemovals
// when mappings have been removed
func (s *SnapshotManager) Update(
	ctx context.Context,
	newComponentMappings []stsSettingsModel.OtelComponentMapping,
	newRelationMappings []stsSettingsModel.OtelRelationMapping,
	onRemovals onRemovals,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, signal := range s.supportedSignals {
		applicableComponents := filterForSignal(newComponentMappings, signal)
		applicableRelations := filterForSignal(newRelationMappings, signal)

		prevComponents := s.componentMappings[signal]
		prevRelations := s.relationMappings[signal]

		change := SnapshotChange{
			RemovedComponentMappings: DiffSettings(prevComponents, applicableComponents),
			RemovedRelationMappings:  DiffSettings(prevRelations, applicableRelations),
		}

		s.componentMappings[signal] = applicableComponents
		s.relationMappings[signal] = applicableRelations

		if len(change.RemovedComponentMappings) > 0 || len(change.RemovedRelationMappings) > 0 {
			onRemovals(ctx, change.RemovedComponentMappings, change.RemovedRelationMappings)
		}
	}
}

func filterForSignal[T stsSettingsModel.SettingExtension](mappings []T, signal stsSettingsModel.OtelInputSignal) []T {
	var filtered []T
	for _, m := range mappings {
		if slices.Contains(m.GetInputSignals(), signal) {
			filtered = append(filtered, m)
		}
	}
	return filtered
}

func (s *SnapshotManager) Current(
	signal stsSettingsModel.OtelInputSignal,
) ([]stsSettingsModel.OtelComponentMapping, []stsSettingsModel.OtelRelationMapping) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	comps := append([]stsSettingsModel.OtelComponentMapping(nil), s.componentMappings[signal]...)
	rels := append([]stsSettingsModel.OtelRelationMapping(nil), s.relationMappings[signal]...)
	return comps, rels
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
