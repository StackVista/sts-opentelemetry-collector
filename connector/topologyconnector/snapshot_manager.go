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

	// lifecycle safety
	started  bool
	refCount int // how many connectors are currently using this instance
}

type onRemovalsFunc = func(
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
	onRemovals onRemovalsFunc,
) error {
	s.mu.Lock()

	s.refCount++
	if s.started {
		s.logger.Debug("SnapshotManager already started, skipping re-init")
		s.mu.Unlock()
		return nil
	}

	s.logger.Info("SnapshotManager subscribed to setting updates")
	settingUpdatesCh, err := settingsProvider.RegisterForUpdates(
		stsSettingsModel.SettingTypeOtelComponentMapping,
		stsSettingsModel.SettingTypeOtelRelationMapping,
	)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	s.stopped = make(chan struct{})
	ctxWithCancel, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.started = true
	s.mu.Unlock()

	go func() {
		defer close(s.stopped)
		for {
			select {
			case <-ctxWithCancel.Done():
				return
			case <-settingUpdatesCh:
				s.logger.Info("Settings update received, updating mapping snapshots")
				s.GetAndUpdateSettingSnapshots(ctxWithCancel, settingsProvider, onRemovals)
			}
		}
	}()

	// Initial load
	s.GetAndUpdateSettingSnapshots(ctxWithCancel, settingsProvider, onRemovals)
	return nil
}

func (s *SnapshotManager) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.refCount > 0 {
		s.refCount--
	}

	if s.refCount == 0 && s.started {
		s.logger.Info("Stopping SnapshotManager (no more active connectors)")
		if s.cancel != nil {
			s.cancel()
		}
		if s.stopped != nil {
			<-s.stopped
		}
		s.started = false
	}
}

func (s *SnapshotManager) GetAndUpdateSettingSnapshots(
	ctx context.Context,
	provider stsSettingsApi.StsSettingsProvider,
	onRemovals onRemovalsFunc,
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

// Update Filter mappings for the selected input signal and trigger onRemovalsFunc
// when mappings have been removed
func (s *SnapshotManager) Update(
	ctx context.Context,
	newComponentMappings []stsSettingsModel.OtelComponentMapping,
	newRelationMappings []stsSettingsModel.OtelRelationMapping,
	onRemovals onRemovalsFunc,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prevComponents := flattenMappings(s.componentMappings)
	prevRelations := flattenMappings(s.relationMappings)

	change := SnapshotChange{
		RemovedComponentMappings: DiffSettings(prevComponents, newComponentMappings),
		RemovedRelationMappings:  DiffSettings(prevRelations, newRelationMappings),
	}

	// Then still update per-signal internal views for Current()
	for _, signal := range s.supportedSignals {
		s.componentMappings[signal] = filterForSignal(newComponentMappings, signal)
		s.relationMappings[signal] = filterForSignal(newRelationMappings, signal)
	}

	if len(change.RemovedComponentMappings) > 0 || len(change.RemovedRelationMappings) > 0 {
		onRemovals(ctx, change.RemovedComponentMappings, change.RemovedRelationMappings)
	}
}

func flattenMappings[T stsSettingsModel.SettingExtension](m map[stsSettingsModel.OtelInputSignal][]T) []T {
	var all []T
	for _, v := range m {
		all = append(all, v...)
	}
	return all
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
