package topologyconnector

import (
	"context"
	"slices"
	"sync"

	stsSettingsApi "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"go.uber.org/zap"
)

type SnapshotChange struct {
	RemovedComponentMappings []settingsproto.OtelComponentMapping
	RemovedRelationMappings  []settingsproto.OtelRelationMapping
}

type SnapshotManager struct {
	logger *zap.Logger
	mu     sync.RWMutex

	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	stopped     chan struct{}

	supportedSignals []settingsproto.OtelInputSignal

	componentMappings map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping
	relationMappings  map[settingsproto.OtelInputSignal][]settingsproto.OtelRelationMapping

	// lifecycle safety
	started  bool
	refCount int // how many connectors are currently using this instance

	observers []SnapshotUpdateListener
}

type onRemovalsFunc = func(
	ctx context.Context,
	componentMappings []settingsproto.OtelComponentMapping,
	relationMappings []settingsproto.OtelRelationMapping,
)

// SnapshotUpdateListener prepares derived data before new mappings become visible.
// Update runs under the snapshot lock; it must not export data or call SnapshotManager.
type SnapshotUpdateListener interface {
	Update(
		signals []settingsproto.OtelInputSignal,
		componentMappings map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping,
		relationMappings map[settingsproto.OtelInputSignal][]settingsproto.OtelRelationMapping,
	)
}

func NewSnapshotManager(
	logger *zap.Logger,
	supportedSignals []settingsproto.OtelInputSignal,
	observers ...SnapshotUpdateListener,
) *SnapshotManager {
	return &SnapshotManager{
		logger:            logger,
		supportedSignals:  supportedSignals,
		componentMappings: make(map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping),
		relationMappings:  make(map[settingsproto.OtelInputSignal][]settingsproto.OtelRelationMapping),
		observers:         observers,
	}
}

func (s *SnapshotManager) Start(
	ctx context.Context,
	settingsProvider stsSettingsApi.StsSettingsProvider,
	onRemovals onRemovalsFunc,
) error {
	s.lifecycleMu.Lock()

	if s.started {
		s.refCount++
		s.logger.Debug("SnapshotManager already started, skipping re-init")
		s.lifecycleMu.Unlock()
		return nil
	}

	s.logger.Info("SnapshotManager subscribed to setting updates")
	settingUpdatesCh, err := settingsProvider.RegisterForUpdates(
		settingsproto.SettingTypeOtelComponentMapping,
		settingsproto.SettingTypeOtelRelationMapping,
	)
	if err != nil {
		s.lifecycleMu.Unlock()
		return err
	}

	stopped := make(chan struct{})
	s.stopped = stopped
	ctxWithCancel, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.started = true
	s.refCount++
	s.lifecycleMu.Unlock()

	go func() {
		defer close(stopped)
		for {
			select {
			case <-ctxWithCancel.Done():
				return
			case _, ok := <-settingUpdatesCh:
				if !ok || ctxWithCancel.Err() != nil {
					return
				}
				s.logger.Info("Settings update received, updating mapping snapshots")
				s.GetAndUpdateSettingSnapshots(ctxWithCancel, settingsProvider, onRemovals)
			}
		}
	}()

	// Initial load
	s.GetAndUpdateSettingSnapshots(ctxWithCancel, settingsProvider, onRemovals)
	return nil
}

// Stop returns true only for the last connector and waits outside the snapshot lock.
func (s *SnapshotManager) Stop(ctx context.Context) (bool, error) {
	s.lifecycleMu.Lock()

	if s.refCount > 0 {
		s.refCount--
	}

	if s.refCount != 0 || !s.started {
		s.lifecycleMu.Unlock()
		return false, nil
	}
	s.logger.Info("Stopping SnapshotManager (no more active connectors)")
	s.cancel()
	stopped := s.stopped
	s.started = false
	s.lifecycleMu.Unlock()
	select {
	case <-stopped:
		return true, nil
	case <-ctx.Done():
		return true, ctx.Err()
	}
}

func (s *SnapshotManager) GetAndUpdateSettingSnapshots(
	ctx context.Context,
	provider stsSettingsApi.StsSettingsProvider,
	onRemovals onRemovalsFunc,
) {
	newComponentMappings, err := stsSettingsApi.GetSettingsAs[settingsproto.OtelComponentMapping](provider)
	if err != nil {
		s.logger.Error("failed to get component mappings", zap.Error(err))
		return
	}

	newRelationMappings, err := stsSettingsApi.GetSettingsAs[settingsproto.OtelRelationMapping](provider)
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
	newComponentMappings []settingsproto.OtelComponentMapping,
	newRelationMappings []settingsproto.OtelRelationMapping,
	onRemovals onRemovalsFunc,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return
	}

	prevComponents := flattenMappings(s.componentMappings)
	prevRelations := flattenMappings(s.relationMappings)

	change := SnapshotChange{
		RemovedComponentMappings: DiffSettings(prevComponents, newComponentMappings),
		RemovedRelationMappings:  DiffSettings(prevRelations, newRelationMappings),
	}

	addedComponentMappings := DiffSettings(newComponentMappings, prevComponents)
	addedRelationMappings := DiffSettings(newRelationMappings, prevRelations)

	s.logger.Info("Updating mapping snapshots",
		zap.Int("componentMappings", len(newComponentMappings)),
		zap.Int("relationMappings", len(newRelationMappings)),
		zap.Strings("addedComponentMappings", identifiersOf(addedComponentMappings)),
		zap.Strings("removedComponentMappings", identifiersOf(change.RemovedComponentMappings)),
		zap.Strings("addedRelationMappings", identifiersOf(addedRelationMappings)),
		zap.Strings("removedRelationMappings", identifiersOf(change.RemovedRelationMappings)),
	)

	// Then still update per-signal internal views for Current()
	for _, signal := range s.supportedSignals {
		s.componentMappings[signal] = filterForSignal(newComponentMappings, signal)
		s.relationMappings[signal] = filterForSignal(newRelationMappings, signal)
	}

	// Give observers copies so they cannot mutate the active mapping slices.
	signalsCopy := append([]settingsproto.OtelInputSignal(nil), s.supportedSignals...)
	componentMappingsCopy := make(map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping)
	relationMappingsCopy := make(map[settingsproto.OtelInputSignal][]settingsproto.OtelRelationMapping)
	for _, sig := range signalsCopy {
		componentMappingsCopy[sig] = append([]settingsproto.OtelComponentMapping(nil), s.componentMappings[sig]...)
		relationMappingsCopy[sig] = append([]settingsproto.OtelRelationMapping(nil), s.relationMappings[sig]...)
	}

	if len(change.RemovedComponentMappings) > 0 || len(change.RemovedRelationMappings) > 0 {
		onRemovals(ctx, change.RemovedComponentMappings, change.RemovedRelationMappings)
	}

	// Prepare references and queue metadata before consumers can read the mappings.
	for _, obs := range s.observers {
		obs.Update(signalsCopy, componentMappingsCopy, relationMappingsCopy)
	}
}

// identifiersOf returns the GetIdentifier() value of each mapping, for concise logging.
func identifiersOf[T settingsproto.SettingExtension](mappings []T) []string {
	ids := make([]string, len(mappings))
	for i, m := range mappings {
		ids[i] = m.GetIdentifier()
	}
	return ids
}

func flattenMappings[T settingsproto.SettingExtension](
	mappingsBySignal map[settingsproto.OtelInputSignal][]T,
) []T {
	seen := make(map[string]struct{})
	var all []T

	for _, mappings := range mappingsBySignal {
		for _, mapping := range mappings {
			id := mapping.GetIdentifier()
			if _, exists := seen[id]; exists {
				continue
			}
			seen[id] = struct{}{}
			all = append(all, mapping)
		}
	}

	return all
}

func filterForSignal[T settingsproto.SettingExtension](mappings []T, signal settingsproto.OtelInputSignal) []T {
	var filtered []T
	for _, m := range mappings {
		if slices.Contains(m.GetInputSignals(), signal) {
			filtered = append(filtered, m)
		}
	}
	return filtered
}

func (s *SnapshotManager) Current(
	signal settingsproto.OtelInputSignal,
) ([]settingsproto.OtelComponentMapping, []settingsproto.OtelRelationMapping) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	comps := append([]settingsproto.OtelComponentMapping(nil), s.componentMappings[signal]...)
	rels := append([]settingsproto.OtelRelationMapping(nil), s.relationMappings[signal]...)
	return comps, rels
}

// DiffSettings returns all settings that exist in `primary` but not in `comparison`.
// Two settings are considered the same if their GetIdentifier() values match.
//
// Example usage:
//
//	added := DiffSettings(newSettings, oldSettings)
//	removed := DiffSettings(oldSettings, newSettings)
func DiffSettings[T settingsproto.SettingExtension](primary, comparison []T) []T {
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
