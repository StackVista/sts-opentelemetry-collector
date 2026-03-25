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
	logger  *zap.Logger
	mu      sync.RWMutex
	cancel  context.CancelFunc
	stopped chan struct{}

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

// SnapshotUpdateListener is notified when mapping snapshots change. Implementors
// can precompute derived data (e.g., expression reference summaries) asynchronously.
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
	s.mu.Lock()

	s.refCount++
	if s.started {
		s.logger.Debug("SnapshotManager already started, skipping re-init")
		s.mu.Unlock()
		return nil
	}

	s.logger.Info("SnapshotManager subscribed to setting updates")
	settingUpdatesCh, err := settingsProvider.RegisterForUpdates(
		settingsproto.SettingTypeOtelComponentMapping,
		settingsproto.SettingTypeOtelRelationMapping,
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

	// Copy current state needed for async ref precomputation
	observersCopy := append([]SnapshotUpdateListener(nil), s.observers...)
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

	s.mu.Unlock()

	// Notify observers asynchronously so we don't block the snapshot update path.
	for _, obs := range observersCopy {
		o := obs
		// If the number of observers (atm, only ExpressionRefManager is a subscriber) or update frequency grows,
		// we should consider serializing (with a buffered worker) updates per observer to avoid a flurry of goroutines.
		// Leaving it as-is for now to prevent pre-maturely optimising.
		go o.Update(signalsCopy, componentMappingsCopy, relationMappingsCopy)
	}
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
