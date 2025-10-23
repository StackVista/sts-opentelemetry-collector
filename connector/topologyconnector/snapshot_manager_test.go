package topologyconnector_test

import (
	"context"
	"sync"
	"testing"
	"time"

	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	topologyConnector "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector"
	stsSettingsApi "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
)

func componentMapping(id string, signals ...stsSettingsApi.OtelInputSignal) stsSettingsApi.OtelComponentMapping {
	return stsSettingsApi.OtelComponentMapping{Identifier: id, InputSignals: signals}
}

func relationMapping(id string, signals ...stsSettingsApi.OtelInputSignal) stsSettingsApi.OtelRelationMapping {
	return stsSettingsApi.OtelRelationMapping{Identifier: id, InputSignals: signals}
}

func TestSnapshotManager_StartStopLifecycle(t *testing.T) {
	logger := zap.NewNop()

	// 1. Initialize mock provider with initial settings
	initialComponents := []stsSettingsApi.OtelComponentMapping{componentMapping("c1", stsSettingsApi.TRACES)}
	initialRelations := []stsSettingsApi.OtelRelationMapping{relationMapping("r1", stsSettingsApi.TRACES)}
	provider := topologyConnector.NewMockStsSettingsProvider(initialComponents, initialRelations)

	manager := topologyConnector.NewSnapshotManager(logger, stsSettingsApi.TRACES)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var removedComponentMappings []stsSettingsApi.OtelComponentMapping
	var removedRelationMappings []stsSettingsApi.OtelRelationMapping

	onRemovals := func(_ context.Context, componentMappings []stsSettingsApi.OtelComponentMapping, relationMappings []stsSettingsApi.OtelRelationMapping) {
		mu.Lock()
		defer mu.Unlock()
		removedComponentMappings = append(removedComponentMappings, componentMappings...)
		removedRelationMappings = append(removedRelationMappings, relationMappings...)
	}

	err := manager.Start(ctx, provider, onRemovals)
	require.NoError(t, err)

	// Verify initial state
	comps, rels := manager.Current()
	require.Equal(t, initialComponents, comps)
	require.Equal(t, initialRelations, rels)

	// Update: remove c1/r1 and add c2/r2
	provider.ComponentMappings = []stsSettingsApi.OtelComponentMapping{componentMapping("c2", stsSettingsApi.TRACES)}
	provider.RelationMappings = []stsSettingsApi.OtelRelationMapping{relationMapping("r2", stsSettingsApi.TRACES)}
	provider.SettingUpdatesCh <- stsSettingsEvents.UpdateSettingsEvent{}

	// Verify callback was triggered with removals
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(removedComponentMappings) == 1 && removedComponentMappings[0].Identifier == "c1" &&
			len(removedRelationMappings) == 1 && removedRelationMappings[0].Identifier == "r1"
	}, time.Second, 50*time.Millisecond, "expected removal callback to fire")

	manager.Stop()

	provider.ComponentMappings = []stsSettingsApi.OtelComponentMapping{componentMapping("c3", stsSettingsApi.TRACES)}
	provider.RelationMappings = []stsSettingsApi.OtelRelationMapping{relationMapping("r3", stsSettingsApi.TRACES)}
	select {
	case provider.SettingUpdatesCh <- stsSettingsEvents.UpdateSettingsEvent{}:
		t.Fatal("expected closed channel to panic or be ignored")
	default:
		// all good, channel closed
	}
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, removedComponentMappings, 1)
	assert.Len(t, removedRelationMappings, 1)
}

func TestSnapshotManager_UpdateDetectsChanges(t *testing.T) {
	logger := zap.NewNop()
	manager := topologyConnector.NewSnapshotManager(logger, stsSettingsApi.TRACES)

	initialComponentMappings := []stsSettingsApi.OtelComponentMapping{
		componentMapping("c1", stsSettingsApi.TRACES),
		componentMapping("c2", stsSettingsApi.TRACES),
	}
	initialRelationMappings := []stsSettingsApi.OtelRelationMapping{
		relationMapping("r1", stsSettingsApi.TRACES),
		relationMapping("r2", stsSettingsApi.TRACES),
	}

	var removedComponentMappings []stsSettingsApi.OtelComponentMapping
	var removedRelationMappings []stsSettingsApi.OtelRelationMapping
	onRemovals := func(_ context.Context, cMappings []stsSettingsApi.OtelComponentMapping, rMappings []stsSettingsApi.OtelRelationMapping) {
		removedComponentMappings = cMappings
		removedRelationMappings = rMappings
	}

	manager.Update(context.Background(), initialComponentMappings, initialRelationMappings, onRemovals)
	assert.Empty(t, removedComponentMappings)
	assert.Empty(t, removedRelationMappings)

	// The current snapshot should be the same as the initial snapshot
	componentMappings, relationMappings := manager.Current()
	assert.Equal(t, initialComponentMappings, componentMappings)
	assert.Equal(t, initialRelationMappings, relationMappings)

	// Second update: add one and remove one
	newComponentMappings := []stsSettingsApi.OtelComponentMapping{
		componentMapping("c2", stsSettingsApi.TRACES), // existing
		componentMapping("c3", stsSettingsApi.TRACES), // new
	}
	newRelationMappings := []stsSettingsApi.OtelRelationMapping{
		relationMapping("r2", stsSettingsApi.TRACES), // existing
		relationMapping("r3", stsSettingsApi.TRACES), // new
	}

	manager.Update(context.Background(), newComponentMappings, newRelationMappings, onRemovals)
	assert.ElementsMatch(t, []stsSettingsApi.OtelComponentMapping{componentMapping("c1", stsSettingsApi.TRACES)}, removedComponentMappings)
	assert.ElementsMatch(t, []stsSettingsApi.OtelRelationMapping{relationMapping("r1", stsSettingsApi.TRACES)}, removedRelationMappings)
}

func TestSnapshotManager_CurrentReturnsCopy(t *testing.T) {
	logger := zap.NewNop()
	manager := topologyConnector.NewSnapshotManager(logger, stsSettingsApi.TRACES)

	initialComponentMappings := []stsSettingsApi.OtelComponentMapping{componentMapping("c1", stsSettingsApi.TRACES)}
	initialRelationMappings := []stsSettingsApi.OtelRelationMapping{relationMapping("r1", stsSettingsApi.TRACES)}
	manager.Update(context.Background(), initialComponentMappings, initialRelationMappings, dummyOnRemovals)

	componentMappings, relationMappings := manager.Current()
	assert.Equal(t, initialComponentMappings, componentMappings)
	assert.Equal(t, initialRelationMappings, relationMappings)

	// Mutate returned slices
	componentMappings[0].Identifier = "mutated"
	relationMappings[0].Identifier = "mutated"

	// Fetch again and verify original data not changed
	componentMappings2, relationMappings2 := manager.Current()
	assert.Equal(t, "c1", componentMappings2[0].Identifier)
	assert.Equal(t, "r1", relationMappings2[0].Identifier)
}

func TestSnapshotManager_MappingsAreFilteredForSignal(t *testing.T) {
	logger := zap.NewNop()
	tracesManager := topologyConnector.NewSnapshotManager(logger, stsSettingsApi.TRACES)
	metricsManager := topologyConnector.NewSnapshotManager(logger, stsSettingsApi.METRICS)

	initialComponentMappings := []stsSettingsApi.OtelComponentMapping{componentMapping("c1", stsSettingsApi.TRACES)}
	initialRelationMappings := []stsSettingsApi.OtelRelationMapping{relationMapping("r1", stsSettingsApi.METRICS)}
	tracesManager.Update(context.Background(), initialComponentMappings, initialRelationMappings, dummyOnRemovals)
	metricsManager.Update(context.Background(), initialComponentMappings, initialRelationMappings, dummyOnRemovals)

	tracesComponentMappings, tracesRelationMappings := tracesManager.Current()
	assert.Equal(t, initialComponentMappings, tracesComponentMappings)
	assert.Empty(t, tracesRelationMappings)

	metricsComponentMappings, metricsRelationMappings := metricsManager.Current()
	assert.Equal(t, initialRelationMappings, metricsRelationMappings)
	assert.Empty(t, metricsComponentMappings)
}

func TestDiffSettings_GenericFunction(t *testing.T) {
	a := []stsSettingsApi.OtelComponentMapping{componentMapping("1", stsSettingsApi.TRACES), componentMapping("2", stsSettingsApi.TRACES), componentMapping("3", stsSettingsApi.TRACES)}
	b := []stsSettingsApi.OtelComponentMapping{componentMapping("2", stsSettingsApi.TRACES), componentMapping("3", stsSettingsApi.TRACES), componentMapping("4", stsSettingsApi.TRACES)}

	added := topologyConnector.DiffSettings(a, b)
	removed := topologyConnector.DiffSettings(b, a) // symmetric

	assert.ElementsMatch(t, []stsSettingsApi.OtelComponentMapping{componentMapping("1", stsSettingsApi.TRACES)}, added)
	assert.ElementsMatch(t, []stsSettingsApi.OtelComponentMapping{componentMapping("4", stsSettingsApi.TRACES)}, removed)
}

func dummyOnRemovals(_ context.Context, _ []stsSettingsApi.OtelComponentMapping, _ []stsSettingsApi.OtelRelationMapping) {
}
