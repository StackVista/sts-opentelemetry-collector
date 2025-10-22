package tracetotopoconnector_test

import (
	"context"
	"sync"
	"testing"
	"time"

	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	stsTraceToTopo "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector"
	stsSettingsApi "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
)

func comp(id string) stsSettingsApi.OtelComponentMapping {
	return stsSettingsApi.OtelComponentMapping{Identifier: id}
}

func rel(id string) stsSettingsApi.OtelRelationMapping {
	return stsSettingsApi.OtelRelationMapping{Identifier: id}
}

func TestSnapshotManager_StartStopLifecycle(t *testing.T) {
	logger := zap.NewNop()

	// 1. Initialize mock provider with initial settings
	initialComponents := []stsSettingsApi.OtelComponentMapping{comp("c1")}
	initialRelations := []stsSettingsApi.OtelRelationMapping{rel("r1")}
	provider := stsTraceToTopo.NewMockStsSettingsProvider(initialComponents, initialRelations)

	manager := stsTraceToTopo.NewSnapshotManager(logger)

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
	provider.ComponentMappings = []stsSettingsApi.OtelComponentMapping{comp("c2")}
	provider.RelationMappings = []stsSettingsApi.OtelRelationMapping{rel("r2")}
	provider.SettingUpdatesCh <- stsSettingsEvents.UpdateSettingsEvent{}

	// Verify callback was triggered with removals
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(removedComponentMappings) == 1 && removedComponentMappings[0].Identifier == "c1" &&
			len(removedRelationMappings) == 1 && removedRelationMappings[0].Identifier == "r1"
	}, time.Second, 50*time.Millisecond, "expected removal callback to fire")

	manager.Stop()

	provider.ComponentMappings = []stsSettingsApi.OtelComponentMapping{comp("c3")}
	provider.RelationMappings = []stsSettingsApi.OtelRelationMapping{rel("r3")}
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
	manager := stsTraceToTopo.NewSnapshotManager(logger)

	initialComponentMappings := []stsSettingsApi.OtelComponentMapping{
		comp("c1"),
		comp("c2"),
	}
	initialRelationMappings := []stsSettingsApi.OtelRelationMapping{
		rel("r1"),
		rel("r2"),
	}

	change := manager.Update(initialComponentMappings, initialRelationMappings)
	assert.Empty(t, change.RemovedComponentMappings)
	assert.Empty(t, change.RemovedRelationMappings)

	// The current snapshot should be the same as the initial snapshot
	componentMappings, relationMappings := manager.Current()
	assert.Equal(t, initialComponentMappings, componentMappings)
	assert.Equal(t, initialRelationMappings, relationMappings)

	// Second update: add one and remove one
	newComponentMappings := []stsSettingsApi.OtelComponentMapping{
		comp("c2"), // existing
		comp("c3"), // new
	}
	newRelationMappings := []stsSettingsApi.OtelRelationMapping{
		rel("r2"), // existing
		rel("r3"), // new
	}

	change = manager.Update(newComponentMappings, newRelationMappings)
	assert.ElementsMatch(t, []stsSettingsApi.OtelComponentMapping{comp("c1")}, change.RemovedComponentMappings)
	assert.ElementsMatch(t, []stsSettingsApi.OtelRelationMapping{rel("r1")}, change.RemovedRelationMappings)
}

func TestSnapshotManager_CurrentReturnsCopy(t *testing.T) {
	logger := zap.NewNop()
	manager := stsTraceToTopo.NewSnapshotManager(logger)

	initialComponentMappings := []stsSettingsApi.OtelComponentMapping{comp("c1")}
	initialRelationMappings := []stsSettingsApi.OtelRelationMapping{rel("r1")}
	manager.Update(initialComponentMappings, initialRelationMappings)

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

func TestDiffSettings_GenericFunction(t *testing.T) {
	a := []stsSettingsApi.OtelComponentMapping{comp("1"), comp("2"), comp("3")}
	b := []stsSettingsApi.OtelComponentMapping{comp("2"), comp("3"), comp("4")}

	added := stsTraceToTopo.DiffSettings(a, b)
	removed := stsTraceToTopo.DiffSettings(b, a) // symmetric

	assert.ElementsMatch(t, []stsSettingsApi.OtelComponentMapping{comp("1")}, added)
	assert.ElementsMatch(t, []stsSettingsApi.OtelComponentMapping{comp("4")}, removed)
}
