package tracetotopoconnector_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	stsTraceToTopo "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector"
	stsSettingsApi "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
)

// Helpers for brevity
func comp(id string) stsSettingsApi.OtelComponentMapping {
	return stsSettingsApi.OtelComponentMapping{Identifier: id}
}

func rel(id string) stsSettingsApi.OtelRelationMapping {
	return stsSettingsApi.OtelRelationMapping{Identifier: id}
}

func TestSnapshotManager_UpdateDetectsChanges(t *testing.T) {
	logger := zap.NewNop()
	manager := stsTraceToTopo.NewSnapshotManager(logger)

	initialComponents := []stsSettingsApi.OtelComponentMapping{
		comp("c1"),
		comp("c2"),
	}
	initialRelations := []stsSettingsApi.OtelRelationMapping{
		rel("r1"),
		rel("r2"),
	}

	change := manager.Update(initialComponents, initialRelations)
	assert.NotEmpty(t, change.AddedComponents)
	assert.Empty(t, change.RemovedComponents)
	assert.NotEmpty(t, change.AddedRelations)
	assert.Empty(t, change.RemovedRelations)

	// The current snapshot should be the same as the initial snapshot change
	components, relations := manager.Current()
	assert.Equal(t, change.AddedComponents, components)
	assert.Equal(t, change.AddedRelations, relations)

	// Second update: add one and remove one
	newComponents := []stsSettingsApi.OtelComponentMapping{
		comp("c2"), // existing
		comp("c3"), // new
	}
	newRelations := []stsSettingsApi.OtelRelationMapping{
		rel("r2"), // existing
		rel("r3"), // new
	}

	change = manager.Update(newComponents, newRelations)

	assert.ElementsMatch(t, []stsSettingsApi.OtelComponentMapping{comp("c3")}, change.AddedComponents)
	assert.ElementsMatch(t, []stsSettingsApi.OtelComponentMapping{comp("c1")}, change.RemovedComponents)

	assert.ElementsMatch(t, []stsSettingsApi.OtelRelationMapping{rel("r3")}, change.AddedRelations)
	assert.ElementsMatch(t, []stsSettingsApi.OtelRelationMapping{rel("r1")}, change.RemovedRelations)
}

func TestSnapshotManager_CurrentReturnsCopy(t *testing.T) {
	logger := zap.NewNop()
	manager := stsTraceToTopo.NewSnapshotManager(logger)

	initialComponents := []stsSettingsApi.OtelComponentMapping{comp("c1")}
	initialRelations := []stsSettingsApi.OtelRelationMapping{rel("r1")}
	manager.Update(initialComponents, initialRelations)

	comps, rels := manager.Current()
	assert.Equal(t, initialComponents, comps)
	assert.Equal(t, initialRelations, rels)

	// Mutate returned slices
	comps[0].Identifier = "mutated"
	rels[0].Identifier = "mutated"

	// Fetch again and verify original data not changed
	comps2, rels2 := manager.Current()
	assert.Equal(t, "c1", comps2[0].Identifier)
	assert.Equal(t, "r1", rels2[0].Identifier)
}

func TestDiffSettings_GenericFunction(t *testing.T) {
	a := []stsSettingsApi.OtelComponentMapping{comp("1"), comp("2"), comp("3")}
	b := []stsSettingsApi.OtelComponentMapping{comp("2"), comp("3"), comp("4")}

	added := stsTraceToTopo.DiffSettings(a, b)
	removed := stsTraceToTopo.DiffSettings(b, a) // symmetric

	assert.ElementsMatch(t, []stsSettingsApi.OtelComponentMapping{comp("1")}, added)
	assert.ElementsMatch(t, []stsSettingsApi.OtelComponentMapping{comp("4")}, removed)
}
