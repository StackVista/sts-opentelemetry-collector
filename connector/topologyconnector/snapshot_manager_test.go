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
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
)

func componentMapping(id string, signals ...settingsproto.OtelInputSignal) settingsproto.OtelComponentMapping {
	return settingsproto.OtelComponentMapping{Identifier: id, Input: settingsproto.OtelInput{Signal: signals}}
}

func relationMapping(id string, signals ...settingsproto.OtelInputSignal) settingsproto.OtelRelationMapping {
	return settingsproto.OtelRelationMapping{Identifier: id, Input: settingsproto.OtelInput{Signal: signals}}
}

func TestSnapshotManager_StartStopLifecycle(t *testing.T) {
	logger := zap.NewNop()

	// 1. Initialize mock provider with initial settings
	initialComponents := []settingsproto.OtelComponentMapping{componentMapping("c1", settingsproto.TRACES)}
	initialRelations := []settingsproto.OtelRelationMapping{relationMapping("r1", settingsproto.TRACES)}
	provider := topologyConnector.NewMockStsSettingsProvider(initialComponents, initialRelations)

	manager := topologyConnector.NewSnapshotManager(logger, []settingsproto.OtelInputSignal{settingsproto.TRACES})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var removedComponentMappings []settingsproto.OtelComponentMapping
	var removedRelationMappings []settingsproto.OtelRelationMapping

	onRemovals := func(_ context.Context, componentMappings []settingsproto.OtelComponentMapping, relationMappings []settingsproto.OtelRelationMapping) {
		mu.Lock()
		defer mu.Unlock()
		removedComponentMappings = append(removedComponentMappings, componentMappings...)
		removedRelationMappings = append(removedRelationMappings, relationMappings...)
	}

	err := manager.Start(ctx, provider, onRemovals)
	require.NoError(t, err)

	// Verify initial state
	comps, rels := manager.Current(settingsproto.TRACES)
	require.Equal(t, initialComponents, comps)
	require.Equal(t, initialRelations, rels)

	// Update: remove c1/r1 and add c2/r2
	provider.ComponentMappings = []settingsproto.OtelComponentMapping{componentMapping("c2", settingsproto.TRACES)}
	provider.RelationMappings = []settingsproto.OtelRelationMapping{relationMapping("r2", settingsproto.TRACES)}
	provider.SettingUpdatesCh <- stsSettingsEvents.UpdateSettingsEvent{}

	// Verify callback was triggered with removals
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(removedComponentMappings) == 1 && removedComponentMappings[0].Identifier == "c1" &&
			len(removedRelationMappings) == 1 && removedRelationMappings[0].Identifier == "r1"
	}, time.Second, 50*time.Millisecond, "expected removal callback to fire")

	last, err := manager.Stop(ctx)
	require.NoError(t, err)
	require.True(t, last)

	provider.ComponentMappings = []settingsproto.OtelComponentMapping{componentMapping("c3", settingsproto.TRACES)}
	provider.RelationMappings = []settingsproto.OtelRelationMapping{relationMapping("r3", settingsproto.TRACES)}
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
	manager := topologyConnector.NewSnapshotManager(logger, []settingsproto.OtelInputSignal{settingsproto.TRACES})

	initialComponentMappings := []settingsproto.OtelComponentMapping{
		componentMapping("c1", settingsproto.TRACES),
		componentMapping("c2", settingsproto.TRACES),
	}
	initialRelationMappings := []settingsproto.OtelRelationMapping{
		relationMapping("r1", settingsproto.TRACES),
		relationMapping("r2", settingsproto.TRACES),
	}

	var removedComponentMappings []settingsproto.OtelComponentMapping
	var removedRelationMappings []settingsproto.OtelRelationMapping
	onRemovals := func(_ context.Context, cMappings []settingsproto.OtelComponentMapping, rMappings []settingsproto.OtelRelationMapping) {
		removedComponentMappings = cMappings
		removedRelationMappings = rMappings
	}

	manager.Update(context.Background(), initialComponentMappings, initialRelationMappings, onRemovals)
	assert.Empty(t, removedComponentMappings)
	assert.Empty(t, removedRelationMappings)

	// The current snapshot should be the same as the initial snapshot
	componentMappings, relationMappings := manager.Current(settingsproto.TRACES)
	assert.Equal(t, initialComponentMappings, componentMappings)
	assert.Equal(t, initialRelationMappings, relationMappings)

	// Second update: add one and remove one
	newComponentMappings := []settingsproto.OtelComponentMapping{
		componentMapping("c2", settingsproto.TRACES), // existing
		componentMapping("c3", settingsproto.TRACES), // new
	}
	newRelationMappings := []settingsproto.OtelRelationMapping{
		relationMapping("r2", settingsproto.TRACES), // existing
		relationMapping("r3", settingsproto.TRACES), // new
	}

	manager.Update(context.Background(), newComponentMappings, newRelationMappings, onRemovals)
	assert.ElementsMatch(t, []settingsproto.OtelComponentMapping{componentMapping("c1", settingsproto.TRACES)}, removedComponentMappings)
	assert.ElementsMatch(t, []settingsproto.OtelRelationMapping{relationMapping("r1", settingsproto.TRACES)}, removedRelationMappings)
}

func TestSnapshotManager_CurrentReturnsCopy(t *testing.T) {
	logger := zap.NewNop()
	manager := topologyConnector.NewSnapshotManager(logger, []settingsproto.OtelInputSignal{settingsproto.TRACES})

	initialComponentMappings := []settingsproto.OtelComponentMapping{componentMapping("c1", settingsproto.TRACES)}
	initialRelationMappings := []settingsproto.OtelRelationMapping{relationMapping("r1", settingsproto.TRACES)}
	manager.Update(context.Background(), initialComponentMappings, initialRelationMappings, dummyOnRemovals)

	componentMappings, relationMappings := manager.Current(settingsproto.TRACES)
	assert.Equal(t, initialComponentMappings, componentMappings)
	assert.Equal(t, initialRelationMappings, relationMappings)

	// Mutate returned slices
	componentMappings[0].Identifier = "mutated"
	relationMappings[0].Identifier = "mutated"

	// Fetch again and verify original data not changed
	componentMappings2, relationMappings2 := manager.Current(settingsproto.TRACES)
	assert.Equal(t, "c1", componentMappings2[0].Identifier)
	assert.Equal(t, "r1", relationMappings2[0].Identifier)
}

func TestSnapshotManager_MappingsAreFilteredForSignal(t *testing.T) {
	logger := zap.NewNop()
	tracesManager := topologyConnector.NewSnapshotManager(logger, []settingsproto.OtelInputSignal{settingsproto.TRACES, settingsproto.METRICS})
	metricsManager := topologyConnector.NewSnapshotManager(logger, []settingsproto.OtelInputSignal{settingsproto.TRACES, settingsproto.METRICS})

	initialComponentMappings := []settingsproto.OtelComponentMapping{componentMapping("c1", settingsproto.TRACES)}
	initialRelationMappings := []settingsproto.OtelRelationMapping{relationMapping("r1", settingsproto.METRICS)}
	tracesManager.Update(context.Background(), initialComponentMappings, initialRelationMappings, dummyOnRemovals)
	metricsManager.Update(context.Background(), initialComponentMappings, initialRelationMappings, dummyOnRemovals)

	tracesComponentMappings, tracesRelationMappings := tracesManager.Current(settingsproto.TRACES)
	assert.Equal(t, initialComponentMappings, tracesComponentMappings)
	assert.Empty(t, tracesRelationMappings)

	metricsComponentMappings, metricsRelationMappings := metricsManager.Current(settingsproto.METRICS)
	assert.Equal(t, initialRelationMappings, metricsRelationMappings)
	assert.Empty(t, metricsComponentMappings)
}

func TestDiffSettings_GenericFunction(t *testing.T) {
	a := []settingsproto.OtelComponentMapping{componentMapping("1", settingsproto.TRACES), componentMapping("2", settingsproto.TRACES), componentMapping("3", settingsproto.TRACES)}
	b := []settingsproto.OtelComponentMapping{componentMapping("2", settingsproto.TRACES), componentMapping("3", settingsproto.TRACES), componentMapping("4", settingsproto.TRACES)}

	added := topologyConnector.DiffSettings(a, b)
	removed := topologyConnector.DiffSettings(b, a) // symmetric

	assert.ElementsMatch(t, []settingsproto.OtelComponentMapping{componentMapping("1", settingsproto.TRACES)}, added)
	assert.ElementsMatch(t, []settingsproto.OtelComponentMapping{componentMapping("4", settingsproto.TRACES)}, removed)
}

func dummyOnRemovals(_ context.Context, _ []settingsproto.OtelComponentMapping, _ []settingsproto.OtelRelationMapping) {
}

type snapshotListenerFunc func(
	[]settingsproto.OtelInputSignal,
	map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping,
	map[settingsproto.OtelInputSignal][]settingsproto.OtelRelationMapping,
)

func (f snapshotListenerFunc) Update(
	signals []settingsproto.OtelInputSignal,
	components map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping,
	relations map[settingsproto.OtelInputSignal][]settingsproto.OtelRelationMapping,
) {
	f(signals, components, relations)
}

func TestSnapshotManager_UpdatesReferencesInSnapshotOrder(t *testing.T) {
	for _, signal := range []settingsproto.OtelInputSignal{settingsproto.TRACES, settingsproto.METRICS, settingsproto.LOGS} {
		t.Run(string(signal), func(t *testing.T) {
			refs := topologyConnector.NewExpressionRefManager(zap.NewNop(), newTestCELEvaluator(t))
			firstStarted := make(chan struct{})
			releaseFirst := make(chan struct{})
			latestFinished := make(chan struct{})
			listenerFinished := make(chan struct{}, 2)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
			defer release()

			listener := snapshotListenerFunc(func(
				signals []settingsproto.OtelInputSignal,
				components map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping,
				relations map[settingsproto.OtelInputSignal][]settingsproto.OtelRelationMapping,
			) {
				if len(relations[signal]) == 0 {
					close(firstStarted)
					<-releaseFirst
				}
				refs.Update(signals, components, relations)
				if len(relations[signal]) > 0 {
					close(latestFinished)
				}
				listenerFinished <- struct{}{}
			})
			manager := topologyConnector.NewSnapshotManager(zap.NewNop(), []settingsproto.OtelInputSignal{signal}, listener)
			component := componentMapping("component", signal)
			component.Output.Identifier = sExpr(`resource.attributes["service.name"]`)
			relation := relationMapping("relation", signal)
			relation.Output.SourceId = sExpr(`resource.attributes["service.name"]`)
			relation.Output.TargetId = sExpr(`resource.attributes["peer.service"]`)
			components := []settingsproto.OtelComponentMapping{component}

			var updates sync.WaitGroup
			updates.Go(func() {
				manager.Update(t.Context(), components, nil, dummyOnRemovals)
			})
			select {
			case <-firstStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("first reference update did not start")
			}
			currentFinished := make(chan struct{})
			go func() {
				manager.Current(signal)
				close(currentFinished)
			}()
			select {
			case <-currentFinished:
				t.Error("mappings became visible before their references were ready")
			case <-time.After(100 * time.Millisecond):
			}
			updates.Go(func() {
				manager.Update(t.Context(), components, []settingsproto.OtelRelationMapping{relation}, dummyOnRemovals)
			})
			select {
			case <-latestFinished:
				t.Error("newer references were published while the older update was still running")
			case <-time.After(100 * time.Millisecond):
			}
			release()
			updates.Wait()
			<-currentFinished
			for range 2 {
				select {
				case <-listenerFinished:
				case <-time.After(5 * time.Second):
					t.Fatal("reference update did not finish")
				}
			}

			_, currentRelations := manager.Current(signal)
			require.Len(t, currentRelations, 1)
			assert.NotNil(t, refs.Current(signal, relation.Identifier))
			dedup := internal.NewTopologyDeduplicator(t.Context(), zap.NewNop(), internal.DeduplicationConfig{
				Enabled:     true,
				CacheConfig: metrics.MeteredCacheSettings{Size: 10},
			}, refs)
			evalCtx := &internal.ExpressionEvalContext{
				Resource: internal.NewResource(map[string]any{"service.name": "checkout", "peer.service": "payment"}),
			}
			for _, id := range []string{component.Identifier, relation.Identifier} {
				assert.True(t, dedup.ShouldSend(id, signal, evalCtx, time.Minute))
				assert.False(t, dedup.ShouldSend(id, signal, evalCtx, time.Minute), "duplicate output for %s", id)
			}
		})
	}
}
