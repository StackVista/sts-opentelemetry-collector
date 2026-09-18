package topologyconnector_test

import (
	"sync"
	"testing"
	"testing/synctest"

	topologyConnector "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

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

func TestSnapshotManager_UpdateWaitsForReferences(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		refs := topologyConnector.NewExpressionRefManager(zap.NewNop(), newTestCELEvaluator(t))
		entered := make(chan struct{})
		release := make(chan struct{})
		var first sync.Once
		listener := snapshotListenerFunc(func(
			signals []settingsproto.OtelInputSignal,
			components map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping,
			relations map[settingsproto.OtelInputSignal][]settingsproto.OtelRelationMapping,
		) {
			first.Do(func() {
				close(entered)
				<-release
			})
			refs.Update(signals, components, relations)
		})
		manager := topologyConnector.NewSnapshotManager(
			zap.NewNop(), []settingsproto.OtelInputSignal{settingsproto.METRICS}, listener,
		)
		component := componentMapping("service", settingsproto.METRICS)
		component.Output.Identifier = sExpr("resource.attributes['service.name']")
		relation := relationMapping("calls", settingsproto.METRICS)
		relation.Output.SourceId = sExpr("datapoint.attributes['client.service']")
		relation.Output.TargetId = sExpr("datapoint.attributes['server.service']")

		returned := make(chan struct{})
		go func() {
			manager.Update(t.Context(), []settingsproto.OtelComponentMapping{component}, nil, dummyOnRemovals)
			close(returned)
		}()
		<-entered
		synctest.Wait()
		select {
		case <-returned:
			t.Error("mapping update returned before its reference summaries were ready")
		default:
		}
		close(release)
		<-returned
		synctest.Wait()

		manager.Update(t.Context(), []settingsproto.OtelComponentMapping{component},
			[]settingsproto.OtelRelationMapping{relation}, dummyOnRemovals)
		require.NotNil(t, refs.Current(settingsproto.METRICS, "service"))
		require.NotNil(t, refs.Current(settingsproto.METRICS, "calls"))
		_, relations := manager.Current(settingsproto.METRICS)
		require.Equal(t, []settingsproto.OtelRelationMapping{relation}, relations)
	})
}

func TestSnapshotManager_ObserverDoesNotBlockMappings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		observer := snapshotListenerFunc(func(
			_ []settingsproto.OtelInputSignal,
			_ map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping,
			_ map[settingsproto.OtelInputSignal][]settingsproto.OtelRelationMapping,
		) {
			close(entered)
			<-release
		})
		manager := topologyConnector.NewSnapshotManager(
			zap.NewNop(), []settingsproto.OtelInputSignal{settingsproto.METRICS}, nil, observer,
		)
		component := componentMapping("service", settingsproto.METRICS)
		returned := make(chan struct{})
		go func() {
			manager.Update(t.Context(), []settingsproto.OtelComponentMapping{component}, nil, dummyOnRemovals)
			components, _ := manager.Current(settingsproto.METRICS)
			if len(components) != 1 || components[0].Identifier != component.Identifier {
				t.Error("current mappings do not contain the service")
			}
			close(returned)
		}()
		<-entered
		synctest.Wait()
		select {
		case <-returned:
		default:
			t.Error("metadata observer blocked access to mappings")
		}
		close(release)
		<-returned
	})
}
