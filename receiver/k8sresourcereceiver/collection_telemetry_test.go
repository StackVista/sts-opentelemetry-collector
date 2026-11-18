//nolint:testpackage
package k8sresourcereceiver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/collectiondeclaration"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/tracker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
	clocktesting "k8s.io/utils/clock/testing"
)

func TestCollectionTelemetrySharedWatchesAndRetryCadence(t *testing.T) {
	recorder := &recordingReconcileRecorder{}
	r := newCollectionRuntime(testSettings(t), collectionTestConfig(t, nil, nil, nil, nil), nil, nil, recorder)
	r.ctx = context.Background()
	control := collectionWatchSnapshot{
		Spec:  collectionWatchSpec{Key: collectionWatchKey{GVR: collectiondeclaration.GVR()}, Source: ObjectSourceCR},
		State: collectiondeclaration.Active, Reason: "Watching",
	}
	watch := collectionWatchSnapshot{
		Spec:  collectionWatchSpec{Key: collectionWatchKey{GVR: collectionTestWidgetsGVR()}, Source: ObjectSourceCR},
		State: collectiondeclaration.Blocked, Reason: "AccessDenied",
	}
	controls := []collectionWatchSnapshot{control}
	observations := []collectionWatchSnapshot{control, watch, watch}
	r.recordWatchHealth(collectionPlan{}, controls, observations, true)
	assert.Equal(t, []reconcileRecord{
		{kind: metrics.InformerKindControl, outcome: metrics.InformerStarted},
		{kind: metrics.InformerKindCR, outcome: metrics.InformerForbidden},
	}, recorder.snapshot())
	r.recordWatchHealth(collectionPlan{}, controls, observations, false)
	assert.Len(t, recorder.snapshot(), 2, "unchanged one-second polls do not count as recovery attempts")
	r.recordWatchHealth(collectionPlan{}, controls, observations, true)
	assert.Len(t, recorder.snapshot(), 4, "periodic reconciliation records persistent failures")
	r.recordWatchHealth(collectionPlan{}, controls, nil, false)
	r.recordWatchHealth(collectionPlan{}, controls, observations, false)
	assert.Len(t, recorder.snapshot(), 5, "a withdrawn and recreated scope is observed again")
}

func TestCollectionTelemetryDeniedAndUnresolvedSelections(t *testing.T) {
	recorder := &recordingReconcileRecorder{}
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "missing-resources"}}, []ObjectMatcher{
		{Name: "widgets", Group: collectionTestGroup},
	})
	selection := collectionTestSelection("widgets", nil, nil)
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*collectiondeclaration.CollectionDeclaration{
		collectionTestDeclaration("a", selection), collectionTestDeclaration("b", selection),
	})
	r := newCollectionRuntime(testSettings(t), cfg, nil, nil, recorder)
	core, logs := observer.New(zap.InfoLevel)
	r.settings.Logger = zap.New(core)
	r.ctx = context.Background()
	r.recordWatchHealth(plan, nil, nil, true)
	assert.ElementsMatch(t, []reconcileRecord{
		{kind: metrics.InformerKindStatic, outcome: metrics.InformerFailed},
		{kind: metrics.InformerKindCR, outcome: metrics.InformerPolicyDenied},
	}, recorder.snapshot(), "unresolved baseline and shared denied type remain visible without watches")
	r.recordWatchHealth(plan, nil, nil, false)
	assert.Len(t, recorder.snapshot(), 2)
	r.recordWatchHealth(plan, nil, nil, true)
	assert.Equal(t, 1, logs.FilterMessage("Configured collection selection could not be resolved").Len())
	fields := logs.FilterMessage("Configured collection selection could not be resolved").All()[0].ContextMap()
	assert.Equal(t, int64(0), fields["selectionIndex"])
	assert.Equal(t, "missing-resources", fields["resource"])
	assert.False(t, collectionBaselineSynced(plan, nil), "an unresolved required watch cannot prove absence")

	cfg.Objects[0].Name = "pods"
	plan = buildCollectionPlan(cfg, collectionTestDiscovery(), nil)
	r.recordWatchHealth(plan, nil, nil, true)
	r.recordWatchHealth(plan, nil, nil, true)
	assert.Equal(t, 1, logs.FilterMessage("Configured collection selection resolved").Len())
}

func TestCollectionRuntimeTelemetryPermissionRecovery(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "pods"}}, nil)
	declaration := collectionTestDeclaration("integration", collectionTestSelection("widgets", nil, nil))
	client := collectionRuntimeClient(t, collectionRuntimeCRD(),
		collectionTestDeclarationObject(t, declaration), collectionTestWidget("widget", "payments"),
		makeTestPod("baseline", "base", nil))
	var granted atomic.Bool
	client.PrependReactor("list", "widgets", func(clienttesting.Action) (bool, runtime.Object, error) {
		if !granted.Load() {
			return true, nil, apierrors.NewForbidden(collectionTestWidgetsGVR().GroupResource(), "", errors.New("reads not granted"))
		}
		return false, nil, nil
	})
	disc := &collectionRuntimeDiscovery{resources: []*metav1.APIResourceList{
		collectionTestAPI("", "v1", true, "pods"),
		collectionTestAPI(collectionTestGroup, "v1", true, "widgets"),
	}}
	recorder := &recordingReconcileRecorder{}
	core, logs := observer.New(zap.InfoLevel)
	settings := testSettings(t)
	settings.Logger = zap.New(core)
	r := newCollectionRuntime(settings, cfg, client, disc, recorder)
	watchClock := clocktesting.NewFakeClock(time.Now())
	r.watchClock = watchClock
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, r.Start(ctx))
	t.Cleanup(func() {
		cancel()
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		require.NoError(t, r.Shutdown(shutdown))
	})
	collectionRuntimeStatus(t, client, declaration.Name, collectiondeclaration.Blocked)
	require.Eventually(t, func() bool {
		for _, record := range recorder.snapshot() {
			if record.kind == metrics.InformerKindCR && record.outcome == metrics.InformerForbidden {
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		return logs.FilterMessage("Collection watch is unavailable; retaining known objects").Len() == 1
	}, 5*time.Second, 20*time.Millisecond)
	assert.True(t, r.collectionSnapshot().Complete)
	granted.Store(true)
	require.Eventually(t, watchClock.HasWaiters, 5*time.Second, 10*time.Millisecond)
	watchClock.Step(tracker.DefaultRetryInterval)
	collectionRuntimeStatus(t, client, declaration.Name, collectiondeclaration.Active)
	require.Eventually(t, func() bool {
		return logs.FilterMessage("Collection watch recovered").Len() == 1
	}, 5*time.Second, 20*time.Millisecond)
	assert.Equal(t, 1, logs.FilterMessage("Collection watch is unavailable; retaining known objects").Len())
	assert.Contains(t, recorder.snapshot(), reconcileRecord{kind: metrics.InformerKindCR, outcome: metrics.InformerStarted})
	assert.Contains(t, recorder.snapshot(), reconcileRecord{kind: metrics.InformerKindStatic, outcome: metrics.InformerStarted})
}
