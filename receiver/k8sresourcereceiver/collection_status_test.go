//nolint:testpackage
package k8sresourcereceiver

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/resourcecollection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

func collectionTestDeclarationObject(t *testing.T, declaration *resourcecollection.ResourceCollection) *unstructured.Unstructured {
	t.Helper()
	declaration.TypeMeta = metav1.TypeMeta{APIVersion: resourcecollection.Group + "/" + resourcecollection.Version, Kind: resourcecollection.Kind}
	declaration.ResourceVersion = "10"
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(declaration)
	require.NoError(t, err)
	return &unstructured.Unstructured{Object: raw}
}

func collectionTestStatusClient(t *testing.T, object *unstructured.Unstructured) *dynamicfake.FakeDynamicClient {
	t.Helper()
	scheme := testScheme()
	registerCRGVR(scheme, resourcecollection.Group, resourcecollection.Version, resourcecollection.Kind)
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		resourcecollection.GVR(): resourcecollection.Kind + "List",
	}, object)
}

func collectionTestStatusResult(name string) collectionDeclarationResult {
	return collectionDeclarationResult{Name: name, UID: types.UID("uid-" + name), Generation: 1,
		Selections: []collectionSelectionResult{{
			Contributor: collectionContributor{SelectionIndex: 0},
			Group:       collectionTestGroup, Resource: "widgets",
			State: resourcecollection.Pending, Reason: "APINotAvailable",
		}}}
}

func TestCollectionStatusPartialWildcardAndBoundedErrors(t *testing.T) {
	declaration := collectionTestDeclaration("integration", collectionTestSelection("*", nil, nil))
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{declaration})
	observations := make([]collectionWatchSnapshot, 0, len(plan.Watches))
	for index, spec := range plan.Watches {
		observation := collectionWatchSnapshot{Spec: spec, Synced: true, State: resourcecollection.Active, Reason: "Watching"}
		switch index {
		case 0:
			observation.State, observation.Reason, observation.Message = resourcecollection.Blocked, "AccessDenied", strings.Repeat("é", 3000)
		case 1:
			observation.Synced, observation.State, observation.Reason = false, resourcecollection.Pending, "AwaitingObservation"
		}
		observations = append(observations, observation)
	}
	status := collectionDeclarationStatus(plan.Declarations[0], observations)
	require.Len(t, status.Resources, 1)
	assert.Equal(t, resourcecollection.Blocked, status.Resources[0].State)
	assert.Contains(t, status.Resources[0].Message, "1 types active, 1 blocked, 1 pending.")
	assert.Equal(t, metav1.ConditionFalse, status.Conditions[0].Status)
	assert.Equal(t, declaration.Generation, status.ObservedGeneration)
	explicit := plan.Declarations[0]
	explicit.Selections[0].Resolved = explicit.Selections[0].Resolved[:1]
	status = collectionDeclarationStatus(explicit, observations)
	assert.LessOrEqual(t, utf8.RuneCountInString(status.Resources[0].Message), 2048)
	assert.True(t, utf8.ValidString(status.Resources[0].Message))

	for index := range observations {
		observations[index].Synced, observations[index].State = true, resourcecollection.Active
	}
	status = collectionDeclarationStatus(plan.Declarations[0], observations)
	assert.Equal(t, metav1.ConditionTrue, status.Conditions[0].Status)
	assert.Equal(t, resourcecollection.Active, status.Resources[0].State)
}

func TestCollectionStatusWritesOnlyChangesAndPreservesSpec(t *testing.T) {
	ctx := context.Background()
	declaration := collectionTestDeclaration("integration", collectionTestSelection("widgets", nil, nil))
	object := collectionTestDeclarationObject(t, declaration)
	client := collectionTestStatusClient(t, object)
	result := collectionTestStatusResult(declaration.Name)
	status := collectionDeclarationStatus(result, nil)
	require.NoError(t, writeCollectionStatus(ctx, client, result, status))
	current, err := client.Resource(resourcecollection.GVR()).Get(ctx, declaration.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, object.Object["spec"], current.Object["spec"])
	assert.Equal(t, int64(1), current.GetGeneration())
	decoded, err := resourcecollection.Decode(current)
	require.NoError(t, err)
	transition := decoded.Status.Conditions[0].LastTransitionTime
	require.False(t, transition.IsZero())

	client.ClearActions()
	require.NoError(t, writeCollectionStatus(ctx, client, result, status))
	for _, action := range client.Actions() {
		assert.NotEqual(t, "patch", action.GetVerb(), "identical feedback must not generate writes")
	}
	status.Conditions[0].Reason = "ResourcesPending"
	status.Conditions[0].Message = "Still waiting."
	require.NoError(t, writeCollectionStatus(ctx, client, result, status))
	current, err = client.Resource(resourcecollection.GVR()).Get(ctx, declaration.Name, metav1.GetOptions{})
	require.NoError(t, err)
	decoded, err = resourcecollection.Decode(current)
	require.NoError(t, err)
	assert.Equal(t, transition, decoded.Status.Conditions[0].LastTransitionTime)
}

func TestCollectionStatusConflictCannotWriteNewGeneration(t *testing.T) {
	declaration := collectionTestDeclaration("integration", collectionTestSelection("widgets", nil, nil))
	object := collectionTestDeclarationObject(t, declaration)
	client := collectionTestStatusClient(t, object)
	result := collectionTestStatusResult(declaration.Name)
	attempts := 0
	client.PrependReactor("patch", "resourcecollections", func(action clienttesting.Action) (bool, runtime.Object, error) {
		attempts++
		assert.Equal(t, "status", action.GetSubresource())
		patch, ok := action.(clienttesting.PatchAction)
		require.True(t, ok)
		var operations []map[string]interface{}
		require.NoError(t, json.Unmarshal(patch.GetPatch(), &operations))
		require.Len(t, operations, 4)
		assert.Equal(t, "/metadata/uid", operations[0]["path"])
		assert.Equal(t, string(result.UID), operations[0]["value"])
		assert.Equal(t, "/metadata/generation", operations[1]["path"])
		assert.Equal(t, float64(result.Generation), operations[1]["value"])
		assert.Equal(t, "/metadata/resourceVersion", operations[2]["path"])
		assert.Equal(t, object.GetResourceVersion(), operations[2]["value"])
		newer := object.DeepCopy()
		newer.SetGeneration(2)
		newer.SetResourceVersion("11")
		require.NoError(t, client.Tracker().Update(resourcecollection.GVR(), newer, ""))
		return true, nil, apierrors.NewConflict(resourcecollection.GVR().GroupResource(), declaration.Name, nil)
	})
	require.NoError(t, writeCollectionStatus(context.Background(), client, result, collectionDeclarationStatus(result, nil)))
	assert.Equal(t, 1, attempts)
	current, err := client.Resource(resourcecollection.GVR()).Get(context.Background(), declaration.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), current.GetGeneration())
	assert.Equal(t, object.Object["status"], current.Object["status"])
}

func TestCollectionStatusRejectsRecreatedIdentityAndCancellation(t *testing.T) {
	for _, test := range []string{"recreated", "canceled-during-get"} {
		t.Run(test, func(t *testing.T) {
			declaration := collectionTestDeclaration("integration", collectionTestSelection("widgets", nil, nil))
			object := collectionTestDeclarationObject(t, declaration)
			if test == "recreated" {
				object.SetUID("replacement-uid")
			}
			client := collectionTestStatusClient(t, object)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if test == "canceled-during-get" {
				client.PrependReactor("get", "resourcecollections", func(clienttesting.Action) (bool, runtime.Object, error) {
					cancel()
					return false, nil, nil
				})
			}
			result := collectionTestStatusResult(declaration.Name)
			err := writeCollectionStatus(ctx, client, result, collectionDeclarationStatus(result, nil))
			if test == "recreated" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
			for _, action := range client.Actions() {
				assert.NotEqual(t, "patch", action.GetVerb())
			}
		})
	}
}

func TestCollectionStatusTransitionOnlyChangesWithReadiness(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	old := resourcecollection.Status{Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse, LastTransitionTime: metav1.NewTime(now)}}}
	next := resourcecollection.Status{Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}}}
	preserveCollectionTransition(&next, old, now.Add(time.Minute))
	assert.Equal(t, now.Add(time.Minute), next.Conditions[0].LastTransitionTime.Time)
}
