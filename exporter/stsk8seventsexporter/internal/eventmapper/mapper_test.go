package eventmapper //nolint:testpackage // Ported from the cluster agent's white-box tests.

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	v1 "k8s.io/api/core/v1"
	obj "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8seventsexporter/internal/urn"
)

// The fixtures and assertions below are ported from stackstate-agent
// pkg/collector/corechecks/cluster/kubeapi kubernetes_event_mapper_test.go and
// kubernetes_events_test.go.

var (
	firstTimestamp = int64(709662600)
	laterTimestamp = int64(709662800)

	eventContainer1 = "stackstate-agent"
	eventContainer2 = "cluster-agent"

	event1 = createEvent(2, "default", "dca-789976f5d7-2ljx6", "Pod", "e6417a7f-f566-11e7-9749-0e4863e1cbf4", "default-scheduler", "machine-blue", "Unhealthy", "Liveness probe errored:", firstTimestamp, firstTimestamp, "Alerts", eventContainer1)
	event2 = createEvent(2, "default", "dca-789976f5d7-2ljx6", "Pod", "e6417a7f-f566-11e7-9749-0e4863e1cbf4", "default-scheduler", "machine-blue", "Unhealthy", "Liveness probe errored:", firstTimestamp, laterTimestamp, "Alerts", eventContainer2)
	event3 = createEvent(2, "default", "dca-789976f5d7-2ljx6", "Pod", "e6417a7f-f566-11e7-9749-0e4863e1cbf4", "default-scheduler", "machine-blue", "Unhealthy", "Liveness probe errored:", laterTimestamp, firstTimestamp, "Alerts", "")
)

func createEvent(count int32, namespace, objname, objkind, objuid, component, hostname, reason, message string, firsttimestamp, lasttimestamp int64, alertType, containername string) *v1.Event {
	if containername != "" {
		containername = " spec.containers{" + containername + "}"
	}
	return &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Name:      objname,
			Kind:      objkind,
			UID:       types.UID(objuid),
			Namespace: namespace,
			FieldPath: fmt.Sprintf("{%s %s %s %s %s}", objkind, namespace, objname, objuid, containername),
		},
		Count:          count,
		Type:           alertType,
		Source:         v1.EventSource{Component: component, Host: hostname},
		Reason:         reason,
		FirstTimestamp: obj.Time{Time: time.Unix(firsttimestamp, 0)},
		LastTimestamp:  obj.Time{Time: time.Unix(lasttimestamp, 0)},
		Message:        message,
	}
}

func testMapper(t *testing.T, overrides map[string]EventCategory) *Mapper {
	t.Helper()
	return New(urn.Kubernetes, "testCluster", overrides, zaptest.NewLogger(t))
}

func TestEventTimestamps(t *testing.T) {
	mapper := testMapper(t, nil)

	mEvent1, err := mapper.Map(event1)
	require.NoError(t, err)
	mEvent2, err := mapper.Map(event2)
	require.NoError(t, err)
	mEvent3, err := mapper.Map(event3)
	require.NoError(t, err)

	assert.Equal(t, firstTimestamp, mEvent1.Ts)
	assert.Equal(t, laterTimestamp, mEvent2.Ts)
	assert.Equal(t, firstTimestamp, mEvent3.Ts)
}

func TestContainerNameFromEvent(t *testing.T) {
	assert.Equal(t, eventContainer1, getContainerNameFromEvent(event1))
	assert.Equal(t, eventContainer2, getContainerNameFromEvent(event2))
	assert.Empty(t, getContainerNameFromEvent(event3))
}

func TestEventTags(t *testing.T) {
	mapper := testMapper(t, nil)

	assert.Contains(t, mapper.getTags(event1), "kube_container_name:"+eventContainer1)
	assert.Contains(t, mapper.getTags(event2), "kube_container_name:"+eventContainer2)
	assert.NotContains(t, mapper.getTags(event3), "kube_container_name:")
}

func TestEventElementIdentifiers(t *testing.T) {
	mapper := testMapper(t, nil)

	assert.Equal(t, []string{
		"urn:kubernetes:/testCluster:default:pod/dca-789976f5d7-2ljx6:container/" + eventContainer1,
		"urn:kubernetes:/testCluster:default:pod/dca-789976f5d7-2ljx6",
	}, mapper.externalIdentifierForInvolvedObject(event1))
	assert.Equal(t, []string{
		"urn:kubernetes:/testCluster:default:pod/dca-789976f5d7-2ljx6:container/" + eventContainer2,
		"urn:kubernetes:/testCluster:default:pod/dca-789976f5d7-2ljx6",
	}, mapper.externalIdentifierForInvolvedObject(event2))
	assert.Equal(t, []string{
		"urn:kubernetes:/testCluster:default:pod/dca-789976f5d7-2ljx6",
	}, mapper.externalIdentifierForInvolvedObject(event3))
}

func TestEventsOverride(t *testing.T) {
	// DarkMystery: V0ID from the agent test is rejected by config validation
	// here, so the mapper only sees valid overrides.
	mapper := testMapper(t, map[string]EventCategory{
		"MysteryActivity":      Activities,
		"MysteryChange":        Changes,
		"MysteryAlert":         Alerts,
		"EnsuringLoadBalancer": Activities,
	})
	createEventShort := func(reason, alertType, message string) *v1.Event {
		return createEvent(1, "default", "pearl-789976f5d7-2ljx6", "Pod", "e6417a7f-f566-11e7-9749-0e4863e1cbf4", "component1", "pearl.host", reason, message, 709662600, 709662600, alertType, "")
	}
	for _, tc := range []struct {
		event    *v1.Event
		category EventCategory
	}{
		{createEventShort("MysteryActivity", "normal", "one"), Activities},
		{createEventShort("MysteryActivity", "warning", "two"), Activities},
		{createEventShort("MysteryChange", "normal", "three"), Changes},
		{createEventShort("MysteryAlert", "normal", "four"), Alerts},
		{createEventShort("DarkMystery", "normal", "five"), Others},
		{createEventShort("EnsuringLoadBalancer", "normal", "six"), Activities},
		{createEventShort("Pulled", "warning", "seven"), Alerts},
		{createEventShort("Pulled", "normal", "eight"), Activities},
	} {
		t.Run(tc.event.Message, func(t *testing.T) {
			mapped, err := mapper.Map(tc.event)
			require.NoError(t, err)
			assert.Equal(t, string(tc.category), mapped.EventContext.Category)
		})
	}
}

func TestEventFields(t *testing.T) {
	mapped, err := testMapper(t, nil).Map(event1)
	require.NoError(t, err)

	assert.Equal(t, "Unhealthy - dca-789976f5d7-2ljx6 Pod (2x)", mapped.Title)
	assert.Equal(t, "Liveness probe errored:", mapped.Text)
	assert.Equal(t, "machine-blue-testCluster", mapped.Host)
	assert.Equal(t, "normal", string(mapped.Priority))
	assert.Equal(t, "info", string(mapped.AlertType), "an unrecognised event type is info")
	assert.Equal(t, "kubernetes", mapped.SourceTypeName)
	assert.Equal(t, "Unhealthy", mapped.EventType)
	assert.Equal(t, []string{
		"kube_namespace:default",
		"kube_container_name:" + eventContainer1,
		"source_component:default-scheduler",
		"kube_object_name:dca-789976f5d7-2ljx6",
		"kube_object_kind:Pod",
		"kube_cluster_name:testCluster",
		"kube_reason:Unhealthy",
		"alert_type:info",
	}, mapped.Tags)
	assert.Equal(t, "kubernetes", mapped.EventContext.Source)
	assert.NotNil(t, mapped.EventContext.SourceLinks)
	assert.Empty(t, mapped.EventContext.SourceLinks)
}

func TestIncompleteEventIsRejected(t *testing.T) {
	event := createEvent(1, "default", "", "Pod", "uid", "kubelet", "node", "Started", "", firstTimestamp, firstTimestamp, "normal", "")
	_, err := testMapper(t, nil).Map(event)
	assert.Error(t, err)
}

func TestOpenShiftSourceType(t *testing.T) {
	mapper := New(urn.OpenShift, "testCluster", nil, zaptest.NewLogger(t))
	mapped, err := mapper.Map(event3)
	require.NoError(t, err)

	assert.Equal(t, "openshift", mapper.SourceType())
	assert.Equal(t, "openshift", mapped.SourceTypeName)
	assert.Equal(t, []string{"urn:openshift:/testCluster:default:pod/dca-789976f5d7-2ljx6"}, mapped.EventContext.ElementIdentifiers)
}
