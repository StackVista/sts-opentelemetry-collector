package eventmapper //nolint:testpackage // Ported from the cluster agent's white-box tests.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	obj "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func terminated(name, reason string) v1.ContainerStatus {
	return v1.ContainerStatus{Name: name, State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
		Reason:     reason,
		StartedAt:  obj.Time{Time: time.Unix(firstTimestamp, 0)},
		FinishedAt: obj.Time{Time: time.Unix(laterTimestamp, 0)},
	}}}
}

func testPod(statuses ...v1.ContainerStatus) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: obj.ObjectMeta{Name: "worker-0", Namespace: "jobs", UID: "c0ffee"},
		Spec:       v1.PodSpec{NodeName: "node-2"},
		Status:     v1.PodStatus{ContainerStatuses: statuses},
	}
}

func TestOutOfMemoryEvent(t *testing.T) {
	events := testMapper(t, nil).MapPod(testPod(terminated("worker", "OOMKilled")))
	require.Len(t, events, 1)
	event := events[0]

	assert.Equal(t, "OOMKilled - worker-0 Pod (1x)", event.Title)
	assert.Equal(t, "Container 'worker' was killed due to an out of memory (OOM) condition", event.Text)
	assert.Equal(t, laterTimestamp, event.Ts)
	assert.Equal(t, "node-2-testCluster", event.Host)
	assert.Equal(t, "warning", string(event.AlertType))
	assert.Equal(t, "OOMKilled", event.EventType)
	assert.Equal(t, string(Alerts), event.EventContext.Category)
	assert.Empty(t, event.EventContext.SourceIdentifier, "synthesized events have no Kubernetes event UID")
	assert.Equal(t, []string{"urn:kubernetes:/testCluster:jobs:pod/worker-0"}, event.EventContext.ElementIdentifiers,
		"the synthesized field path does not name a container, so the agent sends no container URN")
	assert.Contains(t, event.Tags, "source_component:")
	assert.NotContains(t, event.Tags, "kube_container_name:worker")
}

func TestOutOfMemoryEventPerContainer(t *testing.T) {
	events := testMapper(t, nil).MapPod(testPod(
		terminated("a", "OOMKilled"),
		terminated("b", "Completed"),
		v1.ContainerStatus{Name: "c", State: v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
		v1.ContainerStatus{
			Name:                 "d",
			State:                v1.ContainerState{Running: &v1.ContainerStateRunning{}},
			LastTerminationState: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{Reason: "OOMKilled"}},
		},
		terminated("e", "OOMKilled"),
	))
	require.Len(t, events, 2, "only containers currently terminated by OOMKilled produce an event")
	assert.Contains(t, events[0].Text, "'a'")
	assert.Contains(t, events[1].Text, "'e'")
}

func TestPodWithoutOutOfMemory(t *testing.T) {
	assert.Empty(t, testMapper(t, nil).MapPod(testPod(terminated("worker", "Completed"))))
	assert.Empty(t, testMapper(t, nil).MapPod(testPod()))
}
