// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2020 Datadog, Inc.

package eventmapper

import (
	"fmt"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/model/telemetry"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	obj "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MapPod is vendored from stackstate-agent kubernetes_events_pods.go. For every
// container whose current state is terminated with reason OOMKilled, it
// synthesizes the event the cluster agent sends, so each pod update in that
// state yields one event per such container.
func (k *Mapper) MapPod(pod *v1.Pod) []telemetry.Event {
	var events []telemetry.Event

	// Test on active Status. This will be the current state the pod is in not the previous state
	for _, containerStatus := range pod.Status.ContainerStatuses {
		// Terminated is an optional as the state can be anything, for example waiting will make terminated nil
		if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.Reason == "OOMKilled" {
			event, err := k.Map(outOfMemoryEvent(pod, containerStatus.Name, containerStatus.State.Terminated))
			if err != nil {
				k.logger.Warn("Could not map pod to an out of memory event", zap.Error(err))
				continue
			}
			events = append(events, event)
		}
	}

	return events
}

func outOfMemoryEvent(pod *v1.Pod, containerName string, terminatedState *v1.ContainerStateTerminated) *v1.Event {
	return &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Name:      pod.Name,
			Kind:      "Pod",
			UID:       pod.UID,
			Namespace: pod.Namespace,
			FieldPath: fmt.Sprintf("{%s %s %s %s %s}", "Pod", pod.Namespace, pod.Name, pod.UID, containerName),
		},
		Count: int32(1),
		Type:  "warning",
		Source: v1.EventSource{
			Host: pod.Spec.NodeName,
		},
		Reason: terminatedState.Reason,
		FirstTimestamp: obj.Time{
			Time: time.Unix(terminatedState.StartedAt.Unix(), 0),
		},
		LastTimestamp: obj.Time{
			Time: time.Unix(terminatedState.FinishedAt.Unix(), 0),
		},
		Message: fmt.Sprintf("Container '%s' was killed due to an out of memory (OOM) condition", containerName),
	}
}
