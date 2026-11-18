package k8sresourcereceiver

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/collectiondeclaration"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"
)

func collectionDeclarationStatus(
	result collectionDeclarationResult, observations []collectionWatchSnapshot,
) collectiondeclaration.Status {
	watches := map[collectionWatchKey]collectionWatchSnapshot{}
	for _, observation := range observations {
		watches[observation.Spec.Key] = observation
	}
	status := collectiondeclaration.Status{ObservedGeneration: result.Generation}
	state := collectiondeclaration.Active
	if result.Reason != "" {
		state = collectiondeclaration.Blocked
	}
	for _, selection := range result.Selections {
		entry := collectionSelectionStatus(selection, watches)
		status.Resources = append(status.Resources, entry)
		if entry.State == collectiondeclaration.Blocked ||
			(state == collectiondeclaration.Active && entry.State == collectiondeclaration.Pending) {
			state = entry.State
		}
	}
	condition := metav1.Condition{
		Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: result.Generation,
		Reason: "CollectionActive", Message: "All selections are collecting.",
	}
	switch state {
	case collectiondeclaration.Blocked:
		condition.Status, condition.Reason = metav1.ConditionFalse, "CollectionBlocked"
		condition.Message = "One or more selections are blocked; permitted resources continue collecting."
	case collectiondeclaration.Pending:
		condition.Status, condition.Reason = metav1.ConditionFalse, "ResourcesPending"
		condition.Message = "One or more selections are waiting for an API or successful observation."
	}
	if result.Reason != "" {
		condition.Reason, condition.Message = result.Reason, "The declaration is invalid."
	}
	status.Conditions = []metav1.Condition{condition}
	return status
}

func collectionSelectionStatus(
	selection collectionSelectionResult, watches map[collectionWatchKey]collectionWatchSnapshot,
) collectiondeclaration.SelectionStatus {
	entry := collectiondeclaration.SelectionStatus{
		SelectionIndex: selection.Contributor.SelectionIndex,
		Group:          selection.Group, Resource: selection.Resource,
		State: selection.State, Reason: selection.Reason, Message: selection.Message,
	}
	if len(selection.Resolved) == 0 {
		entry.Message = boundedCollectionMessage(entry.Message)
		return entry
	}
	counts := map[string]int{}
	var failures []string
	entry.State, entry.Reason, entry.Message = collectiondeclaration.Active, collectionWatching, ""
	for _, resolved := range selection.Resolved {
		state, reason, message := resolved.State, resolved.Reason, resolved.Message
		if reason == collectionAwaitingObservation {
			state, reason = collectiondeclaration.Active, collectionWatching
			for _, key := range resolved.Watches {
				observation := watches[key]
				if !collectionWatchObserved(observation) {
					failedState := observation.State
					if failedState == "" || failedState == collectiondeclaration.Active {
						failedState = collectiondeclaration.Pending
					}
					if state != collectiondeclaration.Blocked {
						state, reason, message = failedState, observation.Reason, observation.Message
						if reason == "" {
							reason = collectionAwaitingObservation
						}
					}
				}
			}
		}
		counts[state]++
		if state != collectiondeclaration.Active {
			failures = append(failures, fmt.Sprintf("%s (%s)", resolved.Resource.String(), reason))
		}
		if state == collectiondeclaration.Blocked ||
			(entry.State == collectiondeclaration.Active && state == collectiondeclaration.Pending) {
			entry.State, entry.Reason, entry.Message = state, reason, message
		}
	}
	if len(selection.Resolved) > 1 {
		entry.Message = fmt.Sprintf("%d types active, %d blocked, %d pending.",
			counts[collectiondeclaration.Active], counts[collectiondeclaration.Blocked], counts[collectiondeclaration.Pending])
		if len(failures) > 0 {
			total := len(failures)
			if total > 3 {
				failures = failures[:3]
			}
			entry.Message += " " + strings.Join(failures, "; ") + "."
			if total > len(failures) {
				entry.Message += fmt.Sprintf(" %d additional failures.", total-len(failures))
			}
		}
	}
	entry.Message = boundedCollectionMessage(entry.Message)
	return entry
}

func boundedCollectionMessage(message string) string {
	runes := []rune(message)
	if len(runes) > 2048 {
		return string(runes[:2045]) + "..."
	}
	return message
}

func preserveCollectionTransition(
	status *collectiondeclaration.Status, previous collectiondeclaration.Status, now time.Time,
) {
	for index := range status.Conditions {
		condition := &status.Conditions[index]
		condition.LastTransitionTime = metav1.NewTime(now)
		for _, old := range previous.Conditions {
			if old.Type == condition.Type && old.Status == condition.Status && !old.LastTransitionTime.IsZero() {
				condition.LastTransitionTime = old.LastTransitionTime
				break
			}
		}
	}
}

func writeCollectionStatus(
	ctx context.Context, client dynamic.Interface, result collectionDeclarationResult,
	status collectiondeclaration.Status,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, err := client.Resource(collectiondeclaration.GVR()).Get(ctx, result.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if current.GetUID() != result.UID || current.GetGeneration() != result.Generation ||
			current.GetDeletionTimestamp() != nil {
			return nil
		}
		var previous collectiondeclaration.Status
		if raw, found, _ := unstructured.NestedMap(current.Object, "status"); found {
			_ = runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &previous)
		}
		preserveCollectionTransition(&status, previous, time.Now())
		if reflect.DeepEqual(status, previous) {
			return nil
		}
		raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&status)
		if err != nil {
			return err
		}
		current.Object["status"] = raw
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err = client.Resource(collectiondeclaration.GVR()).UpdateStatus(ctx, current, metav1.UpdateOptions{})
		if err == nil || apierrors.IsNotFound(err) {
			return nil
		}
		return err
	})
}
