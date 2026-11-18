package k8sresourcereceiver

import (
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/collectiondeclaration"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type collectionWatchHealth struct {
	State, Reason string
	Failed        bool
}

func (r *collectionRuntime) recordWatchHealth(
	plan collectionPlan, controls, observations []collectionWatchSnapshot, refresh bool,
) {
	next := make(map[collectionWatchKey]collectionWatchHealth, len(controls)+len(observations))
	record := func(observation collectionWatchSnapshot, kind metrics.InformerKind) {
		key := observation.Spec.Key
		if _, recorded := next[key]; recorded {
			return
		}
		health := collectionWatchHealth{State: observation.State, Reason: observation.Reason}
		previous, known := r.watchHealth[key]
		health.Failed = health.State != collectiondeclaration.Active &&
			(previous.Failed || health.Reason != collectionAwaitingObservation)
		next[key] = health
		changed := !known || health != previous
		if refresh || changed {
			outcome := metrics.InformerExists
			switch {
			case health.Reason == "AccessDenied":
				outcome = metrics.InformerForbidden
			case health.Reason == collectionAwaitingObservation:
				outcome = metrics.InformerStarted
				if known {
					outcome = metrics.InformerExists
				}
			case health.State != collectiondeclaration.Active:
				outcome = metrics.InformerFailed
			case !known || previous.State != collectiondeclaration.Active:
				outcome = metrics.InformerStarted
			}
			r.metrics.RecordInformerReconcile(r.ctx, kind, outcome)
		}
		// Control failures already log the retained-selection behavior.
		if !changed || kind == metrics.InformerKindControl {
			return
		}
		fields := []zap.Field{
			zap.String("gvr", formatGVR(key.GVR)), zap.String("namespace", key.Namespace),
			zap.String("reason", health.Reason),
		}
		if health.State != collectiondeclaration.Active && health.Reason != collectionAwaitingObservation {
			r.settings.Logger.Warn("Collection watch is unavailable; retaining known objects",
				append(fields, zap.String("message", observation.Message))...)
		} else if health.State == collectiondeclaration.Active && known && previous.Failed {
			r.settings.Logger.Info("Collection watch recovered", fields...)
		}
	}
	for _, observation := range controls {
		record(observation, metrics.InformerKindControl)
	}
	for _, observation := range observations {
		kind := metrics.InformerKindStatic
		if observation.Spec.Source == ObjectSourceCR {
			kind = metrics.InformerKindCR
		}
		record(observation, kind)
	}
	r.watchHealth = next
	r.recordBaselineHealth(plan)
	if !refresh {
		return
	}
	type unresolvedKey struct {
		Resource schema.GroupResource
		Version  string
		Kind     metrics.InformerKind
		Outcome  metrics.InformerOutcome
	}
	unresolved := map[unresolvedKey]bool{}
	recordSelection := func(selection collectionSelectionResult, kind metrics.InformerKind) {
		recordReason := func(resource schema.GroupResource, version, reason string) {
			if reason == "" || reason == collectionAwaitingObservation {
				return
			}
			outcome := metrics.InformerFailed
			if reason == collectionPolicyDenied {
				outcome = metrics.InformerPolicyDenied
			}
			key := unresolvedKey{Resource: resource, Version: version, Kind: kind, Outcome: outcome}
			if !unresolved[key] {
				unresolved[key] = true
				r.metrics.RecordInformerReconcile(r.ctx, kind, outcome)
			}
		}
		if len(selection.Resolved) == 0 {
			recordReason(schema.GroupResource{Group: selection.Group, Resource: selection.Resource},
				selection.Version, selection.Reason)
		}
		for _, resolved := range selection.Resolved {
			recordReason(resolved.Resource, resolved.GVR.Version, resolved.Reason)
		}
	}
	for _, selection := range plan.Baseline {
		kind := metrics.InformerKindStatic
		if selection.Contributor.Kind == collectionBaselineDiscovery {
			kind = metrics.InformerKindCR
		}
		recordSelection(selection, kind)
	}
	for _, declaration := range plan.Declarations {
		for _, selection := range declaration.Selections {
			recordSelection(selection, metrics.InformerKindCR)
		}
	}
}

func (r *collectionRuntime) recordBaselineHealth(plan collectionPlan) {
	next := map[int]collectionWatchHealth{}
	for _, selection := range plan.Baseline {
		if selection.Contributor.Kind != collectionBaselineObject {
			continue
		}
		index := selection.Contributor.SelectionIndex
		health := collectionWatchHealth{State: selection.State, Reason: selection.Reason}
		previous, known := r.baselineHealth[index]
		next[index] = health
		if known && previous == health {
			continue
		}
		fields := []zap.Field{
			zap.Int("selectionIndex", index), zap.String("group", selection.Group),
			zap.String("resource", selection.Resource), zap.String("version", selection.Version),
			zap.String("reason", selection.Reason),
		}
		if health.Reason != collectionAwaitingObservation && health.Reason != collectionPolicyDenied {
			r.settings.Logger.Warn("Configured collection selection could not be resolved",
				append(fields, zap.String("message", selection.Message))...)
		} else if health.Reason == collectionAwaitingObservation && known &&
			previous.Reason != collectionAwaitingObservation && previous.Reason != collectionPolicyDenied {
			r.settings.Logger.Info("Configured collection selection resolved", fields...)
		}
	}
	r.baselineHealth = next
}
