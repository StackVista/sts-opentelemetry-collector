package k8sresourcereceiver

import (
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/resourcecollection"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type collectionWatchHealth struct {
	State, Reason string
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
			case health.State != resourcecollection.Active:
				outcome = metrics.InformerFailed
			case !known || previous.State != resourcecollection.Active:
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
		if health.State != resourcecollection.Active && health.Reason != collectionAwaitingObservation {
			r.settings.Logger.Warn("Collection watch is unavailable; retaining known objects",
				append(fields, zap.String("message", observation.Message))...)
		} else if health.State == resourcecollection.Active && known &&
			previous.State != resourcecollection.Active && previous.Reason != collectionAwaitingObservation {
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
