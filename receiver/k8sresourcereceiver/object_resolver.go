package k8sresourcereceiver

import (
	"fmt"
	"sort"
	"strings"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// resolvedObjectWatch pairs a user-supplied ObjectWatch with its discovery-
// resolved GroupVersionResource and namespaced flag. Constructed only by
// resolveObjectGVRs, so consumers can assume GVR is populated and accurate.
type resolvedObjectWatch struct {
	ObjectWatch
	GVR        schema.GroupVersionResource
	Namespaced bool
}

// resolveObjectGVRs maps each ObjectWatch to a resolvedObjectWatch using the
// supplied discovery client. Returns an error if any watch fails to resolve
// unambiguously, references a missing resource, requests a version the server
// does not expose, or applies namespace selectors to a cluster-scoped resource.
//
// Partial discovery failures (e.g. a broken aggregated API server) do not abort
// resolution as long as every requested resource is still resolvable from the
// returned data. This matches upstream k8sobjectsreceiver behaviour and keeps
// the receiver functional when unrelated extension APIs are unhealthy.
func resolveObjectGVRs(disc discovery.DiscoveryInterface, watches []ObjectWatch, logger *zap.Logger) ([]resolvedObjectWatch, error) {
	if len(watches) == 0 {
		return nil, nil
	}

	preferred, err := disc.ServerPreferredResources()
	if err != nil && !discovery.IsGroupDiscoveryFailedError(err) {
		return nil, fmt.Errorf("failed to list server-preferred resources: %w", err)
	}
	if discovery.IsGroupDiscoveryFailedError(err) && logger != nil {
		logger.Warn("Partial discovery failure; continuing if requested resources still resolve",
			zap.Error(err))
	}

	candidates := indexResourcesByName(preferred)

	resolved := make([]resolvedObjectWatch, 0, len(watches))
	for i := range watches {
		ow := &watches[i]
		gvr, namespaced, err := pickGVR(ow, candidates)
		if err != nil {
			return nil, fmt.Errorf("objects[%d] (%s): %w", i, describeWatch(ow), err)
		}
		if !namespaced && len(ow.Namespaces) > 0 {
			return nil, fmt.Errorf("objects[%d] (%s): resource is cluster-scoped; remove namespaces",
				i, describeWatch(ow))
		}
		resolved = append(resolved, resolvedObjectWatch{
			ObjectWatch: *ow,
			GVR:         gvr,
			Namespaced:  namespaced,
		})
		if logger != nil {
			logger.Info("Resolved static Kubernetes object watch",
				zap.String("name", ow.Name),
				zap.String("gvr", formatGVR(gvr)),
				zap.Bool("namespaced", namespaced),
				zap.Strings("namespaces", ow.Namespaces),
				zap.String("label_selector", ow.LabelSelector),
				zap.String("field_selector", ow.FieldSelector),
			)
		}
	}
	return resolved, nil
}

// resourceCandidate is one server-side resource exposed in some group/version.
// Subresources (containing "/") are dropped before indexing.
type resourceCandidate struct {
	gvr        schema.GroupVersionResource
	namespaced bool
}

// indexResourcesByName flattens discovery output into a name -> candidates map.
// The core API ("v1") is split as Group="" / Version="v1".
func indexResourcesByName(lists []*metav1.APIResourceList) map[string][]resourceCandidate {
	out := map[string][]resourceCandidate{}
	for _, list := range lists {
		if list == nil {
			continue
		}
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			continue
		}
		for _, r := range list.APIResources {
			if strings.Contains(r.Name, "/") {
				continue
			}
			out[r.Name] = append(out[r.Name], resourceCandidate{
				gvr:        gv.WithResource(r.Name),
				namespaced: r.Namespaced,
			})
		}
	}
	return out
}

// pickGVR selects one candidate matching the ObjectWatch's Name + optional
// Group + optional Version. Returns the resolved GVR and its namespaced flag,
// or a descriptive error: missing (with RBAC hint), ambiguous, or version
// mismatch.
//
// Group resolution rules:
//   - Group explicit and non-empty: exact match required.
//   - Group empty (omitted or "") with one candidate: use it.
//   - Group empty with multiple candidates: prefer core (group "") if present.
//     Mirrors kubectl's behaviour for unqualified plurals (`kubectl get pods`
//     always means core pods, never metrics-server pods).
//   - Group empty with multiple non-core candidates: fail with an ambiguity
//     error listing the candidate groups so the user can disambiguate.
func pickGVR(ow *ObjectWatch, candidates map[string][]resourceCandidate) (schema.GroupVersionResource, bool, error) {
	matches := candidates[ow.Name]
	if len(matches) == 0 {
		return schema.GroupVersionResource{}, false, fmt.Errorf(
			"resource %q not found in server discovery (often indicates missing RBAC list permission)",
			ow.Name)
	}

	var filtered []resourceCandidate
	switch {
	case ow.Group != "":
		filtered = filterByGroup(matches, ow.Group)
		if len(filtered) == 0 {
			return schema.GroupVersionResource{}, false, fmt.Errorf(
				"resource %q not found in group %q (known groups: %s)",
				ow.Name, ow.Group, joinGroups(matches))
		}
	case len(matches) == 1:
		filtered = matches
	default:
		core := filterByGroup(matches, "")
		if len(core) == 1 {
			filtered = core
		} else {
			return schema.GroupVersionResource{}, false, fmt.Errorf(
				"resource %q is ambiguous across non-core groups (%s); set group explicitly",
				ow.Name, joinGroups(matches))
		}
	}

	if ow.Version != "" {
		filtered = filterByVersion(filtered, ow.Version)
		if len(filtered) == 0 {
			return schema.GroupVersionResource{}, false, fmt.Errorf(
				"resource %q version %q not exposed by the server", ow.Name, ow.Version)
		}
	}

	return filtered[0].gvr, filtered[0].namespaced, nil
}

func filterByGroup(in []resourceCandidate, group string) []resourceCandidate {
	out := make([]resourceCandidate, 0, len(in))
	for _, c := range in {
		if c.gvr.Group == group {
			out = append(out, c)
		}
	}
	return out
}

func filterByVersion(in []resourceCandidate, version string) []resourceCandidate {
	out := make([]resourceCandidate, 0, len(in))
	for _, c := range in {
		if c.gvr.Version == version {
			out = append(out, c)
		}
	}
	return out
}

// joinGroups returns a sorted, comma-separated list of distinct groups
// (with "" rendered as "core") for use in error messages.
func joinGroups(cands []resourceCandidate) string {
	seen := map[string]struct{}{}
	for _, c := range cands {
		g := c.gvr.Group
		if g == "" {
			g = "core"
		}
		seen[g] = struct{}{}
	}
	groups := make([]string, 0, len(seen))
	for g := range seen {
		groups = append(groups, g)
	}
	sort.Strings(groups)
	return strings.Join(groups, ", ")
}

func describeWatch(ow *ObjectWatch) string {
	g := ow.Group
	if g == "" {
		g = "<unspecified>"
	}
	return fmt.Sprintf("name=%s group=%s", ow.Name, g)
}

func formatGVR(gvr schema.GroupVersionResource) string {
	if gvr.Group == "" {
		return fmt.Sprintf("/%s/%s", gvr.Version, gvr.Resource)
	}
	return fmt.Sprintf("%s/%s/%s", gvr.Group, gvr.Version, gvr.Resource)
}
