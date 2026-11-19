package k8sresourcereceiver

import (
	"fmt"
	"sort"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type collectionDiscoverySnapshot struct {
	PreferredResources []*metav1.APIResourceList
	ServedResources    []*metav1.APIResourceList
	CRDs               []*apiextensionsv1.CustomResourceDefinition
}

type collectionResource struct {
	gvr        schema.GroupVersionResource
	namespaced bool
	source     ObjectSource
}

type collectionCatalog struct {
	resources         map[schema.GroupResource]collectionResource
	baselineResources map[schema.GroupResource]collectionResource
	candidates        map[string][]resourceCandidate
	servedCandidates  map[string][]resourceCandidate
}

type collectionResolutionError struct {
	reason, message string
}

func (e *collectionResolutionError) Error() string { return e.message }

// YAML keeps its preferred/explicit versions; declarations use CRD storage versions.
func newCollectionCatalog(snapshot collectionDiscoverySnapshot) collectionCatalog {
	catalog := collectionCatalog{
		resources:         map[schema.GroupResource]collectionResource{},
		baselineResources: map[schema.GroupResource]collectionResource{},
		candidates:        indexResourcesByName(snapshot.PreferredResources),
		servedCandidates:  indexResourcesByName(snapshot.ServedResources),
	}
	for name, preferred := range catalog.candidates {
		known := map[schema.GroupVersionResource]bool{}
		for _, candidate := range catalog.servedCandidates[name] {
			known[candidate.gvr] = true
		}
		for _, candidate := range preferred {
			if !known[candidate.gvr] {
				// Preferred discovery also proves this exact version is served.
				// Keep explicit served discovery's scope when both sources have it.
				catalog.servedCandidates[name] = append(catalog.servedCandidates[name], candidate)
				known[candidate.gvr] = true
			}
		}
	}
	for _, candidates := range catalog.candidates {
		sort.Slice(candidates, func(i, j int) bool { return formatGVR(candidates[i].gvr) < formatGVR(candidates[j].gvr) })
		for _, candidate := range candidates {
			key := candidate.gvr.GroupResource()
			if _, exists := catalog.resources[key]; !exists {
				catalog.resources[key] = collectionResource{
					gvr: candidate.gvr, namespaced: candidate.namespaced, source: ObjectSourceStatic,
				}
			}
		}
	}
	for _, candidates := range catalog.servedCandidates {
		sort.Slice(candidates, func(i, j int) bool { return formatGVR(candidates[i].gvr) < formatGVR(candidates[j].gvr) })
		for _, candidate := range candidates {
			key := candidate.gvr.GroupResource()
			if _, exists := catalog.resources[key]; !exists {
				// Served discovery proves availability but cannot choose a YAML default.
				catalog.resources[key] = collectionResource{
					namespaced: candidate.namespaced, source: ObjectSourceStatic,
				}
			}
		}
	}
	for _, crd := range snapshot.CRDs {
		if crd == nil || crd.Spec.Group == "" || crd.Spec.Names.Plural == "" {
			continue
		}
		key := schema.GroupResource{Group: crd.Spec.Group, Resource: crd.Spec.Names.Plural}
		resource := catalog.resources[key]
		resource.source = ObjectSourceCR
		catalog.resources[key] = resource
		baseline := collectionResource{
			source: ObjectSourceCR, namespaced: crd.Spec.Scope == apiextensionsv1.NamespaceScoped,
		}
		for _, version := range crd.Spec.Versions {
			if version.Storage && version.Served {
				matches := filterByVersion(filterByGroup(catalog.servedCandidates[key.Resource], key.Group), version.Name)
				if len(matches) != 0 {
					baseline.gvr = key.WithVersion(version.Name)
				}
				break
			}
		}
		catalog.baselineResources[key] = baseline
	}
	return catalog
}

func (c collectionCatalog) resolveBaseline(selection ObjectWatch) (schema.GroupVersionResource, bool, error) {
	candidates := c.candidates
	if selection.Version == "" {
		return pickCollectionGVR(selection, candidates)
	}
	return pickCollectionGVR(selection, c.servedCandidates)
}

func (c collectionCatalog) forDeclaration(key schema.GroupResource) collectionResource {
	return c.baselineResources[key]
}

func pickCollectionGVR(
	selection ObjectWatch, candidates map[string][]resourceCandidate,
) (schema.GroupVersionResource, bool, error) {
	matches := candidates[selection.Name]
	if len(matches) == 0 {
		return schema.GroupVersionResource{}, false, fmt.Errorf("resource %q not found in server discovery", selection.Name)
	}
	group := selection.Group
	if group == "" {
		groups := map[string]bool{}
		for _, candidate := range matches {
			groups[candidate.gvr.Group] = true
		}
		if !groups[""] {
			if len(groups) != 1 {
				return schema.GroupVersionResource{}, false, &collectionResolutionError{
					reason: "AmbiguousResource",
					message: fmt.Sprintf("resource %q is ambiguous across groups (%s); set group explicitly",
						selection.Name, joinGroups(matches)),
				}
			}
			for onlyGroup := range groups {
				group = onlyGroup
			}
		}
	}
	matches = filterByGroup(matches, group)
	if selection.Version != "" {
		matches = filterByVersion(matches, selection.Version)
	}
	if len(matches) == 0 {
		return schema.GroupVersionResource{}, false, fmt.Errorf(
			"resource %q group %q version %q not exposed by the server", selection.Name, group, selection.Version)
	}
	return matches[0].gvr, matches[0].namespaced, nil
}

func (c collectionCatalog) keys() []schema.GroupResource {
	return sortedCollectionResourceKeys(c.resources)
}

func sortedCollectionResourceKeys(resources map[schema.GroupResource]collectionResource) []schema.GroupResource {
	keys := make([]schema.GroupResource, 0, len(resources))
	for key := range resources {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Group != keys[j].Group {
			return keys[i].Group < keys[j].Group
		}
		return keys[i].Resource < keys[j].Resource
	})
	return keys
}
