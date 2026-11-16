//nolint:testpackage
package k8sresourcereceiver

import (
	"encoding/json"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/resourcecollection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

const collectionTestGroup = "collection-test.example.com"

func collectionTestConfig(t *testing.T, includes, excludes []string, objects []ObjectWatch, denied []ObjectMatcher) *Config {
	t.Helper()
	cfg := &Config{
		ResourceCollections:     ResourceCollectionsConfig{Enabled: true},
		CustomResourceAPIGroups: &APIGroups{Include: includes, Exclude: excludes},
		Objects:                 objects,
		DeniedObjects:           denied,
	}
	require.NoError(t, cfg.Validate())
	return cfg
}

func collectionTestAPI(group, version string, namespaced bool, resources ...string) *metav1.APIResourceList {
	result := &metav1.APIResourceList{GroupVersion: schema.GroupVersion{Group: group, Version: version}.String()}
	for _, resource := range resources {
		result.APIResources = append(result.APIResources, metav1.APIResource{Name: resource, Namespaced: namespaced})
	}
	return result
}

func collectionTestCRD(resource string, scope apiextensionsv1.ResourceScope, versions ...string) *apiextensionsv1.CustomResourceDefinition {
	result := &apiextensionsv1.CustomResourceDefinition{Spec: apiextensionsv1.CustomResourceDefinitionSpec{
		Group: collectionTestGroup, Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: resource}, Scope: scope,
	}}
	for index, version := range versions {
		result.Spec.Versions = append(result.Spec.Versions, apiextensionsv1.CustomResourceDefinitionVersion{
			Name: version, Served: true, Storage: index == 0,
		})
	}
	return result
}

func collectionTestDiscovery() collectionDiscoverySnapshot {
	preferred := []*metav1.APIResourceList{
		collectionTestAPI("", "v1", true, "pods", "services"),
		collectionTestAPI(collectionTestGroup, "v1", true, "widgets", "gadgets", "widgets/status"),
		collectionTestAPI(collectionTestGroup, "v1", false, "fleets"),
	}
	return collectionDiscoverySnapshot{
		PreferredResources: preferred, ServedResources: preferred,
		CRDs: []*apiextensionsv1.CustomResourceDefinition{
			collectionTestCRD("widgets", apiextensionsv1.NamespaceScoped, "v1"),
			collectionTestCRD("gadgets", apiextensionsv1.NamespaceScoped, "v1"),
			collectionTestCRD("fleets", apiextensionsv1.ClusterScoped, "v1"),
		},
	}
}

func collectionTestDeclaration(name string, selections ...resourcecollection.Selection) *resourcecollection.ResourceCollection {
	return &resourcecollection.ResourceCollection{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name), Generation: 1},
		Spec:       resourcecollection.Spec{Resources: selections},
	}
}

func collectionTestSelection(resource string, namespaces []string, labels map[string]string) resourcecollection.Selection {
	selection := resourcecollection.Selection{
		Group: collectionTestGroup, Resource: resource, Namespaces: namespaces,
	}
	if labels != nil {
		selection.LabelSelector = &metav1.LabelSelector{MatchLabels: labels}
	}
	return selection
}

func TestCollectionPlanOverlapAndContributionRemoval(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{
		Name: "widgets", Group: collectionTestGroup, Namespaces: []string{"team-a"},
		LabelSelector: "monitoring=enabled",
	}}, nil)
	a := collectionTestDeclaration("a", collectionTestSelection("widgets", nil, nil))
	b := collectionTestDeclaration("b", collectionTestSelection("widgets", []string{"team-a"}, map[string]string{"monitoring": "enabled"}))
	full := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{b, a})
	require.Len(t, full.Watches, 2)
	require.Len(t, full.Watches[1].Contributors, 2)
	assert.Equal(t, collectionBaselineObject, full.Watches[1].Contributors[0].Kind)
	assert.Equal(t, "b", full.Watches[1].Contributors[1].Declaration)

	afterA := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{b})
	require.Len(t, afterA.Watches, 1)
	assert.Equal(t, full.Watches[1], afterA.Watches[0])
	afterB := buildCollectionPlan(cfg, collectionTestDiscovery(), nil)
	require.Len(t, afterB.Watches, 1)
	assert.Equal(t, afterA.Watches[0].Key, afterB.Watches[0].Key)
	assert.Len(t, afterB.Watches[0].Contributors, 1)
}

func TestCollectionPlanPreservesNamespaceSelectorClauses(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	declaration := collectionTestDeclaration("crossed",
		collectionTestSelection("widgets", []string{"team-a"}, map[string]string{"app": "alpha"}),
		collectionTestSelection("widgets", []string{"team-b"}, map[string]string{"app": "beta"}),
	)
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{declaration})
	require.Len(t, plan.Watches, 2)
	assert.Equal(t, "team-a", plan.Watches[0].Key.Namespace)
	assert.Equal(t, "app=alpha", plan.Watches[0].Key.LabelSelector)
	assert.Equal(t, "team-b", plan.Watches[1].Key.Namespace)
	assert.Equal(t, "app=beta", plan.Watches[1].Key.LabelSelector)
	for index, watch := range plan.Watches {
		require.Len(t, watch.Contributors, 1)
		assert.Equal(t, index, watch.Contributors[0].SelectionIndex)
	}
}

func TestCollectionPlanSharesCanonicalExactWatches(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{
		{Name: "widgets", Group: collectionTestGroup, Namespaces: []string{"team-a"}, LabelSelector: "env=prod,app=web", FieldSelector: "metadata.name=web,metadata.namespace=team-a"},
		{Name: "widgets", Group: collectionTestGroup, Namespaces: []string{"team-a"}, LabelSelector: "app=web,env=prod", FieldSelector: "metadata.namespace=team-a,metadata.name=web"},
	}, nil)
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), nil)
	require.Len(t, plan.Watches, 1)
	assert.Len(t, plan.Watches[0].Contributors, 2)
	assert.Equal(t, "app=web,env=prod", plan.Watches[0].Key.LabelSelector)
	assert.Equal(t, "metadata.name=web,metadata.namespace=team-a", plan.Watches[0].Key.FieldSelector)
	cr := collectionTestDeclaration("widgets", resourcecollection.Selection{
		Group: collectionTestGroup, Resource: "widgets", Namespaces: []string{"team-a"},
		LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"env": "prod", "app": "web"}},
	})
	withCR := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{cr})
	assert.Len(t, withCR.Watches, 2)
}

func TestCollectionPlanNamespacesExpandWithoutChangingInputs(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	cr := collectionTestDeclaration("scoped", collectionTestSelection("widgets", []string{"team-b", "team-a", "team-a"}, nil))
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{cr})
	require.Len(t, plan.Watches, 2)
	assert.Equal(t, "team-a", plan.Watches[0].Key.Namespace)
	assert.Equal(t, "team-b", plan.Watches[1].Key.Namespace)
	assert.Equal(t, []string{"team-b", "team-a", "team-a"}, cr.Spec.Resources[0].Namespaces)
	assert.Len(t, plan.Declarations[0].Selections[0].Resolved[0].Watches, 2)
}

func TestCollectionPlanPolicyOverridesBaselineAndDeclarations(t *testing.T) {
	cfg := collectionTestConfig(t, []string{collectionTestGroup}, nil, nil, []ObjectMatcher{{Group: collectionTestGroup, Name: "widgets"}})
	a := collectionTestDeclaration("a", collectionTestSelection("*", nil, nil))
	b := collectionTestDeclaration("b", collectionTestSelection("widgets", nil, nil))
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{a, b})
	require.Len(t, plan.Watches, 2)
	for _, watch := range plan.Watches {
		assert.NotEqual(t, "widgets", watch.Key.GVR.Resource)
		assert.Equal(t, ObjectSourceCR, watch.Source)
	}
	for _, declaration := range plan.Declarations {
		assert.Equal(t, resourcecollection.Blocked, declaration.Selections[0].State)
		assert.Equal(t, "PolicyDenied", declaration.Selections[0].Reason)
	}
	assert.Contains(t, plan.Declarations[0].Selections[0].Message, "widgets")
	missing := collectionDiscoverySnapshot{}
	deniedWithoutAPI := buildCollectionPlan(cfg, missing, []*resourcecollection.ResourceCollection{b})
	assert.Equal(t, "PolicyDenied", deniedWithoutAPI.Declarations[0].Selections[0].Reason)
}

func TestCollectionPlanBaselineFiltersDoNotVetoDeclarations(t *testing.T) {
	for _, excludes := range [][]string{nil, {collectionTestGroup}} {
		cfg := collectionTestConfig(t, []string{"other.example.com"}, excludes, nil, nil)
		cr := collectionTestDeclaration("explicit", collectionTestSelection("widgets", nil, nil))
		plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{cr})
		require.Len(t, plan.Watches, 1)
		assert.Equal(t, ObjectSourceCR, plan.Watches[0].Source)
		assert.Equal(t, "widgets", plan.Watches[0].Key.GVR.Resource)
	}
	cfg := collectionTestConfig(t, []string{collectionTestGroup}, []string{collectionTestGroup}, nil, nil)
	cr := collectionTestDeclaration("wildcard", collectionTestSelection("*", nil, nil))
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{cr})
	assert.Empty(t, plan.Baseline)
	assert.Len(t, plan.Watches, 3)
}

func TestCollectionPlanDeclarationsRejectNativeAndAggregatedResources(t *testing.T) {
	discovery := collectionTestDiscovery()
	discovery.PreferredResources = append(discovery.PreferredResources,
		collectionTestAPI("apps", "v1", true, "deployments"),
		collectionTestAPI("metrics.example.com", "v1", true, "pods"),
	)
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "pods"}, {Name: "deployments", Group: "apps"}}, nil)
	cr := collectionTestDeclaration("custom-only",
		resourcecollection.Selection{Group: "", Resource: "pods"},
		resourcecollection.Selection{Group: "apps", Resource: "deployments"},
		resourcecollection.Selection{Group: "metrics.example.com", Resource: "pods"},
		resourcecollection.Selection{Group: "metrics.example.com", Resource: "*"},
		collectionTestSelection("widgets", nil, nil),
	)
	plan := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{cr})
	require.Len(t, plan.Watches, 3)
	results := plan.Declarations[0].Selections
	assert.Equal(t, "InvalidSelection", results[0].Reason)
	for _, index := range []int{1, 2} {
		assert.Equal(t, resourcecollection.Blocked, results[index].State)
		assert.Equal(t, "NotCustomResource", results[index].Reason)
	}
	assert.Equal(t, "APINotAvailable", results[3].Reason)
	assert.Equal(t, "AwaitingObservation", results[4].Reason)
	for _, spec := range plan.Watches {
		if spec.Source == ObjectSourceStatic {
			require.Len(t, spec.Contributors, 1)
			assert.Equal(t, collectionBaselineObject, spec.Contributors[0].Kind)
		}
	}
}

func TestCollectionPlanWildcardAddsOnlyCRDTypes(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	cr := collectionTestDeclaration("wildcard", collectionTestSelection("*", nil, nil))
	discovery := collectionTestDiscovery()
	discovery.PreferredResources = append(discovery.PreferredResources,
		collectionTestAPI(collectionTestGroup, "v1", true, "builtins"),
		collectionTestAPI("unrelated.example.com", "v1", true, "others"),
	)
	before := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{cr})
	require.Len(t, before.Watches, 3)
	for _, watch := range before.Watches {
		assert.NotEqual(t, "builtins", watch.Key.GVR.Resource)
		assert.Equal(t, ObjectSourceCR, watch.Source)
	}
	discovery.CRDs = append(discovery.CRDs, collectionTestCRD("gizmos", apiextensionsv1.NamespaceScoped, "v1"))
	discovery.PreferredResources = append(discovery.PreferredResources, collectionTestAPI(collectionTestGroup, "v1", true, "gizmos"))
	after := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{cr})
	assert.Len(t, after.Watches, 4)
	assert.Len(t, after.Declarations[0].Selections[0].Resolved, 4)
}

func TestCollectionPlanMissingAPIsAndRuntimeScopes(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "missing", Group: collectionTestGroup}}, nil)
	cr := collectionTestDeclaration("mixed",
		collectionTestSelection("missing", nil, nil),
		collectionTestSelection("fleets", []string{"team-a"}, nil),
		collectionTestSelection("widgets", nil, nil),
	)
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{cr})
	require.Len(t, plan.Watches, 1)
	assert.Equal(t, "widgets", plan.Watches[0].Key.GVR.Resource)
	assert.Equal(t, "APINotAvailable", plan.Baseline[0].Reason)
	results := plan.Declarations[0].Selections
	assert.Equal(t, resourcecollection.Pending, results[0].State)
	assert.Equal(t, "APINotAvailable", results[0].Reason)
	assert.Equal(t, resourcecollection.Blocked, results[1].State)
	assert.Equal(t, "IncompatibleScope", results[1].Reason)
	assert.Equal(t, resourcecollection.Pending, results[2].State)
	assert.Equal(t, "AwaitingObservation", results[2].Reason)
}

func TestCollectionPlanStorageVersionDoesNotRequirePreferredDiscovery(t *testing.T) {
	discovery := collectionDiscoverySnapshot{
		ServedResources: []*metav1.APIResourceList{collectionTestAPI(collectionTestGroup, "v1", true, "widgets")},
		CRDs:            []*apiextensionsv1.CustomResourceDefinition{collectionTestCRD("widgets", apiextensionsv1.NamespaceScoped, "v1")},
	}
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{
		Name: "widgets", Group: collectionTestGroup, Namespaces: []string{"team-a"},
	}}, nil)
	cr := collectionTestDeclaration("waiting", resourcecollection.Selection{
		Group: collectionTestGroup, Resource: "widgets", Namespaces: []string{"team-a"},
	})
	plan := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{cr})
	require.Len(t, plan.Watches, 1)
	assert.Equal(t, resourcecollection.Pending, plan.Baseline[0].State)
	assert.Equal(t, "APINotAvailable", plan.Baseline[0].Reason)
	assert.Equal(t, resourcecollection.Pending, plan.Declarations[0].Selections[0].State)
	assert.Equal(t, "AwaitingObservation", plan.Declarations[0].Selections[0].Reason)
	assert.Equal(t, "team-a", plan.Watches[0].Key.Namespace)
	discovery.PreferredResources = discovery.ServedResources
	available := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{cr})
	require.Len(t, available.Watches, 1)
	assert.Equal(t, "team-a", available.Watches[0].Key.Namespace)
	assert.Len(t, available.Watches[0].Contributors, 2)
}

func TestCollectionPlanWildcardPartialScopeAndMissingPreferred(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	cr := collectionTestDeclaration("partial", collectionTestSelection("*", []string{"team-a"}, nil))
	discovery := collectionTestDiscovery()
	discovery.CRDs = append(discovery.CRDs, collectionTestCRD("late", apiextensionsv1.NamespaceScoped, "v1"))
	plan := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{cr})
	require.Len(t, plan.Watches, 2)
	assert.Equal(t, "IncompatibleScope", plan.Declarations[0].Selections[0].Reason)
	for _, result := range plan.Declarations[0].Selections[0].Resolved {
		if result.Resource.Resource == "late" {
			assert.Equal(t, resourcecollection.Pending, result.State)
			assert.Equal(t, "APINotAvailable", result.Reason)
		}
	}
}

func TestCollectionPlanStorageVersionAndIndependentYAMLDefaultsAndPins(t *testing.T) {
	discovery := collectionDiscoverySnapshot{
		PreferredResources: []*metav1.APIResourceList{collectionTestAPI(collectionTestGroup, "v2", true, "widgets")},
		ServedResources: []*metav1.APIResourceList{
			collectionTestAPI(collectionTestGroup, "v1", true, "widgets"),
			collectionTestAPI(collectionTestGroup, "v2", true, "widgets"),
			collectionTestAPI(collectionTestGroup, "v3", true, "widgets"),
		},
		CRDs: []*apiextensionsv1.CustomResourceDefinition{collectionTestCRD("widgets", apiextensionsv1.NamespaceScoped, "v1", "v2", "v3")},
	}
	a := collectionTestDeclaration("a", collectionTestSelection("*", nil, nil))
	b := collectionTestDeclaration("b", collectionTestSelection("widgets", nil, nil))
	for _, tc := range []struct {
		name     string
		includes []string
		objects  []ObjectWatch
		versions []string
	}{
		{name: "storage rather than preferred", versions: []string{"v1"}},
		{name: "baseline storage", includes: []string{collectionTestGroup}, versions: []string{"v1"}},
		{name: "static preferred default", objects: []ObjectWatch{{Name: "widgets", Group: collectionTestGroup}}, versions: []string{"v1", "v2"}},
		{name: "static nonpreferred pin", includes: []string{collectionTestGroup}, objects: []ObjectWatch{{Name: "widgets", Group: collectionTestGroup, Version: "v3"}}, versions: []string{"v1", "v3"}},
		{name: "static unqualified pin", objects: []ObjectWatch{{Name: "widgets", Version: "v3"}}, versions: []string{"v1", "v3"}},
		{name: "unavailable pin does not affect storage", objects: []ObjectWatch{{Name: "widgets", Group: collectionTestGroup, Version: "v9"}}, versions: []string{"v1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := collectionTestConfig(t, tc.includes, nil, tc.objects, nil)
			full := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{b, a})
			reordered := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{a, b})
			require.Equal(t, full, reordered)
			removed := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{b})
			require.Len(t, full.Watches, len(tc.versions))
			require.Len(t, removed.Watches, len(tc.versions))
			for i, version := range tc.versions {
				assert.Equal(t, version, full.Watches[i].Key.GVR.Version)
				assert.Equal(t, full.Watches[i].Key, removed.Watches[i].Key)
				assert.Equal(t, ObjectSourceCR, removed.Watches[i].Source)
				for _, contributor := range full.Watches[i].Contributors {
					if contributor.Kind == collectionDeclaration {
						assert.Equal(t, "v1", version)
					}
					if contributor.Kind == collectionBaselineDiscovery {
						assert.Equal(t, "v1", version)
					}
				}
			}
			if len(tc.objects) > 0 && tc.objects[0].Version == "v9" {
				assert.Equal(t, resourcecollection.Pending, full.Baseline[0].State)
				assert.Equal(t, "v9", full.Baseline[0].Version)
				assert.Equal(t, "APINotAvailable", full.Baseline[0].Reason)
			}
		})
	}
}

func TestCollectionPlanMultipleBaselinePinsCoexist(t *testing.T) {
	discovery := collectionDiscoverySnapshot{
		PreferredResources: []*metav1.APIResourceList{collectionTestAPI(collectionTestGroup, "v2", true, "widgets")},
		ServedResources: []*metav1.APIResourceList{
			collectionTestAPI(collectionTestGroup, "v1", true, "widgets"),
			collectionTestAPI(collectionTestGroup, "v2", true, "widgets"),
		},
		CRDs: []*apiextensionsv1.CustomResourceDefinition{collectionTestCRD("widgets", apiextensionsv1.NamespaceScoped, "v1", "v2")},
	}
	objects := []ObjectWatch{
		{Name: "widgets", Group: collectionTestGroup, Version: "v1"},
		{Name: "widgets", Group: collectionTestGroup, Version: "v2"},
	}
	cfg := collectionTestConfig(t, []string{collectionTestGroup}, nil, objects, nil)
	cr := collectionTestDeclaration("widgets", collectionTestSelection("widgets", nil, nil))
	plan := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{cr})
	require.Len(t, plan.Watches, 2)
	assert.Equal(t, "v1", plan.Watches[0].Key.GVR.Version)
	assert.Len(t, plan.Watches[0].Contributors, 3)
	assert.Equal(t, "v2", plan.Watches[1].Key.GVR.Version)
	assert.Len(t, plan.Watches[1].Contributors, 1)
	for _, result := range plan.Baseline {
		assert.Equal(t, "AwaitingObservation", result.Reason)
	}
	assert.Equal(t, "AwaitingObservation", plan.Declarations[0].Selections[0].Reason)
	cfg = collectionTestConfig(t, []string{collectionTestGroup}, nil, []ObjectWatch{objects[1], objects[0]}, nil)
	reordered := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{cr})
	require.Len(t, reordered.Watches, 2)
	for i := range plan.Watches {
		assert.Equal(t, plan.Watches[i].Key, reordered.Watches[i].Key)
		assert.Equal(t, plan.Watches[i].Source, reordered.Watches[i].Source)
		assert.Len(t, reordered.Watches[i].Contributors, len(plan.Watches[i].Contributors))
	}
	assert.Equal(t, plan.Declarations, reordered.Declarations)
}

func TestCollectionPlanOmittedVersionsKeepPreferredScopeBesidePins(t *testing.T) {
	discovery := collectionDiscoverySnapshot{
		PreferredResources: []*metav1.APIResourceList{
			collectionTestAPI("aggregated.example.com", "v2", false, "widgets"),
		},
		ServedResources: []*metav1.APIResourceList{
			collectionTestAPI("aggregated.example.com", "v1", true, "widgets"),
			collectionTestAPI("aggregated.example.com", "v2", false, "widgets"),
		},
	}
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{
		{Name: "widgets", Group: "aggregated.example.com"},
		{Name: "widgets", Group: "aggregated.example.com", Version: "v1", Namespaces: []string{"team-a"}},
	}, nil)
	plan := buildCollectionPlan(cfg, discovery, nil)
	require.Len(t, plan.Watches, 2)
	assert.Equal(t, "v1", plan.Watches[0].Key.GVR.Version)
	assert.Equal(t, "team-a", plan.Watches[0].Key.Namespace)
	assert.True(t, plan.Watches[0].Namespaced)
	assert.Equal(t, "v2", plan.Watches[1].Key.GVR.Version)
	assert.Empty(t, plan.Watches[1].Key.Namespace)
	assert.False(t, plan.Watches[1].Namespaced)
	for _, watch := range plan.Watches {
		assert.Equal(t, ObjectSourceStatic, watch.Source)
		assert.Len(t, watch.Contributors, 1)
	}
	assert.Equal(t, "v2", plan.Baseline[0].Resolved[0].GVR.Version)
	cfg = collectionTestConfig(t, nil, nil, cfg.Objects[:1], nil)
	afterRemoval := buildCollectionPlan(cfg, discovery, nil)
	require.Len(t, afterRemoval.Watches, 1)
	assert.Equal(t, plan.Watches[1], afterRemoval.Watches[0])
}

func TestCollectionPlanLegacyGroupAmbiguityAndCorePreference(t *testing.T) {
	discovery := collectionDiscoverySnapshot{
		PreferredResources: []*metav1.APIResourceList{
			collectionTestAPI("one.example.com", "v1", true, "widgets"),
			collectionTestAPI("two.example.com", "v1", true, "widgets"),
		},
		ServedResources: []*metav1.APIResourceList{
			collectionTestAPI("one.example.com", "v1", true, "widgets"),
			collectionTestAPI("one.example.com", "v1beta1", true, "widgets"),
			collectionTestAPI("two.example.com", "v1", true, "widgets"),
		},
	}
	for _, version := range []string{"", "v1"} {
		cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "widgets", Version: version}}, nil)
		plan := buildCollectionPlan(cfg, discovery, nil)
		assert.Empty(t, plan.Watches)
		assert.Equal(t, resourcecollection.Blocked, plan.Baseline[0].State)
		assert.Equal(t, "AmbiguousResource", plan.Baseline[0].Reason)
	}
	discovery.PreferredResources = append(discovery.PreferredResources, collectionTestAPI("", "v1", true, "widgets"))
	discovery.ServedResources = append(discovery.ServedResources, collectionTestAPI("", "v1", true, "widgets"))
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "widgets", Version: "v1"}}, nil)
	plan := buildCollectionPlan(cfg, discovery, nil)
	require.Len(t, plan.Watches, 1)
	assert.Empty(t, plan.Watches[0].Key.GVR.Group)
	assert.Equal(t, ObjectSourceStatic, plan.Watches[0].Source)
}

func TestCollectionPlanPinnedAggregatedVersionUsesItsScope(t *testing.T) {
	for _, pinnedNamespaced := range []bool{false, true} {
		t.Run(map[bool]string{false: "cluster-scoped", true: "namespaced"}[pinnedNamespaced], func(t *testing.T) {
			discovery := collectionDiscoverySnapshot{
				PreferredResources: []*metav1.APIResourceList{
					collectionTestAPI("aggregated.example.com", "v2", !pinnedNamespaced, "widgets"),
				},
				ServedResources: []*metav1.APIResourceList{
					collectionTestAPI("aggregated.example.com", "v1", pinnedNamespaced, "widgets"),
					collectionTestAPI("aggregated.example.com", "v2", !pinnedNamespaced, "widgets"),
				},
			}
			cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{
				Name: "widgets", Group: "aggregated.example.com", Version: "v1", Namespaces: []string{"team-a"},
			}}, nil)
			plan := buildCollectionPlan(cfg, discovery, nil)
			if !pinnedNamespaced {
				assert.Empty(t, plan.Watches)
				assert.Equal(t, "IncompatibleScope", plan.Baseline[0].Reason)
				return
			}
			require.Len(t, plan.Watches, 1)
			assert.Equal(t, "v1", plan.Watches[0].Key.GVR.Version)
			assert.Equal(t, "team-a", plan.Watches[0].Key.Namespace)
			assert.True(t, plan.Watches[0].Namespaced)
			assert.Equal(t, ObjectSourceStatic, plan.Watches[0].Source)
			assert.Len(t, plan.Watches[0].Contributors, 1)
		})
	}
}

func TestCollectionPlanIsDeterministicAndDoesNotMutateSnapshots(t *testing.T) {
	cfg := collectionTestConfig(t, []string{collectionTestGroup}, nil, nil, nil)
	discovery := collectionTestDiscovery()
	a := collectionTestDeclaration("a", collectionTestSelection("*", []string{"team-b", "team-a"}, nil))
	b := collectionTestDeclaration("b", collectionTestSelection("widgets", nil, map[string]string{"env": "prod", "app": "web"}))
	b.Spec.Resources[0].LabelSelector.MatchExpressions = []metav1.LabelSelectorRequirement{{
		Key: "monitoring", Operator: metav1.LabelSelectorOpIn, Values: []string{"z", "a"},
	}}
	declarations := []*resourcecollection.ResourceCollection{b, nil, a}
	before, err := json.Marshal([]interface{}{cfg, discovery, declarations})
	require.NoError(t, err)
	plan := buildCollectionPlan(cfg, discovery, declarations)
	for i := 0; i < 20; i++ {
		assert.Equal(t, plan, buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{a, b, nil}))
	}
	reversed := collectionDiscoverySnapshot{}
	for i := len(discovery.PreferredResources) - 1; i >= 0; i-- {
		reversed.PreferredResources = append(reversed.PreferredResources, discovery.PreferredResources[i])
	}
	for i := len(discovery.ServedResources) - 1; i >= 0; i-- {
		reversed.ServedResources = append(reversed.ServedResources, discovery.ServedResources[i])
	}
	for i := len(discovery.CRDs) - 1; i >= 0; i-- {
		reversed.CRDs = append(reversed.CRDs, discovery.CRDs[i])
	}
	assert.Equal(t, plan, buildCollectionPlan(cfg, reversed, []*resourcecollection.ResourceCollection{a, b, nil}))
	after, err := json.Marshal([]interface{}{cfg, discovery, declarations})
	require.NoError(t, err)
	assert.Equal(t, string(before), string(after))
	assert.Same(t, b, declarations[0])
	assert.Equal(t, []string{"z", "a"}, b.Spec.Resources[0].LabelSelector.MatchExpressions[0].Values)
	for _, selection := range plan.Declarations[0].Selections {
		assert.NotEqual(t, resourcecollection.Active, selection.State)
	}
}

func TestCollectionPlanUnavailableDiscoveryRetainsRequests(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	explicit := collectionTestDeclaration("explicit", collectionTestSelection("widgets", nil, nil))
	wildcard := collectionTestDeclaration("wildcard", collectionTestSelection("*", nil, nil))
	before := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{explicit, wildcard})
	discovery := collectionTestDiscovery()
	discovery.PreferredResources, discovery.ServedResources = nil, nil
	after := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{explicit, wildcard})
	assert.Empty(t, after.Watches)
	require.Len(t, after.Declarations, 2)
	for i, declaration := range after.Declarations {
		require.Len(t, declaration.Selections, 1)
		result := declaration.Selections[0]
		assert.Equal(t, resourcecollection.Pending, result.State)
		assert.Equal(t, "APINotAvailable", result.Reason)
		assert.Equal(t, before.Declarations[i].Selections[0].Contributor, result.Contributor)
		assert.Equal(t, before.Declarations[i].Selections[0].Group, result.Group)
		assert.Equal(t, before.Declarations[i].Selections[0].Resource, result.Resource)
		for _, resolved := range result.Resolved {
			assert.Equal(t, "APINotAvailable", resolved.Reason)
		}
	}
	missingEverything := buildCollectionPlan(cfg, collectionDiscoverySnapshot{}, []*resourcecollection.ResourceCollection{explicit, wildcard})
	require.Len(t, missingEverything.Declarations, 2)
	assert.Equal(t, after.Declarations[0].Selections[0].Contributor, missingEverything.Declarations[0].Selections[0].Contributor)
	assert.Equal(t, resourcecollection.Pending, missingEverything.Declarations[0].Selections[0].State)
}

func TestCollectionPlanInvalidSelectionDoesNotBreakOtherSelections(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	cr := collectionTestDeclaration("invalid",
		resourcecollection.Selection{Group: "", Resource: "*"},
		collectionTestSelection("widgets", nil, nil),
	)
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{cr})
	require.Len(t, plan.Watches, 1)
	assert.Equal(t, "InvalidSelection", plan.Declarations[0].Selections[0].Reason)
	assert.Equal(t, "AwaitingObservation", plan.Declarations[0].Selections[1].Reason)
	assert.Equal(t, int64(1), plan.Declarations[0].Generation)
	assert.Equal(t, types.UID("uid-invalid"), plan.Declarations[0].UID)
}

func TestCollectionPlanDeclarationFieldsShareOnlyExactScopes(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{
		Name: "widgets", Group: collectionTestGroup, Version: "v1", Namespaces: []string{"team-a"},
		FieldSelector: "metadata.namespace=team-a,metadata.name=web",
	}}, nil)
	cr := collectionTestDeclaration("fields",
		resourcecollection.Selection{Group: collectionTestGroup, Resource: "widgets", Namespaces: []string{"team-a"},
			FieldSelector: "metadata.name==web,metadata.namespace=team-a"},
		resourcecollection.Selection{Group: collectionTestGroup, Resource: "widgets", Namespaces: []string{"team-a"},
			FieldSelector: "metadata.name!=web"},
	)
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{cr})
	require.Len(t, plan.Watches, 2)
	assert.Equal(t, "metadata.name!=web", plan.Watches[0].Key.FieldSelector)
	assert.Equal(t, "metadata.name=web,metadata.namespace=team-a", plan.Watches[1].Key.FieldSelector)
	assert.Len(t, plan.Watches[1].Contributors, 2)
	cr.Spec.Resources[0].FieldSelector = "status.phase in (Running)"
	invalid := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{cr})
	assert.Equal(t, "InvalidSelection", invalid.Declarations[0].Selections[0].Reason)
	assert.Len(t, invalid.Watches, 2) // Valid baseline and second selection remain independent.
}

func TestCollectionPlanPartialServedDiscoveryIncludesPreferredVersions(t *testing.T) {
	discovery := collectionDiscoverySnapshot{
		PreferredResources: []*metav1.APIResourceList{collectionTestAPI(collectionTestGroup, "v1", true, "widgets")},
		ServedResources:    []*metav1.APIResourceList{collectionTestAPI("unrelated.example.com", "v2", false, "others")},
		CRDs:               []*apiextensionsv1.CustomResourceDefinition{collectionTestCRD("widgets", apiextensionsv1.NamespaceScoped, "v1")},
	}
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "widgets", Group: collectionTestGroup, Version: "v1", Namespaces: []string{"team-a"}}}, nil)
	cr := collectionTestDeclaration("partial", collectionTestSelection("widgets", []string{"team-a"}, nil))
	plan := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{cr})
	require.Len(t, plan.Watches, 1)
	assert.Equal(t, "v1", plan.Watches[0].Key.GVR.Version)
	assert.Len(t, plan.Watches[0].Contributors, 2)
	discovery.CRDs[0] = collectionTestCRD("widgets", apiextensionsv1.NamespaceScoped, "v9")
	missing := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{cr})
	assert.Equal(t, "APINotAvailable", missing.Declarations[0].Selections[0].Reason)
	require.Len(t, missing.Watches, 1)
	assert.Len(t, missing.Watches[0].Contributors, 1)
}

func TestCollectionPlanServedScopeWinsOverSamePreferredVersion(t *testing.T) {
	discovery := collectionDiscoverySnapshot{
		PreferredResources: []*metav1.APIResourceList{collectionTestAPI("aggregated.example.com", "v1", false, "widgets")},
		ServedResources:    []*metav1.APIResourceList{collectionTestAPI("aggregated.example.com", "v1", true, "widgets")},
	}
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{
		Name: "widgets", Group: "aggregated.example.com", Version: "v1", Namespaces: []string{"team-a"},
	}}, nil)
	plan := buildCollectionPlan(cfg, discovery, nil)
	require.Len(t, plan.Watches, 1)
	assert.True(t, plan.Watches[0].Namespaced)
	assert.Equal(t, "team-a", plan.Watches[0].Key.Namespace)
	assert.Len(t, plan.Watches[0].Contributors, 1)
}

func TestCollectionPlanWildcardUsesEachTypeStorageVersion(t *testing.T) {
	discovery := collectionDiscoverySnapshot{
		PreferredResources: []*metav1.APIResourceList{collectionTestAPI(collectionTestGroup, "v2", true, "widgets", "gadgets")},
		ServedResources: []*metav1.APIResourceList{
			collectionTestAPI(collectionTestGroup, "v1", true, "widgets"),
			collectionTestAPI(collectionTestGroup, "v2", true, "widgets", "gadgets"),
		},
		CRDs: []*apiextensionsv1.CustomResourceDefinition{
			collectionTestCRD("widgets", apiextensionsv1.NamespaceScoped, "v1", "v2"),
			collectionTestCRD("gadgets", apiextensionsv1.NamespaceScoped, "v2"),
		},
	}
	cr := collectionTestDeclaration("wildcard", collectionTestSelection("*", nil, nil))
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	plan := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{cr})
	require.Len(t, plan.Watches, 2)
	assert.Equal(t, "v1", plan.Watches[0].Key.GVR.Version)
	assert.Equal(t, "widgets", plan.Watches[0].Key.GVR.Resource)
	assert.Equal(t, "v2", plan.Watches[1].Key.GVR.Version)
	assert.Equal(t, "gadgets", plan.Watches[1].Key.GVR.Resource)
	discovery.CRDs[1].Spec.Versions[0].Served = false
	partial := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{cr})
	require.Len(t, partial.Watches, 1)
	assert.Equal(t, "widgets", partial.Watches[0].Key.GVR.Resource)
	assert.Equal(t, "APINotAvailable", partial.Declarations[0].Selections[0].Reason)
	discovery.CRDs[1].Spec.Versions[0].Served = true
	assert.Equal(t, plan, buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{cr}))
}

func TestCollectionPlanStorageAndYAMLContributionsRemoveIndependently(t *testing.T) {
	discovery := collectionDiscoverySnapshot{
		PreferredResources: []*metav1.APIResourceList{collectionTestAPI(collectionTestGroup, "v2", true, "widgets")},
		ServedResources: []*metav1.APIResourceList{
			collectionTestAPI(collectionTestGroup, "v1", true, "widgets"),
			collectionTestAPI(collectionTestGroup, "v2", true, "widgets"),
		},
		CRDs: []*apiextensionsv1.CustomResourceDefinition{collectionTestCRD("widgets", apiextensionsv1.NamespaceScoped, "v1", "v2")},
	}
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{{Name: "widgets", Group: collectionTestGroup, Version: "v2"}}, nil)
	a := collectionTestDeclaration("a", collectionTestSelection("widgets", nil, nil))
	b := collectionTestDeclaration("b", collectionTestSelection("widgets", nil, nil))
	plan := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{b, a})
	require.Len(t, plan.Watches, 2)
	assert.Equal(t, "v1", plan.Watches[0].Key.GVR.Version)
	assert.Len(t, plan.Watches[0].Contributors, 2)
	assert.Equal(t, "v2", plan.Watches[1].Key.GVR.Version)
	assert.Len(t, plan.Watches[1].Contributors, 1)
	assert.Equal(t, plan, buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{a, b}))
	afterRemoval := buildCollectionPlan(cfg, discovery, []*resourcecollection.ResourceCollection{b})
	require.Len(t, afterRemoval.Watches, 2)
	assert.Len(t, afterRemoval.Watches[0].Contributors, 1)
	assert.Equal(t, plan.Watches[1], afterRemoval.Watches[1])
	baselineOnly := buildCollectionPlan(cfg, discovery, nil)
	require.Len(t, baselineOnly.Watches, 1)
	assert.Equal(t, plan.Watches[1], baselineOnly.Watches[0])
}

func TestCollectionPlanLabelExpressions(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	cr := collectionTestDeclaration("labels", collectionTestSelection("widgets", []string{"team-a"}, map[string]string{"team": "payments"}))
	cr.Spec.Resources[0].LabelSelector.MatchExpressions = []metav1.LabelSelectorRequirement{
		{Key: "environment", Operator: metav1.LabelSelectorOpIn, Values: []string{"prod", "staging"}},
		{Key: "tier", Operator: metav1.LabelSelectorOpNotIn, Values: []string{"batch"}},
		{Key: "monitoring", Operator: metav1.LabelSelectorOpExists},
		{Key: "disabled", Operator: metav1.LabelSelectorOpDoesNotExist},
	}
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{cr})
	require.Len(t, plan.Watches, 1)
	selector, err := labels.Parse(plan.Watches[0].Key.LabelSelector)
	require.NoError(t, err)
	matching := labels.Set{"team": "payments", "environment": "prod", "tier": "api", "monitoring": "enabled"}
	assert.True(t, selector.Matches(matching))
	for _, changes := range []labels.Set{
		{"team": "other"}, {"environment": "development"}, {"tier": "batch"}, {"disabled": "true"},
	} {
		altered := labels.Set{}
		for k, v := range matching {
			altered[k] = v
		}
		for k, v := range changes {
			altered[k] = v
		}
		assert.False(t, selector.Matches(altered), "must exclude %v", altered)
	}
	delete(matching, "monitoring")
	assert.False(t, selector.Matches(matching))
}

func TestCollectionPlanAbsentAndEmptyLabelSelectorsHaveNoFilter(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	a := collectionTestDeclaration("a", collectionTestSelection("widgets", nil, nil))
	b := collectionTestDeclaration("b", collectionTestSelection("widgets", nil, nil))
	b.Spec.Resources[0].LabelSelector = &metav1.LabelSelector{}
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{a, b})
	require.Len(t, plan.Watches, 1)
	assert.Empty(t, plan.Watches[0].Key.LabelSelector)
	assert.Empty(t, plan.Watches[0].Key.Namespace)
	assert.Len(t, plan.Watches[0].Contributors, 2)
}
