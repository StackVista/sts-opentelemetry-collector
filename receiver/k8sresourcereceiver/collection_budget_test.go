//nolint:testpackage
package k8sresourcereceiver

import (
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/resourcecollection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectionWatchBudgetSharesDeclarationControlWatch(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	cfg.MaxDeclarationWatches = 1
	catalog := collectionTestDiscovery()
	controlAPI := collectionTestAPI(resourcecollection.Group, resourcecollection.Version, false, "resourcecollections")
	catalog.PreferredResources = append(catalog.PreferredResources, controlAPI)
	catalog.ServedResources = append(catalog.ServedResources, controlAPI)
	crd := collectionTestCRD("resourcecollections", "Cluster", resourcecollection.Version)
	crd.Spec.Group = resourcecollection.Group
	catalog.CRDs = append(catalog.CRDs, crd)
	widgets := collectionTestDeclaration("a", collectionTestSelection("widgets", nil, nil))
	control := collectionTestDeclaration("b", resourcecollection.Selection{
		Group: resourcecollection.Group, Resource: "resourcecollections",
	})
	filtered := collectionTestDeclaration("c", control.Spec.Resources[0])
	filtered.Spec.Resources[0].FieldSelector = "metadata.name=example"
	plan := buildCollectionPlan(cfg, catalog, []*resourcecollection.ResourceCollection{widgets, control, filtered})
	require.Len(t, plan.Watches, 2)
	assert.Equal(t, collectionAwaitingObservation, plan.Declarations[1].Selections[0].Reason)
	assert.Equal(t, collectionWatchBudgetExceeded, plan.Declarations[2].Selections[0].Reason,
		"a filtered declaration watch requires its own informer")

	cfg.MaxDeclarationWatches = 2
	declarations := []*resourcecollection.ResourceCollection{widgets, control}
	plan = buildCollectionPlan(cfg, catalog, declarations)
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	require.NoError(t, handover.update(plan))
	oldKey := collectionTestWidgetsGVR()
	registry.observe(collectionWatchKey{GVR: oldKey}, collectionHandoverObject("old", "old"))
	registry.observe(collectionWatchKey{GVR: resourcecollection.GVR()})
	collectionHandoverRead(t, handover)
	catalog.CRDs[0] = collectionTestCRD("widgets", catalog.CRDs[0].Spec.Scope, "v2", "v1")
	catalog.ServedResources = append(catalog.ServedResources, collectionTestAPI(collectionTestGroup, "v2", true, "widgets"))
	require.NoError(t, handover.update(buildCollectionPlan(cfg, catalog, declarations)))
	assert.Len(t, registry.snapshot(), 3, "a shared control watch must not consume handover capacity")
}

func TestCollectionWatchBudgetSharesWatchesAndProtectsBaseline(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, []ObjectWatch{
		{Name: "pods"},
		{Name: "widgets", Group: collectionTestGroup},
	}, nil)
	cfg.MaxDeclarationWatches = 1
	shared := collectionTestSelection("widgets", []string{"team-a"}, nil)
	a := collectionTestDeclaration("a", shared)
	b := collectionTestDeclaration("b", shared)
	c := collectionTestDeclaration("c", collectionTestSelection("widgets", []string{"team-b"}, nil))
	d := collectionTestDeclaration("d", collectionTestSelection("widgets", nil, nil))
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{d, c, b, a})

	require.Len(t, plan.Watches, 3, "two baseline watches plus one shared declaration watch")
	assert.Len(t, plan.Watches[1].Contributors, 2)
	blocked := plan.Declarations[2].Selections[0]
	assert.Equal(t, resourcecollection.Blocked, blocked.State)
	assert.Equal(t, "WatchBudgetExceeded", blocked.Reason)
	assert.Contains(t, blocked.Message, "1")
	assert.Empty(t, blocked.Resolved[0].Watches)
	assert.Equal(t, collectionAwaitingObservation, plan.Declarations[3].Selections[0].Reason,
		"a declaration can still share a baseline watch when the budget is full")

	recovered := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{c, d})
	require.Len(t, recovered.Watches, 3)
	assert.Equal(t, collectionAwaitingObservation, recovered.Declarations[0].Selections[0].Reason)
}

func TestCollectionWatchBudgetBoundsStorageVersionHandover(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	cfg.MaxDeclarationWatches = 1
	declaration := collectionTestDeclaration("widgets", collectionTestSelection("widgets", nil, nil))
	catalog := collectionTestDiscovery()
	original := buildCollectionPlan(cfg, catalog, []*resourcecollection.ResourceCollection{declaration})
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	require.NoError(t, handover.update(original))
	object := collectionHandoverObject("old", "old")
	registry.observe(original.Watches[0].Key, object)
	collectionHandoverRead(t, handover)

	catalog.CRDs[0] = collectionTestCRD("widgets", catalog.CRDs[0].Spec.Scope, "v2", "v1")
	catalog.ServedResources = append(catalog.ServedResources, collectionTestAPI(collectionTestGroup, "v2", true, "widgets"))
	next := buildCollectionPlan(cfg, catalog, []*resourcecollection.ResourceCollection{declaration})
	require.NoError(t, handover.update(next))
	require.Len(t, registry.snapshot(), 1, "retained watches must also respect the budget")
	assert.Contains(t, registry.observations, next.Watches[0].Key)
	view := collectionHandoverRead(t, handover)
	assert.Equal(t, []resourceObject{object}, view.Objects[collectionTestWidgetsGVR()].Objects,
		"retiring the old watch must preserve its last observation until replacement syncs")

	registry.observe(next.Watches[0].Key, collectionHandoverObject("new", "new"))
	view = collectionHandoverRead(t, handover)
	assert.True(t, view.Complete)
	assert.NotContains(t, view.Objects, collectionTestWidgetsGVR())
	require.Len(t, registry.snapshot(), 1)
}

func TestCollectionWatchBudgetAdmitsAllNamespacesForEachType(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	cfg.MaxDeclarationWatches = 2
	a := collectionTestDeclaration("a", collectionTestSelection("widgets", []string{"team-a"}, nil))
	b := collectionTestDeclaration("b",
		collectionTestSelection("widgets", []string{"team-c", "team-b"}, nil),
		collectionTestSelection("gadgets", nil, nil),
	)
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{b, a})
	require.Len(t, plan.Watches, 2)
	assert.Equal(t, resourcecollection.Blocked, plan.Declarations[1].Selections[0].State)
	assert.Empty(t, plan.Declarations[1].Selections[0].Resolved[0].Watches)
	assert.Equal(t, collectionAwaitingObservation, plan.Declarations[1].Selections[1].Reason,
		"an oversized selection must not consume capacity")

	b.Spec.Resources = []resourcecollection.Selection{collectionTestSelection("*", nil, nil)}
	plan = buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{b})
	require.Len(t, plan.Watches, 2, "wildcard expansion also consumes the budget")
	status := collectionDeclarationStatus(plan.Declarations[0], nil)
	require.Len(t, status.Resources, 1)
	assert.Equal(t, resourcecollection.Blocked, status.Resources[0].State)
	assert.Equal(t, "WatchBudgetExceeded", status.Resources[0].Reason)
}

func TestCollectionWatchBudgetWithdrawsDisplacedContribution(t *testing.T) {
	cfg := collectionTestConfig(t, nil, nil, nil, nil)
	cfg.MaxDeclarationWatches = 1
	original := collectionTestDeclaration("z", collectionTestSelection("widgets", nil, nil))
	plan := buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{original})
	registry := newCollectionHandoverTestRegistry()
	handover := newCollectionHandover(registry)
	require.NoError(t, handover.update(plan))
	object := collectionHandoverObject("old", "old")
	registry.observe(plan.Watches[0].Key, object)
	collectionHandoverRead(t, handover)

	first := collectionTestDeclaration("a", collectionTestSelection("gadgets", nil, nil))
	plan = buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{original, first})
	require.NoError(t, handover.update(plan))
	require.Len(t, registry.snapshot(), 1)
	view := collectionHandoverRead(t, handover)
	assert.Empty(t, view.Objects, "blocked contributions must not retain their old watches or objects")
	assert.Equal(t, collectionWatchBudgetExceeded, plan.Declarations[1].Selections[0].Reason)
	allowDelete := collectionDeletionPolicy(cfg, plan, collectionTestDiscovery(), registry.snapshot(), view.Withdrawn, true, true, true)
	assert.True(t, allowDelete(ResourceChange{GVR: collectionTestWidgetsGVR(), Obj: object, Source: ObjectSourceCR}),
		"budget withdrawal must remove prior peer observations too")

	plan = buildCollectionPlan(cfg, collectionTestDiscovery(), []*resourcecollection.ResourceCollection{original})
	require.NoError(t, handover.update(plan))
	registry.observe(plan.Watches[0].Key, object)
	assert.True(t, collectionHandoverRead(t, handover).Complete)
}

func TestCollectionWatchBudgetConfiguration(t *testing.T) {
	cfg := &Config{}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 256, cfg.MaxDeclarationWatches)
	defaultConfig, ok := createDefaultConfig().(*Config)
	require.True(t, ok)
	assert.Equal(t, 256, defaultConfig.MaxDeclarationWatches)
	cfg.MaxDeclarationWatches = -1
	require.ErrorContains(t, cfg.Validate(), "max_declaration_watches")
}
