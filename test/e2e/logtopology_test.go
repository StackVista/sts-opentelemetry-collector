//nolint:testpackage
package e2e

import (
	"testing"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/stretchr/testify/require"

	"e2e/harness"
)

// TODO: exercise the pick/format helper functions
func TestLogToOtelTopology_CreateComponentAndRelationMappings(t *testing.T) {
	env := harness.SetupTopologyTest(t, 1, false)
	defer env.Cleanup()

	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(otelLogComponentMappingSpecPolicyServer()),
		otelRelationMappingSnapshot(otelLogRelationMappingSpecPolicyEnforcedByServer()),
	)
	sendLogs(t, env)

	recs := env.ConsumeTopologyRecords(t, 4)
	components, relations, errs := harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)

	assertLogComponents(t, components)
	assertLogRelations(t, relations)
}

func TestLogToOtelTopology_UpdateComponentAndRelationMappings(t *testing.T) {
	env := harness.SetupTopologyTest(t, 1, false)
	defer env.Cleanup()

	// Publish initial settings
	policyServerComponent := otelLogComponentMappingSpecPolicyServer()
	policyRelation := otelLogRelationMappingSpecPolicyEnforcedByServer()
	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(policyServerComponent),
		otelRelationMappingSnapshot(policyRelation),
	)
	sendLogs(t, env)

	recs := env.ConsumeTopologyRecords(t, 4)
	components, relations, errs := harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)
	assertLogComponents(t, components)
	assertLogRelations(t, relations)

	// Update the settings
	// Component: update name
	policyServerComponent.Output.Name = harness.StrExpr("policy-server-updated")

	// Relation: update type name
	policyRelation.Output.TypeName = harness.StrExpr("enforced-by-updated")

	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(policyServerComponent),
		otelRelationMappingSnapshot(policyRelation),
	)
	sendLogs(t, env)

	recs = env.ConsumeTopologyRecords(t, 4)
	components, relations, errs = harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)

	// Assert that the component and relations mappings have updated values
	require.Len(t, components, 1)
	foundUpdatedPolicyServer := false
	for _, c := range components {
		if c.ExternalId == "urn:kubewarden:cluster/production:policyserver/default" && c.Name == "policy-server-updated" {
			foundUpdatedPolicyServer = true
		}
	}
	require.True(t, foundUpdatedPolicyServer, "expected updated policy server component mapping not found")

	require.Len(t, relations, 2)
	for _, r := range relations {
		require.Equal(t, "enforced-by-updated", r.TypeName, "expected updated relation mapping not found")
	}
}

func TestLogToOtelTopology_ErrorReturnedOnIncorrectMappingConfig(t *testing.T) {
	env := harness.SetupTopologyTest(t, 1, false)
	defer env.Cleanup()

	component := otelLogComponentMappingSpecPolicyServer()
	// modify base component mapping to have an invalid expression
	component.Output.Name = harness.StrExpr(`${log.attributes}`) // a map reference where a string expression is required
	env.PublishSettingSnapshots(t, otelComponentMappingSnapshot(component))

	sendLogs(t, env)
	recs := env.ConsumeTopologyRecords(t, 1) // Expecting 1 record for the error

	components, relations, errs := harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, components, 0)
	require.Len(t, relations, 0)
	// errs should be present because the mapping eval fails
	require.Greater(t, len(errs), 0, "expected errors not returned")
	require.Contains(
		t,
		errs[0].Message, // all the errors should be the same
		"expected string type, got: map(string, dyn)",
		"expected error on incorrect mapping config",
	)
}

func TestLogToOtelTopology_RemovesMappingsWhenOmittedFromNextSnapshot(t *testing.T) {
	env := harness.SetupTopologyTest(t, 1, false)
	defer env.Cleanup()

	// Publish initial settings with both component and relation mappings
	policyServerComponent := otelLogComponentMappingSpecPolicyServer()
	policyRelation := otelLogRelationMappingSpecPolicyEnforcedByServer()
	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(policyServerComponent),
		otelRelationMappingSnapshot(policyRelation),
	)
	sendLogs(t, env)

	// Consume records and verify both components and relations are generated
	recs := env.ConsumeTopologyRecords(t, 8)
	components, relations, errs := harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)
	require.Len(t, components, 1, "expected 1 component (policy server)")
	require.Len(t, relations, 2, "expected 2 relations from initial mappings")

	// Now publish new snapshots with ONLY the component mapping (remove the relation mapping)
	// This verifies that the system respects the removal of mappings from settings
	// We must explicitly publish an empty relation mapping snapshot to remove the old one
	// Note: The settings extension does not currently notify the snapshot manager when
	// an empty snapshot is published (optimization: no change in effective state).
	// See: https://github.com/StackVista/sts-opentelemetry-collector/issues/[TODO]
	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(policyServerComponent),
		otelRelationMappingSnapshot(), // Empty snapshot to remove old relation mappings
	)

	// Send the same logs again
	// Note: Due to the settings extension not notifying on empty snapshots, the relation
	// mappings remain active and will still generate relations from matching logs
	sendLogs(t, env)
	recs = env.ConsumeTopologyRecords(t, 3)
	components, relations, errs = harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)
	require.Len(t, components, 1, "policy-server component still exists")
	// TODO: Change this expectation to 0 once the settings extension properly notifies
	// on empty snapshots, allowing mapping removals to work as intended
	require.Len(t, relations, 2, "relations still generated due to mapping not being removed")
}

// sendLogs builds log data and calls harness.BuildAndSendLogs.
func sendLogs(t *testing.T, env *harness.TopologyTestEnv) {
	endpoint := env.Collector.Instances[0].HostAddr
	logData := *logSpecWithPolicyServerAndPolicy()
	err := harness.BuildAndSendLogs(env.Ctx, env.Logger, endpoint, logData)
	require.NoError(t, err)
}

// logSpecWithPolicyServerAndPolicy creates a log spec representing Kubewarden policy server and policy logs.
// Bodies are structured maps (Kubernetes objects) as provided by a proper receiver; the mapping engine
// does not parse string bodies.
func logSpecWithPolicyServerAndPolicy() *harness.LogSpec {
	return &harness.LogSpec{
		ResourceAttributes: map[string]string{
			"k8s.cluster.name":   "production",
			"k8s.namespace.name": "kubewarden",
			"service.name":       "kubewarden",
		},
		ScopeAttributes: map[string]string{
			"otel.scope.name":    "kubewarden.policy.server",
			"otel.scope.version": "1.0.0",
		},
		LogRecords: []harness.LogRecordSpec{
			{
				EventName: "policy-server-created",
				Body: map[string]interface{}{
					"apiVersion": "policies.kubewarden.io/v1",
					"kind":       "PolicyServer",
					"metadata": map[string]interface{}{
						"name": "default",
					},
					"spec": map[string]interface{}{
						"version": "1.0.0",
					},
				},
				Attributes: map[string]interface{}{
					"k8s.resource.kind":      "PolicyServer",
					"k8s.resource.api_group": "policies.kubewarden.io",
					"k8s.resource.name":      "default",
				},
			},
			{
				EventName: "policy-deployed",
				Body: map[string]interface{}{
					"apiVersion": "policies.kubewarden.io/v1",
					"kind":       "AdmissionPolicy",
					"metadata": map[string]interface{}{
						"name":      "user-policy",
						"namespace": "kubewarden",
					},
					"spec": map[string]interface{}{
						"policyServer": "default",
					},
				},
				Attributes: map[string]interface{}{
					"k8s.resource.kind":        "AdmissionPolicy",
					"k8s.resource.api_group":   "policies.kubewarden.io",
					"k8s.resource.name":        "user-policy",
					"kubewarden.policy_server": "default",
				},
			},
			{
				EventName: "cluster-policy-deployed",
				Body: map[string]interface{}{
					"apiVersion": "policies.kubewarden.io/v1",
					"kind":       "ClusterAdmissionPolicy",
					"metadata": map[string]interface{}{
						"name": "cluster-wide-policy",
					},
					"spec": map[string]interface{}{
						"policyServer": "default",
					},
				},
				Attributes: map[string]interface{}{
					"k8s.resource.kind":        "ClusterAdmissionPolicy",
					"k8s.resource.api_group":   "policies.kubewarden.io",
					"k8s.resource.name":        "cluster-wide-policy",
					"kubewarden.policy_server": "default",
				},
			},
		},
	}
}

func assertLogComponents(t *testing.T, components map[string]*topostreamv1.TopologyStreamComponent) {
	require.Len(t, components, 1, "expected 1 policy server component")
	for _, c := range components {
		require.Equal(t, "default", c.Name)
		require.Equal(t, "urn:kubewarden:cluster/production:policyserver/default", c.ExternalId)
		require.Equal(t, "policy server", c.TypeName)
		require.Equal(t, "Control Plane", c.LayerName)
		require.Equal(t, "Kubernetes", c.DomainName)
	}
}

func assertLogRelations(t *testing.T, relations map[string]*topostreamv1.TopologyStreamRelation) {
	require.Len(t, relations, 2, "expected 2 policy-enforced-by-server relations")

	expectedRelations := []struct {
		Source, Target string
	}{
		{
			"urn:kubewarden:cluster/production:namespace/kubewarden:admissionpolicy/user-policy",
			"urn:kubewarden:cluster/production:policyserver/default",
		},
		{
			"urn:kubewarden:cluster/production:clusteradmissionpolicy/cluster-wide-policy",
			"urn:kubewarden:cluster/production:policyserver/default",
		},
	}

	for _, r := range relations {
		found := false
		for _, expected := range expectedRelations {
			if r.SourceIdentifier == expected.Source && r.TargetIdentifier == expected.Target {
				found = true
				require.Equal(t, "enforced by", r.TypeName)
				break
			}
		}
		require.True(t, found, "unexpected relation: %s -> %s", r.SourceIdentifier, r.TargetIdentifier)
	}
}

// otelLogComponentMappingSpecPolicyServer creates a component mapping for Kubewarden PolicyServer
func otelLogComponentMappingSpecPolicyServer() *harness.OtelComponentMappingSpec {
	return &harness.OtelComponentMappingSpec{
		MappingID:         "log-comp-policy-server",
		MappingIdentifier: "urn:stackpack:kubewarden:shared:otel-component-mapping:policy-server",
		Name:              "Kubewarden Policy Server",
		ExpireAfterMs:     900000,
		Input: settingsproto.OtelInput{
			Signal: settingsproto.OtelInputSignalList{
				settingsproto.LOGS,
			},
			Resource: settingsproto.OtelInputResource{
				Scope: &settingsproto.OtelInputScope{
					Log: &settingsproto.OtelInputLog{
						Action: harness.Ptr(settingsproto.CREATE),
						Condition: harness.PtrBoolExpr(
							`log.attributes["k8s.resource.kind"] == "PolicyServer" && log.attributes["k8s.resource.api_group"] == "policies.kubewarden.io"`,
						),
					},
				},
			},
		},
		Vars: []settingsproto.OtelVariableMapping{
			{
				Name:  "serverName",
				Value: harness.AnyExpr(`${log.attributes["k8s.resource.name"]}`),
			},
			{
				Name:  "clusterName",
				Value: harness.AnyExpr(`${resource.attributes["k8s.cluster.name"]}`),
			},
		},
		Output: settingsproto.OtelComponentMappingOutput{
			Identifier: harness.StrExpr(`urn:kubewarden:cluster/${vars.clusterName}:policyserver/${vars.serverName}`),
			Name:       harness.StrExpr(`${vars.serverName}`),
			TypeName:   harness.StrExpr("policy server"),
			LayerName:  harness.StrExpr("Control Plane"),
			DomainName: harness.StrExpr("Kubernetes"),
		},
	}
}

// otelLogRelationMappingSpecPolicyEnforcedByServer creates a relation mapping for policy enforced by server
func otelLogRelationMappingSpecPolicyEnforcedByServer() *harness.OtelRelationMappingSpec {
	return &harness.OtelRelationMappingSpec{
		MappingID:         "log-rel-policy-enforced-by-server",
		MappingIdentifier: "urn:stackpack:kubewarden:shared:otel-relation-mapping:policy-enforced-by-server",
		ExpireAfterMs:     900000,
		Input: settingsproto.OtelInput{
			Signal: settingsproto.OtelInputSignalList{
				settingsproto.LOGS,
			},
			Resource: settingsproto.OtelInputResource{
				Scope: &settingsproto.OtelInputScope{
					Log: &settingsproto.OtelInputLog{
						Action: harness.Ptr(settingsproto.CREATE),
						Condition: harness.PtrBoolExpr(
							`log.attributes["k8s.resource.api_group"] == "policies.kubewarden.io" && ` +
								`(log.attributes["k8s.resource.kind"] == "AdmissionPolicy" || ` +
								`log.attributes["k8s.resource.kind"] == "ClusterAdmissionPolicy") && ` +
								`log.attributes["kubewarden.policy_server"] != null`,
						),
					},
				},
			},
		},
		Vars: []settingsproto.OtelVariableMapping{
			{
				Name:  "policyName",
				Value: harness.AnyExpr(`${log.attributes["k8s.resource.name"]}`),
			},
			{
				Name:  "policyKind",
				Value: harness.AnyExpr(`${log.attributes["k8s.resource.kind"]}`),
			},
			{
				Name:  "serverName",
				Value: harness.AnyExpr(`${log.attributes["kubewarden.policy_server"]}`),
			},
			{
				Name:  "clusterName",
				Value: harness.AnyExpr(`${resource.attributes["k8s.cluster.name"]}`),
			},
			{
				Name:  "namespace",
				Value: harness.AnyExpr(`${resource.attributes["k8s.namespace.name"]}`),
			},
		},
		Output: settingsproto.OtelRelationMappingOutput{
			SourceId: harness.StrExpr(
				`${vars.policyKind == "ClusterAdmissionPolicy" ? "urn:kubewarden:cluster/" + vars.clusterName + ":clusteradmissionpolicy/" + vars.policyName : "urn:kubewarden:cluster/" + vars.clusterName + ":namespace/" + vars.namespace + ":admissionpolicy/" + vars.policyName}`,
			),
			TargetId: harness.StrExpr(`urn:kubewarden:cluster/${vars.clusterName}:policyserver/${vars.serverName}`),
			TypeName: harness.StrExpr("enforced by"),
		},
	}
}
