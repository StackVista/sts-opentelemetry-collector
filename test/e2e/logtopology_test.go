//nolint:testpackage
package e2e

import (
	"testing"
	"time"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"e2e/harness"
)

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

func TestLogToOtelTopology_CreateComponentAndRelationMappings_DeduplicationEnabled(t *testing.T) {
	env := harness.SetupTopologyTest(t, 1, true)
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

	// send logs again (same input) - should not produce any new topology records
	sendLogs(t, env)

	recs, err := env.Kafka.TopologyConsumer.ConsumeTopology(env.Ctx, 4, time.Second*5)
	require.NoError(t, err)
	components, relations, errs = harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)
	require.Len(t, components, 0)
	require.Len(t, relations, 0)
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
	policyServerComponent.Output.Name = harness.StrExpr("'policy-server-updated'")

	// Relation: update type name
	policyRelation.Output.TypeName = harness.StrExpr("'enforced-by-updated'")

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
	component.Output.Name = harness.StrExpr(`log.attributes`) // a map reference where a string expression is required
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

	// Now publish new snapshots with only the component mapping (remove the relation mapping)
	// This would remove the relation mapping from being applied to new logs
	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(policyServerComponent),
		otelRelationMappingSnapshot(), // Empty snapshot to remove old relation mappings
	)

	// First, check that TopologyStreamRemove messages are sent for the removed mappings
	recs = env.ConsumeTopologyRecords(t, 4) // 4 shards
	foundRemovals := 0
	for _, rec := range recs {
		var msg topostreamv1.TopologyStreamMessage
		require.NoError(t, proto.Unmarshal(rec.Value, &msg))

		if rm := msg.GetTopologyStreamRemove(); rm != nil {
			foundRemovals++
			require.Contains(t, rm.RemovalCause, "Setting with identifier")
		}
	}
	require.Equal(t, 4, foundRemovals, "expected removal messages")

	// Send the same logs again; since the relation mapping is gone, no new relations should be created
	sendLogs(t, env)
	recs = env.ConsumeTopologyRecords(t, 3)
	components, relations, errs = harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)
	require.Len(t, components, 1, "policy-server component still exists")
	require.Len(t, relations, 0, "no new relations without the relation mapping")
}

// sendLogs builds log data and calls harness.BuildAndSendLogs.
func sendLogs(t *testing.T, env *harness.TopologyTestEnv) {
	endpoint := env.Collector.Instances[0].HostAddr
	logData := *logSpecWithPolicyServerAndPolicy()
	err := harness.BuildAndSendLogs(env.Ctx, env.Logger, endpoint, logData)
	require.NoError(t, err)
}

// logSpecWithPolicyServerAndPolicy creates a log spec representing Kubewarden policy server and policy logs.
func logSpecWithPolicyServerAndPolicy() *harness.LogSpec {
	policyServerSpec := map[string]interface{}{
		"image":              "ghcr.io/kubewarden/policy-server:v1.33.1",
		"replicas":           1,
		"serviceAccountName": "policy-server",
		"env": []interface{}{
			map[string]interface{}{
				"name":  "KUBEWARDEN_LOG_LEVEL",
				"value": "info",
			},
		},
	}

	policyServerStatus := map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"lastTransitionTime": "2026-03-27T08:54:33Z",
				"message":            "",
				"reason":             "ReconciliationSucceeded",
				"status":             "True",
				"type":               "DeploymentReconciled",
			},
		},
	}

	clusterPolicySpec := map[string]interface{}{
		"backgroundAudit": true,
		"mode":            "protect",
		"module":          "ghcr.io/kubewarden/policies/user-group-psp:v0.4.9",
		"mutating":        false,
		"policyServer":    "default",
		"timeoutSeconds":  10,
		"rules": []interface{}{
			map[string]interface{}{
				"apiGroups":   []interface{}{""},
				"apiVersions": []interface{}{"v1"},
				"operations":  []interface{}{"CREATE", "UPDATE"},
				"resources":   []interface{}{"pods"},
			},
		},
		"settings": map[string]interface{}{
			"run_as_user": map[string]interface{}{
				"rule": "MustRunAsNonRoot",
			},
		},
	}

	clusterPolicyStatus := map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"lastTransitionTime": "2026-03-27T09:03:52Z",
				"message":            "The policy webhook has been created",
				"reason":             "PolicyActive",
				"status":             "True",
				"type":               "PolicyActive",
			},
		},
		"mode":         "protect",
		"policyStatus": "active",
	}

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
					"spec":   policyServerSpec,
					"status": policyServerStatus,
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
					"spec":   clusterPolicySpec,
					"status": clusterPolicyStatus,
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

		// Verify configuration is populated with omit and does not contain status
		require.NotNil(t, c.ResourceDefinition, "configuration should be populated via omit(log.body, ['status'])")
		configFields := c.ResourceDefinition.GetFields()
		require.Contains(t, configFields, "spec", "omit should have preserved all fields except status")
		require.NotContains(t, configFields, "status", "omit should have removed status field")

		specMap := configFields["spec"].GetStructValue()
		require.NotNil(t, specMap)
		specFields := specMap.GetFields()
		require.Contains(t, specFields, "image")
		require.Contains(t, specFields, "replicas")

		// Verify status is populated with pick and contains only status
		require.NotNil(t, c.StatusData, "status should be populated via pick(log.body, ['status'])")
		statusFields := c.StatusData.GetFields()
		require.Contains(t, statusFields, "status", "pick should have created a status wrapper")

		statusStatusMap := statusFields["status"].GetStructValue()
		require.NotNil(t, statusStatusMap)
		statusStatusFields := statusStatusMap.GetFields()
		require.Contains(t, statusStatusFields, "conditions", "status should contain conditions")
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

func otelLogComponentMappingSpecPolicyServer() *harness.OtelComponentMappingSpec {
	return &harness.OtelComponentMappingSpec{
		MappingID:         "log-comp-policy-server",
		MappingIdentifier: "urn:stackpack:kubewarden:otel-component-mapping:policy-server",
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
				Value: harness.AnyExpr(`log.attributes["k8s.resource.name"]`),
			},
			{
				Name:  "clusterName",
				Value: harness.AnyExpr(`resource.attributes["k8s.cluster.name"]`),
			},
		},
		Output: settingsproto.OtelComponentMappingOutput{
			Identifier: harness.StrExpr(`"urn:kubewarden:cluster/" + vars.clusterName + ":policyserver/" + vars.serverName`),
			Name:       harness.StrExpr(`vars.serverName`),
			TypeName:   harness.StrExpr("'policy server'"),
			LayerName:  harness.StrExpr("'Control Plane'"),
			DomainName: harness.StrExpr("'Kubernetes'"),
			Required: &settingsproto.OtelComponentMappingFieldMapping{
				Configuration: harness.Ptr(harness.AnyExpr(`omit(log.body, ['status'])`)),
				Status:        harness.Ptr(harness.AnyExpr(`pick(log.body, ['status'])`)),
			},
		},
	}
}

func otelLogRelationMappingSpecPolicyEnforcedByServer() *harness.OtelRelationMappingSpec {
	return &harness.OtelRelationMappingSpec{
		MappingID:         "log-rel-policy-enforced-by-server",
		MappingIdentifier: "urn:stackpack:kubewarden:otel-relation-mapping:policy-enforced-by-server",
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
				Value: harness.AnyExpr(`log.attributes["k8s.resource.name"]`),
			},
			{
				Name:  "policyKind",
				Value: harness.AnyExpr(`log.attributes["k8s.resource.kind"]`),
			},
			{
				Name:  "serverName",
				Value: harness.AnyExpr(`log.attributes["kubewarden.policy_server"]`),
			},
			{
				Name:  "clusterName",
				Value: harness.AnyExpr(`resource.attributes["k8s.cluster.name"]`),
			},
			{
				Name:  "namespace",
				Value: harness.AnyExpr(`resource.attributes["k8s.namespace.name"]`),
			},
		},
		Output: settingsproto.OtelRelationMappingOutput{
			SourceId: harness.StrExpr(
				`vars.policyKind == "ClusterAdmissionPolicy" ? "urn:kubewarden:cluster/" + vars.clusterName + ":clusteradmissionpolicy/" + vars.policyName : "urn:kubewarden:cluster/" + vars.clusterName + ":namespace/" + vars.namespace + ":admissionpolicy/" + vars.policyName`,
			),
			TargetId: harness.StrExpr(`'urn:kubewarden:cluster/' + vars.clusterName + ':policyserver/' + vars.serverName`),
			TypeName: harness.StrExpr("'enforced by'"),
		},
	}
}
