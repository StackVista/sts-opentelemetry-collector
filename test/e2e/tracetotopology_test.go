package e2e_test

import (
	"e2e/harness"
	"fmt"
	"slices"
	"testing"

	"github.com/google/uuid"
	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTraceToOtelTopology_CreateComponentAndRelationMappings(t *testing.T) {
	env := harness.SetupTopologyTest(t, 1)
	defer env.Cleanup()

	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(otelComponentMappingSpecCheckoutService()),
		otelRelationMappingSnapshot(otelRelationMappingSpec()),
	)
	sendTraces(t, env)

	recs := env.ConsumeTopologyRecords(t, 6)
	components, relations, errs := harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)

	assertComponents(t, components)
	assertRelations(t, relations)
}

func TestTraceToOtelTopology_UpdateComponentAndRelationMappings(t *testing.T) {
	env := harness.SetupTopologyTest(t, 1)
	defer env.Cleanup()

	// Publish initial settings
	component := otelComponentMappingSpecCheckoutService()
	relation := otelRelationMappingSpec()
	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(component),
		otelRelationMappingSnapshot(relation),
	)
	sendTraces(t, env)

	recs := env.ConsumeTopologyRecords(t, 6)
	components, relations, errs := harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)
	assertComponents(t, components)
	assertRelations(t, relations)

	// Update the settings
	newVersion := "1.2.4"

	// Component: update name + add tag
	component.Output.Name = harness.StrExpr("checkout-service-updated")
	if component.Output.Required.Tags == nil {
		component.Output.Required.Tags = &[]settings.OtelTagMapping{}
	}
	*component.Output.Required.Tags = append(*component.Output.Required.Tags, settings.OtelTagMapping{
		Source: harness.AnyExpr(newVersion),
		Target: "version",
	})

	// Relation: update name
	relation.Output.TypeName = harness.StrExpr("http-request-updated")

	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(component),
		otelRelationMappingSnapshot(relation),
	)
	sendTraces(t, env)

	recs = env.ConsumeTopologyRecords(t, 6)
	components, relations, errs = harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)

	// Assert that the component and relations mappings have updated values
	require.Len(t, components, 1)
	found := false
	for _, c := range components {
		if c.Name == "checkout-service-updated" && slices.Contains(c.Tags, "version:1.2.4") {
			found = true
		}
	}
	require.True(t, found, "expected updated component mapping not found")

	assertRelations(t, relations)
	for _, r := range relations {
		require.Equal(t, "http-request-updated", r.TypeName, "expected updated relation mapping not found")
	}
}

func TestTraceToOtelTopology_ErrorReturnedOnIncorrectMappingConfig(t *testing.T) {
	env := harness.SetupTopologyTest(t, 1)
	defer env.Cleanup()

	component := otelComponentMappingSpecCheckoutService()
	// modify base component mapping to have an invalid expression
	component.Output.Name = harness.StrExpr("${resourceAttributes}") // a map reference where a string expression is required
	env.PublishSettingSnapshots(t, otelComponentMappingSnapshot(component))

	sendTraces(t, env)
	recs := env.ConsumeTopologyRecords(t, 1)

	components, relations, errs := harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, components, 0)
	require.Len(t, relations, 0)
	// errs should equal numSpans because each span goes through mapping eval
	require.Greater(t, len(errs), 0, "expected errors not returned")
	require.Contains(
		t,
		errs[0].Message, // all the errors should be the same
		"expected string type, got: map(string, dyn), for expression '${resourceAttributes}'",
		"expected error on incorrect mapping config",
	)
}

func TestTraceToOtelTopology_RemovesMappingsWhenOmittedFromNextSnapshot(t *testing.T) {
	env := harness.SetupTopologyTest(t, 1)
	defer env.Cleanup()

	// Note, the component mappings are somewhat contrived here, but it's to simulate a scenario where two
	// mappings are created, after one is removed from the next snapshot.

	// Publish initial snapshots with two mappings of each type
	component1 := otelComponentMappingSpecCheckoutService()
	component1.MappingID = "comp-1"
	component1.MappingIdentifier = "urn:comp-checkout-service"
	component2 := otelComponentMappingSpecPeerService("payment-service")
	component2.MappingID = "comp-2"
	component2.MappingIdentifier = "urn:comp-payment-service"
	component3 := otelComponentMappingSpecPeerService("shipment-service")
	component3.MappingID = "comp-3"
	component3.MappingIdentifier = "urn:comp-shipment-service"

	relation1 := otelRelationMappingSpec()

	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(component1, component2, component3),
		otelRelationMappingSnapshot(relation1),
	)
	sendTraces(t, env)

	recs := env.ConsumeTopologyRecords(t, 8)
	components, relations, errs := harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)
	require.Len(t, components, 3) // checkout-service, payment-service, shipment-service
	require.Len(t, relations, 2)

	// Publish new snapshots with one less mapping
	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(component1), // "remove" peer component mapping
		otelRelationMappingSnapshot(relation1),
	)

	// First, check that TopologyStreamRemove messages are sent for the removed mappings
	recs = env.ConsumeTopologyRecords(t, 10) // note, we're only expecting 8: 2 components * 4 shards, but making it 10 so we can fail if there are any extra messages
	foundRemovals := 0
	for _, rec := range recs {
		var msg topostreamv1.TopologyStreamMessage
		require.NoError(t, proto.Unmarshal(rec.Value, &msg))

		if rm := msg.GetTopologyStreamRemove(); rm != nil {
			foundRemovals++
			require.Contains(t, rm.RemovalCause, "Setting with identifier")
		}
	}
	require.Equal(t, foundRemovals, 8, "expected removal messages")

	// Then, send traces to ensure that the removed mappings are not used
	sendTraces(t, env)
	recs = env.ConsumeTopologyRecords(t, 6)
	components, relations, errs = harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)
	require.Len(t, components, 1) // checkout-service only
	require.Len(t, relations, 2)
}

// sendTraces builds trace and span data and calls harness.BuildAndSendTrace.
func sendTraces(t *testing.T, env *harness.TopologyTestEnv) {
	endpoint := env.Collector.Instances[0].HostAddr
	traceData := *traceSpec()
	err := harness.BuildAndSendTrace(env.Ctx, env.Logger, endpoint, traceData)
	require.NoError(t, err)
}

func assertComponents(t *testing.T, components map[string]*topostreamv1.TopologyStreamComponent) {
	require.Len(t, components, 1)
	for _, c := range components {
		require.Equal(t, "checkout-service", c.Name)
		require.Equal(t, "checkout-service", c.ExternalId)
	}
}

func assertRelations(t *testing.T, relations map[string]*topostreamv1.TopologyStreamRelation) {
	require.Len(t, relations, 2)

	expectedRelations := []struct{ Source, Target string }{
		{"checkout-service", "payment-service"},
		{"payment-service", "shipment-service"},
	}

	for _, r := range relations {
		found := false
		for _, expected := range expectedRelations {
			if r.SourceIdentifier == expected.Source && r.TargetIdentifier == expected.Target {
				found = true
				break
			}
		}
		require.True(t, found, "unexpected relation: %s -> %s", r.SourceIdentifier, r.TargetIdentifier)
	}
}

func traceSpec() *harness.TraceSpec {
	return &harness.TraceSpec{
		ResourceAttributes: map[string]string{
			"service.name":        "checkout-service",
			"service.namespace":   "shop",
			"service.version":     "1.2.3",
			"service.instance.id": "627cc493",
			"cloud.provider":      "aws",
			"k8s.pod.name":        "checkout-service-8675309",
			"host.name":           "ip-10-1-2-3.ec2.internal",
		},
		ScopeAttributes: map[string]string{
			"otel.scope.name":    "io.opentelemetry.instrumentation.http",
			"otel.scope.version": "1.17.0",
		},
		Spans: []harness.SpanSpec{
			{
				// Root span doesn't need a ParentID, but logically a child will reference it with '1'
				Name: "GET /checkout",
				Attributes: map[string]interface{}{
					"http.method":          "GET",
					"http.route":           "/checkout",
					"http.status_code":     200,
					"net.protocol.version": "1.1",
					"url.path":             "/checkout",
					"server.address":       "checkout-service",
				},
				Children: []harness.SpanSpec{
					{
						Name: "SELECT * FROM orders WHERE user_id=?",
						Attributes: map[string]interface{}{
							"db.system":     "postgresql",
							"db.statement":  "SELECT * FROM orders WHERE user_id=123",
							"db.operation":  "SELECT",
							"db.name":       "shop_db",
							"net.peer.name": "db-host",
						},
					},
				},
			},
			{
				Name:     "POST /payment",
				ParentID: "1",
				Attributes: map[string]interface{}{
					"http.method":      "POST",
					"http.status_code": 201,
					"server.address":   "payment-service",
					"client.address":   "checkout-service",
					"net.peer.name":    "payment-service",
				},
			},
			{
				Name:     "POST /shipment",
				ParentID: "2",
				Attributes: map[string]interface{}{
					"http.method":      "POST",
					"http.status_code": 202,
					"server.address":   "shipment-service",
					"client.address":   "payment-service",
					"net.peer.name":    "shipment-service",
				},
			},
		},
	}
}

func otelComponentMappingSnapshot(mappings ...*harness.OtelComponentMappingSpec) *harness.OtelComponentMappingSnapshot {
	return &harness.OtelComponentMappingSnapshot{
		SnapshotID: uuid.NewString(),
		Mappings:   mappings,
	}
}

// Note, technically otelComponentMappingSpecCheckoutService and otelComponentMappingSpecPeerService can easily be
// combined into a single component mapping spec. However, to prevent having complex trace and test data, we're keeping
// them separate to cover various e2e flows/scenarios.

func otelComponentMappingSpecCheckoutService() *harness.OtelComponentMappingSpec {
	return otelComponentMappingSpec(
		harness.BoolExpr(`resourceAttributes["service.name"] == "checkout-service"`),
		settings.OtelVariableMapping{
			Name: "name", Value: harness.AnyExpr(`${resourceAttributes["service.name"]}`),
		},
		settings.OtelVariableMapping{
			Name: "instanceId", Value: harness.AnyExpr(`${resourceAttributes["service.name"]}`),
		},
	)
}

func otelComponentMappingSpecPeerService(peerService string) *harness.OtelComponentMappingSpec {
	return otelComponentMappingSpec(
		harness.BoolExpr(fmt.Sprintf(`spanAttributes["net.peer.name"] == "%s"`, peerService)),
		settings.OtelVariableMapping{
			Name: "name", Value: harness.AnyExpr(`${"net.peer.name" in spanAttributes ? spanAttributes["net.peer.name"] : resourceAttributes["service.name"]}`),
		},
		settings.OtelVariableMapping{
			Name: "instanceId", Value: harness.AnyExpr(`${"net.peer.name" in spanAttributes ? spanAttributes["net.peer.name"] : resourceAttributes["service.name"]}`),
		},
	)
}

func otelComponentMappingSpec(condExpression settings.OtelBooleanExpression, varMappings ...settings.OtelVariableMapping) *harness.OtelComponentMappingSpec {
	return &harness.OtelComponentMappingSpec{
		MappingID:         "comp-mapping-1",
		MappingIdentifier: "urn:comp-mapping-1",
		Name:              "service mapping",
		ExpireAfterMs:     60000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: condExpression},
		},
		Output: settings.OtelComponentMappingOutput{
			Identifier: harness.StrExpr("${vars.instanceId}"),
			Name:       harness.StrExpr(`${vars.name}`),
			TypeName:   harness.StrExpr("service-instance"),
			DomainName: harness.StrExpr(`${resourceAttributes["service.namespace"]}`),
			LayerName:  harness.StrExpr("backend"),
			Required: &settings.OtelComponentMappingFieldMapping{
				AdditionalIdentifiers: &[]settings.OtelStringExpression{
					{Expression: `${resourceAttributes["k8s.pod.name"]}`},
				},
				Tags: &[]settings.OtelTagMapping{
					{
						Source: harness.AnyExpr(`${resourceAttributes["host.name"]}`),
						Target: "host",
					},
				},
			},
		},
		InputSignals: []settings.OtelInputSignal{settings.TRACES},
		Vars:         varMappings,
	}
}

func otelRelationMappingSnapshot(mappings ...*harness.OtelRelationMappingSpec) *harness.OtelRelationMappingSnapshot {
	return &harness.OtelRelationMappingSnapshot{
		SnapshotID: uuid.NewString(),
		Mappings:   mappings,
	}
}

func otelRelationMappingSpec() *harness.OtelRelationMappingSpec {
	return &harness.OtelRelationMappingSpec{
		MappingID:         "rel-mapping-1",
		MappingIdentifier: "urn:rel-mapping-1",
		ExpireAfterMs:     300000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: harness.BoolExpr(`"client.address" in spanAttributes && "server.address" in spanAttributes`)},
		},
		Output: settings.OtelRelationMappingOutput{
			SourceId: harness.StrExpr(`${spanAttributes["client.address"]}`),
			TargetId: harness.StrExpr(`${spanAttributes["server.address"]}`),
			TypeName: harness.StrExpr("http-request"),
		},
		InputSignals: []settings.OtelInputSignal{settings.TRACES},
	}
}
