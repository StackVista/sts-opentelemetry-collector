package e2e_test

import (
	slices "slices"
	"testing"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"e2e/harness"
)

func TestMetricToOtelTopology_CreateComponentAndRelationMappings(t *testing.T) {
	env := harness.SetupTopologyTest(t, 1)
	defer env.Cleanup()

	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(
			otelComponentMappingSpecForService(),
			otelComponentMappingSpecForQueue(),
		),
		otelRelationMappingSnapshot(otelRelationMappingSpecForMetrics()),
	)
	sendMetrics(t, env)

	recs := env.ConsumeTopologyRecords(t, 6)
	components, relations, errs := harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)

	assertMetricComponents(t, components)
	assertMetricRelations(t, relations)
}

func TestMetricToOtelTopology_UpdateComponentAndRelationMappings(t *testing.T) {
	env := harness.SetupTopologyTest(t, 1)
	defer env.Cleanup()

	// Publish initial settings
	serviceComponent := otelComponentMappingSpecForService()
	queueComponent := otelComponentMappingSpecForQueue()
	relation := otelRelationMappingSpecForMetrics()
	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(serviceComponent, queueComponent),
		otelRelationMappingSnapshot(relation),
	)
	sendMetrics(t, env)

	recs := env.ConsumeTopologyRecords(t, 6)
	components, relations, errs := harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)
	assertMetricComponents(t, components)
	assertMetricRelations(t, relations)

	// Update the settings
	newVersion := "2.0.0"

	// Component: update name + add tag
	serviceComponent.Output.Name = harness.StrExpr("billing-service-updated")
	if serviceComponent.Output.Required == nil {
		serviceComponent.Output.Required = &settings.OtelComponentMappingFieldMapping{}
	}
	if serviceComponent.Output.Required.Tags == nil {
		serviceComponent.Output.Required.Tags = &[]settings.OtelTagMapping{}
	}
	*serviceComponent.Output.Required.Tags = append(*serviceComponent.Output.Required.Tags, settings.OtelTagMapping{
		Source: harness.AnyExpr(newVersion),
		Target: "version",
	})

	// Relation: update type name
	relation.Output.TypeName = harness.StrExpr("calls-updated")

	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(serviceComponent, queueComponent),
		otelRelationMappingSnapshot(relation),
	)
	sendMetrics(t, env)

	recs = env.ConsumeTopologyRecords(t, 6)
	components, relations, errs = harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)

	// Assert that the component and relations mappings have updated values
	require.Len(t, components, 2)
	foundUpdatedBilling := false
	for _, c := range components {
		if c.ExternalId == "urn:service:billing-service" && c.Name == "billing-service-updated" && slices.Contains(c.Tags, "version:2.0.0") {
			foundUpdatedBilling = true
		}
	}
	require.True(t, foundUpdatedBilling, "expected updated billing service component mapping not found")

	require.Len(t, relations, 1)
	for _, r := range relations {
		require.Equal(t, "calls-updated", r.TypeName, "expected updated relation mapping not found")
	}
}

func TestMetricToOtelTopology_ErrorReturnedOnIncorrectMappingConfig(t *testing.T) {
	env := harness.SetupTopologyTest(t, 1)
	defer env.Cleanup()

	component := otelComponentMappingSpecForService()
	// modify base component mapping to have an invalid expression
	component.Output.Name = harness.StrExpr(`${resourceAttributes}`) // a map reference where a string expression is required
	env.PublishSettingSnapshots(t, otelComponentMappingSnapshot(component))

	sendMetrics(t, env)
	recs := env.ConsumeTopologyRecords(t, 1) // Expecting 1 record for the error

	components, relations, errs := harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, components, 0)
	require.Len(t, relations, 0)
	// errs should equal numMetrics because each metric goes through mapping eval
	require.Greater(t, len(errs), 0, "expected errors not returned")
	require.Contains(
		t,
		errs[0].Message, // all the errors should be the same
		"expected string type, got: map(string, dyn), for expression '${resourceAttributes}'",
		"expected error on incorrect mapping config",
	)
}

func TestMetricToOtelTopology_RemovesMappingsWhenOmittedFromNextSnapshot(t *testing.T) {
	env := harness.SetupTopologyTest(t, 1)
	defer env.Cleanup()

	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(otelComponentMappingSpecForService(), otelComponentMappingSpecForQueue()),
		otelRelationMappingSnapshot(otelRelationMappingSpecForMetrics()),
	)
	sendMetrics(t, env)

	recs := env.ConsumeTopologyRecords(t, 6)
	components, relations, errs := harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)
	require.Len(t, components, 2)
	require.Len(t, relations, 1)

	// Publish new snapshots with one less component mapping
	env.PublishSettingSnapshots(
		t,
		otelComponentMappingSnapshot(otelComponentMappingSpecForService()),
		otelRelationMappingSnapshot(otelRelationMappingSpecForMetrics()),
	)

	// First, check that TopologyStreamRemove messages are sent for the removed mappings
	recs = env.ConsumeTopologyRecords(t, 4)
	foundRemovals := 0
	for _, rec := range recs {
		var msg topostreamv1.TopologyStreamMessage
		require.NoError(t, proto.Unmarshal(rec.Value, &msg))

		if rm := msg.GetTopologyStreamRemove(); rm != nil {
			foundRemovals++
			require.Contains(t, rm.RemovalCause, "Setting with identifier")
		}
	}
	require.Equal(t, foundRemovals, 4, "expected removal messages")

	// Then, send metrics to ensure that the removed mappings are not used
	sendMetrics(t, env)
	recs = env.ConsumeTopologyRecords(t, 6)
	components, relations, errs = harness.ExtractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)
	require.Len(t, components, 1) // billing-service
	require.Len(t, relations, 1)
}

// sendMetrics builds metric data and calls harness.BuildAndSendMetrics.
func sendMetrics(t *testing.T, env *harness.TopologyTestEnv) {
	endpoint := env.Collector.Instances[0].HostAddr
	metricData := *metricSpecWithRelation()
	err := harness.BuildAndSendMetrics(env.Ctx, env.Logger, endpoint, metricData)
	require.NoError(t, err)
}

func assertMetricComponents(t *testing.T, components map[string]*topostreamv1.TopologyStreamComponent) {
	require.Len(t, components, 2)

	// Assert billing-service component
	billingService, ok := components["urn:service:billing-service"]
	require.True(t, ok)
	require.Equal(t, "billing-service", billingService.Name)
	require.Equal(t, "service", billingService.TypeName)

	// Assert queue component
	queue, ok := components["urn:queue:billing-service:orders"]
	require.True(t, ok)
	require.Equal(t, "orders", queue.Name)
	require.Equal(t, "queue", queue.TypeName)
}

func assertMetricRelations(t *testing.T, relations map[string]*topostreamv1.TopologyStreamRelation) {
	require.Len(t, relations, 1)

	relation, ok := relations["urn:service:billing-service-urn:service:payment-service"]
	require.True(t, ok)
	require.Equal(t, "urn:service:billing-service", relation.SourceIdentifier)
	require.Equal(t, "urn:service:payment-service", relation.TargetIdentifier)
	require.Equal(t, "calls", relation.TypeName)
}

func metricSpecWithRelation() *harness.MetricSpec {
	return &harness.MetricSpec{
		ResourceAttributes: map[string]string{
			"service.name":        "billing-service",
			"service.namespace":   "shop",
			"service.instance.id": "billing-service-1",
		},
		Gauges: []harness.GaugeSpec{
			{
				Name: "billing.request.duration",
				Attributes: map[string]interface{}{
					"http.method":    "POST",
					"http.route":     "/process",
					"client.service": "billing-service",
					"server.service": "payment-service",
					"queue.name":     "orders",
				},
				Value: 150.0,
			},
		},
	}
}

func otelComponentMappingSpecForService() *harness.OtelComponentMappingSpec {
	return &harness.OtelComponentMappingSpec{
		MappingID:         "service",
		MappingIdentifier: "urn:metrics-service",
		Name:              "service",
		ExpireAfterMs:     60000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: harness.BoolExpr(`'service.name' in resourceAttributes`)},
		},
		Output: settings.OtelComponentMappingOutput{
			Identifier: harness.StrExpr(`urn:service:${resourceAttributes["service.name"]}`),
			Name:       harness.StrExpr(`${resourceAttributes["service.name"]}`),
			TypeName:   harness.StrExpr("service"),
			DomainName: harness.StrExpr(`${resourceAttributes["service.namespace"]}`),
			LayerName:  harness.StrExpr("backend"),
		},
		InputSignals: []settings.OtelInputSignal{settings.METRICS},
	}
}

func otelComponentMappingSpecForQueue() *harness.OtelComponentMappingSpec {
	return &harness.OtelComponentMappingSpec{
		MappingID:         "metrics-queue",
		MappingIdentifier: "urn:metrics-queue",
		Name:              "queue mapping",
		ExpireAfterMs:     60000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: harness.BoolExpr(`"queue.name" in metricAttributes`)},
		},
		Output: settings.OtelComponentMappingOutput{
			Identifier: harness.StrExpr(`urn:queue:${resourceAttributes["service.name"]}:${metricAttributes["queue.name"]}`),
			Name:       harness.StrExpr(`${metricAttributes["queue.name"]}`),
			TypeName:   harness.StrExpr("queue"),
			DomainName: harness.StrExpr(`${resourceAttributes["service.namespace"]}`),
			LayerName:  harness.StrExpr("backend"),
		},
		InputSignals: []settings.OtelInputSignal{settings.METRICS},
	}
}

func otelRelationMappingSpecForMetrics() *harness.OtelRelationMappingSpec {
	return &harness.OtelRelationMappingSpec{
		MappingID:         "rel-mapping-metrics-1",
		MappingIdentifier: "urn:rel-mapping-metrics-1",
		ExpireAfterMs:     300000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: harness.BoolExpr(`"client.service" in metricAttributes && "server.service" in metricAttributes`)},
		},
		Output: settings.OtelRelationMappingOutput{
			SourceId: harness.StrExpr(`urn:service:${metricAttributes["client.service"]}`),
			TargetId: harness.StrExpr(`urn:service:${metricAttributes["server.service"]}`),
			TypeName: harness.StrExpr("calls"),
		},
		InputSignals: []settings.OtelInputSignal{settings.METRICS},
	}
}
