package e2e_test

import (
	"context"
	"e2e/harness"
	"slices"
	"testing"
	"time"

	topo_stream_v1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
)

const testLevelTimeout = 2 * time.Minute

type TopologyKafkaTestEnv struct {
	Instance      *harness.KafkaInstance
	SettingsTopic string
	TopologyTopic string
}

type TopologyCollectorTestEnv struct {
	Instances []*harness.CollectorInstance
}

type TopologyTestEnv struct {
	//nolint:containedctx
	Ctx       context.Context
	Logger    *zap.Logger
	Kafka     TopologyKafkaTestEnv
	Collector TopologyCollectorTestEnv
	Cleanup   func()
}

func SetupTopologyTest(t *testing.T, numCollectors int) *TopologyTestEnv {
	t.Helper()

	// Create a timeout context for the whole test
	ctx, cancel := context.WithTimeout(context.Background(), testLevelTimeout)

	network := harness.EnsureNetwork(ctx, t)

	kafkaInstance := harness.StartKafka(ctx, t, network.Name)

	settingsTopic := harness.UniqueTopic("sts-internal-settings")
	topologyTopic := harness.UniqueTopic("sts-otel-topology")
	require.NoError(t, harness.CreateTopics(ctx, kafkaInstance.HostAddr, []string{settingsTopic, topologyTopic}))

	cfg := harness.CollectorConfig{
		NumCollectors:     numCollectors,
		DockerNetworkName: network.Name,
		KafkaBroker:       kafkaInstance.ContainerAddr,
		SettingsTopic:     settingsTopic,
		TopologyTopic:     topologyTopic,
	}

	collectors := harness.StartCollectors(ctx, t, cfg)

	cleanup := func() {
		cancel()
	}

	logger := zaptest.NewLogger(t)
	logger.Info("Test setup complete")

	return &TopologyTestEnv{
		Ctx:    ctx,
		Logger: logger,
		Kafka: TopologyKafkaTestEnv{
			Instance:      kafkaInstance,
			SettingsTopic: settingsTopic,
			TopologyTopic: topologyTopic,
		},
		Collector: TopologyCollectorTestEnv{
			Instances: collectors,
		},
		Cleanup: cleanup,
	}
}

func TestTraceToOtelTopology_CreateComponentAndRelationMappings(t *testing.T) {
	env := SetupTopologyTest(t, 1)
	defer env.Cleanup()

	publishSettingSnapshots(t, env)
	sendTraces(t, env)

	recs := consumeTopologyRecords(t, env, 2)
	components, relations, errs := extractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)

	assertComponents(t, components)
	assertRelations(t, relations)
}

func TestTraceToOtelTopology_UpdateComponentAndRelationMappings(t *testing.T) {
	env := SetupTopologyTest(t, 1)
	defer env.Cleanup()

	// Publish initial settings
	publishSettingSnapshots(t, env)
	sendTraces(t, env)
	recs := consumeTopologyRecords(t, env, 2)
	components, relations, errs := extractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)
	assertComponents(t, components)
	assertRelations(t, relations)

	// Publish updated settings
	newVersion := "1.2.4"
	publishUpdatedSettingSnapshots(t, env, newVersion)

	sendTraces(t, env)
	recs = consumeTopologyRecords(t, env, 2)
	components, relations, errs = extractComponentsAndRelations(t, recs)
	require.Len(t, errs, 0)

	// Assert that the updated component and relations
	require.Len(t, components, 1)
	found := false
	for _, c := range components {
		if c.Name == "checkout-service-updated" && c.ExternalId == "627cc493" && slices.Contains(c.Tags, "version:1.2.4") {
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
	env := SetupTopologyTest(t, 1)
	defer env.Cleanup()

	component := otelComponentMappingSnapshot()
	// modify base component mapping to have an invalid expression
	component.Output.Name = strExpr("${resourceAttributes}") // a map reference where a string expression is required
	harness.PublishSettings(t, env.Logger, env.Kafka.Instance.HostAddr, env.Kafka.SettingsTopic, component)
	numSpans := sendTraces(t, env)
	recs := consumeTopologyRecords(t, env, 1)

	components, relations, errs := extractComponentsAndRelations(t, recs)
	require.Len(t, components, 0)
	require.Len(t, relations, 0)
	// errs should equal numSpans because each span goes through mapping eval
	require.Len(t, errs, numSpans, "expected errors not returned")
	require.Contains(
		t,
		errs[0], // all the errors should be the same
		"expression did not evaluate to string",
		"expected error on incorrect mapping config",
	)
}

func publishSettingSnapshots(t *testing.T, env *TopologyTestEnv) {
	component := otelComponentMappingSnapshot()
	relation := otelRelationMappingSnapshot()
	harness.PublishSettings(t, env.Logger, env.Kafka.Instance.HostAddr, env.Kafka.SettingsTopic, component, relation)
}

func publishUpdatedSettingSnapshots(t *testing.T, env *TopologyTestEnv, newVersion string) {
	component := otelComponentMappingSnapshot()

	// Update 1: component name change
	component.Output.Name = strExpr("checkout-service-updated")

	if component.Output.Required.Tags == nil {
		component.Output.Required.Tags = &[]settings.OtelTagMapping{}
	}

	// Update 2: additional required tag
	*component.Output.Required.Tags = append(*component.Output.Required.Tags, settings.OtelTagMapping{
		Source: strExpr(newVersion),
		Target: "version",
	})

	// Update 3: relation name change
	relation := otelRelationMappingSnapshot()
	relation.Output.TypeName = strExpr("http-request-updated")

	harness.PublishSettings(t, env.Logger, env.Kafka.Instance.HostAddr, env.Kafka.SettingsTopic, component, relation)
}

// sendTraces builds trace and span data and calls harness.BuildAndSendTrace. It returns the number of spans sent.
func sendTraces(t *testing.T, env *TopologyTestEnv) int {
	endpoint := env.Collector.Instances[0].HostAddr
	traceData := *traceSpec()
	err := harness.BuildAndSendTrace(env.Ctx, env.Logger, endpoint, traceData)
	require.NoError(t, err)
	return len(traceData.Spans)
}

func consumeTopologyRecords(t *testing.T, env *TopologyTestEnv, minRecords int) []*kgo.Record {
	recs := harness.ConsumeTopology(env.Ctx, env.Logger, env.Kafka.Instance.HostAddr, env.Kafka.TopologyTopic, minRecords)
	require.NotEmpty(t, recs)
	return recs
}

func extractComponentsAndRelations(
	t *testing.T,
	recs []*kgo.Record,
) (map[string]*topo_stream_v1.TopologyStreamComponent, map[string]*topo_stream_v1.TopologyStreamRelation, []string) {
	components := make(map[string]*topo_stream_v1.TopologyStreamComponent)
	relations := make(map[string]*topo_stream_v1.TopologyStreamRelation)
	errs := make([]string, 0)

	for _, rec := range recs {
		var topoMsg topo_stream_v1.TopologyStreamMessage
		require.NoError(t, proto.Unmarshal(rec.Value, &topoMsg))
		require.NotNil(t, topoMsg.Payload)

		data := topoMsg.GetTopologyStreamRepeatElementsData()
		require.NotNil(t, data)
		for _, c := range data.Components {
			components[c.ExternalId] = c
		}
		for _, r := range data.Relations {
			relations[r.ExternalId] = r
		}
		errs = append(errs, data.Errors...)
	}

	return components, relations, errs
}

func assertComponents(t *testing.T, components map[string]*topo_stream_v1.TopologyStreamComponent) {
	require.Len(t, components, 1)
	for _, c := range components {
		require.Equal(t, "checkout-service", c.Name)
		require.Equal(t, "627cc493", c.ExternalId)
	}
}

func assertRelations(t *testing.T, relations map[string]*topo_stream_v1.TopologyStreamRelation) {
	require.Len(t, relations, 2)

	expectedRelations := []struct{ Source, Target string }{
		{"checkout-service", "payment-service"},
		{"checkout-service", "shipment-service"},
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
			"service.instance.id": "627cc493",
			"service.namespace":   "shop",
			"service.version":     "1.2.3",
			"host.name":           "ip-10-1-2-3.ec2.internal",
			"os.type":             "linux",
			"process.pid":         "12345",
			"cloud.provider":      "aws",
			"k8s.pod.name":        "checkout-service-8675309",
		},
		ScopeAttributes: map[string]string{
			"otel.scope.name":    "io.opentelemetry.instrumentation.http",
			"otel.scope.version": "1.17.0",
		},
		Spans: []harness.SpanSpec{
			{
				Name: "GET /checkout",
				Attributes: map[string]interface{}{
					"http.method":  "GET",
					"http.status":  200,
					"user.id":      "123",
					"db.system":    "postgresql",
					"db.statement": "SELECT * FROM orders WHERE user_id=123",
					"service.name": "checkout-service",
				},
			},
			{
				Name: "POST /payment",
				Attributes: map[string]interface{}{
					"http.method":  "POST",
					"http.status":  201,
					"user.id":      "123",
					"db.system":    "postgresql",
					"db.statement": "INSERT INTO payments ...",
					"service.name": "payment-service",
				},
			},
			{
				Name: "POST /shipment",
				Attributes: map[string]interface{}{
					"http.method":  "POST",
					"http.status":  202,
					"user.id":      "123",
					"db.system":    "postgresql",
					"db.statement": "INSERT INTO shipments ...",
					"service.name": "shipment-service",
				},
			},
		},
	}
}

func otelComponentMappingSnapshot() *harness.OtelComponentMappingSnapshot {
	return &harness.OtelComponentMappingSnapshot{
		SnapshotID:    "component-mapping-1",
		MappingID:     "comp-mapping-1",
		Name:          "checkout-service mapping",
		ExpireAfterMs: 60000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: boolExpr(`resourceAttributes["service.name"] == "checkout-service"`)},
		},
		Output: settings.OtelComponentMappingOutput{
			Identifier: strExpr("${vars.instanceId}"),
			Name:       strExpr(`${resourceAttributes["service.name"]}`),
			TypeName:   strExpr("service-instance"),
			DomainName: strExpr(`${resourceAttributes["service.namespace"]}`),
			LayerName:  strExpr("backend"),
			Required: &settings.OtelComponentMappingFieldMapping{
				AdditionalIdentifiers: &[]settings.OtelStringExpression{
					{Expression: `${resourceAttributes["k8s.pod.name"]}`},
				},
				Tags: &[]settings.OtelTagMapping{
					{
						Source: strExpr(`${resourceAttributes["host.name"]}`),
						Target: "host",
					},
				},
			},
		},
		Vars: []settings.OtelVariableMapping{
			{Name: "instanceId", Value: strExpr(`${resourceAttributes["service.instance.id"]}`)},
		},
	}
}

func otelRelationMappingSnapshot() *harness.OtelRelationMappingSnapshot {
	return &harness.OtelRelationMappingSnapshot{
		SnapshotID:    "relation-mapping-1",
		MappingID:     "rel-mapping-1",
		ExpireAfterMs: 300000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: boolExpr(`resourceAttributes["service.name"] != spanAttributes["service.name"] && spanAttributes["http.method"] != ""`)},
		},
		Output: settings.OtelRelationMappingOutput{
			SourceId: strExpr(`${resourceAttributes["service.name"]}`),
			TargetId: strExpr(`${spanAttributes["service.name"]}`),
			TypeName: strExpr("http-request"),
		},
	}
}

func strExpr(s string) settings.OtelStringExpression {
	return settings.OtelStringExpression{
		Expression: s,
	}
}

func boolExpr(s string) settings.OtelBooleanExpression {
	return settings.OtelBooleanExpression{
		Expression: s,
	}
}
