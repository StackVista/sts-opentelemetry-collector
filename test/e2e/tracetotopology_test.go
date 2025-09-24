package e2e

import (
	"context"
	"e2e/harness"
	topo_stream_v1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
	"log"
	"testing"
	"time"
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

func TestTraceToOtelTopology_CreateMapping(t *testing.T) {
	env := SetupTopologyTest(t, 1)
	defer env.Cleanup()

	// Publish settings
	component := otelComponentMappingSnapshot()
	relation := otelRelationMappingSnapshot()
	harness.PublishSettings(t, env.Logger, env.Kafka.Instance.HostAddr, env.Kafka.SettingsTopic, component, relation)

	// Send traces to the collector
	endpoint := env.Collector.Instances[0].HostAddr
	err := harness.BuildAndSendTrace(env.Ctx, env.Logger, endpoint, *traceSpec())
	require.NoError(t, err)

	// Verify topology records in Instance
	recs := harness.ConsumeTopology(env.Ctx, env.Logger, env.Kafka.Instance.HostAddr, env.Kafka.TopologyTopic, 2)

	for _, rec := range recs {
		var topoMsg topo_stream_v1.TopologyStreamMessage
		if err := proto.Unmarshal(rec.Value, &topoMsg); err != nil {
			env.Logger.Fatal("failed to unmarshal record", zap.Error(err))
		}

		// Optionally, assert on fields
		require.NotNil(t, topoMsg.Payload)
		for _, c := range topoMsg.GetTopologyStreamRepeatElementsData().Components {
			env.Logger.Info("Component", zap.String("name", c.Name), zap.String("externalId", c.ExternalId))
		}
		for _, r := range topoMsg.GetTopologyStreamRepeatElementsData().Relations {
			env.Logger.Info("Relation", zap.String("source", r.SourceIdentifier), zap.String("target", r.TargetIdentifier))
			log.Printf("Relation: %s -> %s", r.SourceIdentifier, r.TargetIdentifier)
		}
	}
	require.NotEmpty(t, recs)
}

func traceSpec() *harness.TraceSpec {
	now := time.Now().UnixMilli()

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
			"otel.scope.name":        "io.opentelemetry.instrumentation.http",
			"otel.scope.version":     "1.17.0",
			"otel.scope.description": "HTTP Client Instrumentation",
		},
		Spans: []harness.SpanSpec{
			{
				Name: "GET /checkout",
				Attributes: map[string]interface{}{
					"http.method":         "GET",
					"http.status":         200,
					"user.id":             "123",
					"db.system":           "postgresql",
					"db.statement":        "SELECT * FROM orders WHERE user_id=123",
					"net.peer.name":       "orders-db",
					"submitted.timestamp": now,
				},
			},
			{
				Name: "POST /payment",
				Attributes: map[string]interface{}{
					"http.method":         "POST",
					"http.status":         201,
					"user.id":             "123",
					"db.system":           "postgresql",
					"db.statement":        "INSERT INTO payments ...",
					"net.peer.name":       "payments-db",
					"submitted.timestamp": now,
				},
			},
			{
				Name: "POST /shipment",
				Attributes: map[string]interface{}{
					"http.method":         "POST",
					"http.status":         202,
					"user.id":             "123",
					"db.system":           "postgresql",
					"db.statement":        "INSERT INTO shipments ...",
					"net.peer.name":       "shipments-db",
					"submitted.timestamp": now,
				},
			},
		},
	}
}

func otelComponentMappingSnapshot() *harness.OtelComponentMappingSnapshot {
	return &harness.OtelComponentMappingSnapshot{
		SnapshotID:    "snap-comp-1",
		MappingID:     "mapping1a",
		Name:          "checkout-service mapping",
		ExpireAfterMs: 60000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: boolExpr(`vars.instanceId == "627cc493"`)},
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
			Optional: &settings.OtelComponentMappingFieldMapping{
				AdditionalIdentifiers: &[]settings.OtelStringExpression{
					{Expression: `${resourceAttributes["service.instance.id"]}`},
				},
				Tags: &[]settings.OtelTagMapping{
					{
						Source: strExpr(`${scopeAttributes["otel.scope.name"]}`),
						Target: "instrumentation-lib",
					},
					{
						Source: strExpr(`${scopeAttributes["otel.scope.version"]}`),
						Target: "instrumentation-version",
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
		SnapshotID:    "snap-rel-1",
		MappingID:     "mapping1b",
		Name:          "checkout->web",
		ExpireAfterMs: 300000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: boolExpr(`spanAttributes["http.method"] == "GET"`)},
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
