package harness

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
)

const testLevelTimeout = 2 * time.Minute

type TopologyKafkaTestEnv struct {
	Instance         *KafkaInstance
	SettingsTopic    string
	TopologyTopic    string
	TopologyConsumer *TopologyConsumer
}

type TopologyCollectorTestEnv struct {
	Instances []*CollectorInstance
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

	network := EnsureNetwork(ctx, t)

	kafkaInstance := StartKafka(ctx, t, network.Name)

	settingsTopic := UniqueTopic("sts-internal-settings")
	topologyTopic := UniqueTopic("sts-otel-topology")
	require.NoError(t, CreateTopics(ctx, kafkaInstance.HostAddr, []string{settingsTopic, topologyTopic}))

	cfg := CollectorConfig{
		NumCollectors:     numCollectors,
		DockerNetworkName: network.Name,
		KafkaBroker:       kafkaInstance.ContainerAddr,
		SettingsTopic:     settingsTopic,
		TopologyTopic:     topologyTopic,
	}

	collectors := StartCollectors(ctx, t, cfg)

	logger := zaptest.NewLogger(t)
	logger.Info("Test setup complete")

	consumer, err := NewTopologyConsumer(
		kafkaInstance.HostAddr, topologyTopic, fmt.Sprintf("topology-consumer-%s", uuid.NewString()), logger,
	)
	require.NoError(t, err)

	cleanup := func() {
		cancel()
		consumer.Close()
	}

	return &TopologyTestEnv{
		Ctx:    ctx,
		Logger: logger,
		Kafka: TopologyKafkaTestEnv{
			Instance:         kafkaInstance,
			SettingsTopic:    settingsTopic,
			TopologyTopic:    topologyTopic,
			TopologyConsumer: consumer,
		},
		Collector: TopologyCollectorTestEnv{
			Instances: collectors,
		},
		Cleanup: cleanup,
	}
}

func (env *TopologyTestEnv) ConsumeTopologyRecords(t *testing.T, minRecords int) []*kgo.Record {
	recs, err := env.Kafka.TopologyConsumer.ConsumeTopology(env.Ctx, minRecords, time.Second*5)
	require.NoError(t, err)
	require.NotEmpty(t, recs)
	return recs
}

func (env *TopologyTestEnv) PublishSettingSnapshots(t *testing.T, snapshots ...TestSnapshot) {
	PublishSettings(t, env.Logger, env.Kafka.Instance.HostAddr, env.Kafka.SettingsTopic, snapshots...)
}

// extractComponentsAndRelations extracts the components and relations from the topology (Kafka) records.
// It does basic deduplication of components and relations based on external ID.
// It returns a map of component external IDs to components, a map of relation external IDs to relations,
// and a slice of TopoStreamError messages.
func ExtractComponentsAndRelations(
	t *testing.T,
	recs []*kgo.Record,
) (
	map[string]*topostreamv1.TopologyStreamComponent,
	map[string]*topostreamv1.TopologyStreamRelation,
	[]*topostreamv1.TopoStreamError,
) {
	components := make(map[string]*topostreamv1.TopologyStreamComponent)
	relations := make(map[string]*topostreamv1.TopologyStreamRelation)
	errs := make([]*topostreamv1.TopoStreamError, 0)

	for _, rec := range recs {
		var topoMsg topostreamv1.TopologyStreamMessage
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

func AnyExpr(s string) settings.OtelAnyExpression {
	return settings.OtelAnyExpression{
		Expression: s,
	}
}

func StrExpr(s string) settings.OtelStringExpression {
	return settings.OtelStringExpression{
		Expression: s,
	}
}

func BoolExpr(s string) settings.OtelBooleanExpression {
	return settings.OtelBooleanExpression{
		Expression: s,
	}
}
