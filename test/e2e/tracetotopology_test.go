package e2e

import (
	"context"
	"e2e/harness"
	"github.com/stretchr/testify/require"
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

	// Collector config template
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

	return &TopologyTestEnv{
		Ctx: ctx,
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

func TestTraceToOtelTopology_Correctness(t *testing.T) {
	env := SetupTopologyTest(t, 1)
	defer env.Cleanup()

	// Publish settings
	harness.PublishSettings(t, env.Kafka.Instance.HostAddr, env.Kafka.SettingsTopic)

	// Send traces to the collector
	endpoint := env.Collector.Instances[0].HostAddr
	require.NoError(t, harness.SendTracesRoundRobin(env.Ctx, []string{endpoint}, 10))

	// Verify topology records in Instance
	recs := harness.ConsumeTopology(env.Ctx, t, env.Kafka.Instance.HostAddr, env.Kafka.TopologyTopic, 5)
	require.NotEmpty(t, recs)
}
