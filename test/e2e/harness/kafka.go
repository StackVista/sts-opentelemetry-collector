package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

const kafkaImage = "confluentinc/confluent-local:7.5.0"

type KafkaInstance struct {
	Container     testcontainers.Container
	HostAddr      string // host:port (for test code)
	ContainerAddr string // alias:port (for other containers)
	NetworkName   string
	ContainerName string
}

func withNetwork(networkName string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Networks = []string{networkName}

		return nil
	}
}

// StartKafka starts a Kafka broker for tests.
// The container is automatically terminated when the test finishes.
func StartKafka(ctx context.Context, t *testing.T, networkName string) *KafkaInstance {
	t.Helper()

	logger := zaptest.NewLogger(t)

	container, err := kafka.Run(ctx, kafkaImage, withNetwork(networkName))
	require.NoError(t, err)

	containerJSON, err := container.Inspect(ctx)
	require.NoError(t, err)
	containerName := strings.TrimPrefix(containerJSON.Name, "/")

	t.Cleanup(func() {
		_ = container.Terminate(ctx)
		logger.Info("Kafka broker (testcontainer) terminated", zap.String("containerName", containerName))
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	// Discover the mapped host port
	mappedPort, err := container.MappedPort(ctx, "9093/tcp")
	require.NoError(t, err)

	logger.Info(
		"Kafka broker (testcontainer) started",
		zap.String("network", networkName),
		zap.String("host", host),
		zap.String("mappedPort", mappedPort.Port()),
		zap.String("containerName", containerName),
	)

	return &KafkaInstance{
		Container:     container,
		HostAddr:      fmt.Sprintf("%s:%s", host, mappedPort.Port()),
		ContainerAddr: fmt.Sprintf("%s:%d", containerName, 9092), // internal listener
		NetworkName:   networkName,
		ContainerName: containerName,
	}
}

func CreateTopics(ctx context.Context, brokers string, topics []string) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		return err
	}
	defer client.Close()

	adm := kadm.NewClient(client)
	_, err = adm.CreateTopics(ctx, 1, 1, nil, topics...)
	return err
}

func UniqueTopic(topicName string) string {
	return fmt.Sprintf("%s-%s", topicName, uuid.NewString()[:8])
}

// PublishSettings writes settings protocol messages to Kafka.
func PublishSettings(
	t *testing.T,
	logger *zap.Logger,
	brokers string,
	settingsTopic string,
	snapshots ...TestSnapshot,
) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.AllowAutoTopicCreation(),
	)
	require.NoError(t, err)
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, s := range snapshots {
		records, err := s.Records(settingsTopic)
		require.NoError(t, err)

		for _, rec := range records {
			require.NoError(t, cl.ProduceSync(ctx, rec).FirstErr())
		}
	}

	logger.Info(
		"Published setting snapshots to Kafka",
		zap.String("topic", settingsTopic),
		zap.Int("# records", len(snapshots)),
	)

	// A bit of settle time to ensure setting snapshots are processed in the collector and subscribers notified
	time.Sleep(2 * time.Second)
}

type TopologyConsumer struct {
	client *kgo.Client
	logger *zap.Logger
	topic  string
}

func NewTopologyConsumer(brokers, topic, groupID string, logger *zap.Logger) (*TopologyConsumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.AutoCommitMarks(), // automatically commit offsets for marked records
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	return &TopologyConsumer{
		client: cl,
		logger: logger,
		topic:  topic,
	}, nil
}

func (tc *TopologyConsumer) Close() {
	tc.client.Close()
}

// ConsumeTopology reads N records of TopologyStreamMessage from the topology topic.
func (tc *TopologyConsumer) ConsumeTopology(
	ctx context.Context,
	minRecords int,
	pollDeadline time.Duration,
) ([]*kgo.Record, error) {
	var recs []*kgo.Record

	ctx, cancel := context.WithTimeout(ctx, pollDeadline)
	defer cancel()

	for {
		fetches := tc.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.DeadlineExceeded) || errors.Is(e.Err, context.Canceled) {
					tc.logger.Info("Kafka poll timeout or context canceled")
					return recs, nil
				}
				return recs, fmt.Errorf("kafka fetch error: %w", e.Err)
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			recs = append(recs, r)
			tc.client.MarkCommitRecords(r)
		})

		if len(recs) >= minRecords {
			break
		}

		if ctx.Err() != nil {
			tc.logger.Info("Context deadline reached before collecting enough records")
			break
		}
	}

	// Commit offsets to Kafka so subsequent calls resume from the next record
	if err := tc.client.CommitUncommittedOffsets(ctx); err != nil {
		tc.logger.Warn("Failed to commit offsets", zap.Error(err))
	}

	tc.logger.Info("Consumed records from Kafka", zap.String("topic", tc.topic), zap.Int("# records", len(recs)))
	return recs, nil
}
