//go:build integration

package stskafkaexporter_test

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	testContainersKafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	"log"
	"testing"
	"time"
)

const (
	kafkaImageName = "confluentinc/confluent-local:7.5.0"
)

type testContext struct {
	ctx            context.Context
	cfg            *stskafkaexporter.Config
	exporter       *stskafkaexporter.KafkaExporter
	kafkaContainer *testContainersKafka.KafkaContainer
	cleanup        func()
}

func TestKafkaExporter_Integration(t *testing.T) {
	tc := setupTest(t)
	defer tc.cleanup()

	// Start exporter
	require.NoError(t, tc.exporter.Start(tc.ctx, componenttest.NewNopHost()), "failed to Start exporter")

	// Build logs
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()

	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetEmptyBytes().FromRaw([]byte("hello world"))
	lr.Attributes().PutStr("stskafka.key", "mykey")

	// Export
	require.NoError(t, tc.exporter.ExportData(tc.ctx, ld), "failed to export data")

	// Consume message and assert
	rec := consumeSingleKafkaRecord(t, tc.cfg.Brokers, tc.cfg.Topic)
	require.Equal(t, []byte("hello world"), rec.Value)
	require.Len(t, rec.Key, 8) // hashed key
}

func setupTest(t *testing.T) *testContext {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	kafkaContainer, err := testContainersKafka.Run(ctx, kafkaImageName)
	require.NoError(t, err, "failed to Start container")

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	topicName := fmt.Sprintf("sts-otel-topology-%s", uuid.New().String())
	createTopic(t, brokers, topicName)

	// Create exporter
	cfg := stskafkaexporter.Config{
		Brokers:        brokers,
		Topic:          topicName,
		ProduceTimeout: 10 * time.Second,
		ReadTimeout:    5 * time.Second,
		RequiredAcks:   "all",
	}
	set := exporter.CreateSettings{
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}
	exp, err := stskafkaexporter.NewKafkaExporter(cfg, set)
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, exp.Shutdown(ctx), "failed to Shutdown exporter")
		if err := kafkaContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
		cancel()
	}

	return &testContext{
		ctx:            ctx,
		cfg:            &cfg,
		exporter:       exp,
		kafkaContainer: kafkaContainer,
		cleanup:        cleanup,
	}
}

func createTopic(t *testing.T, brokers []string, topicName string) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)

	adminClient := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer func() {
		client.Close()
		cancel()
	}()

	resp, err := adminClient.CreateTopics(ctx, 1, 1, map[string]*string{}, topicName)
	require.NoError(t, err, "failed to create topic")

	for topic, topicResponse := range resp {
		if topicResponse.Err != nil {
			t.Fatalf("failed to create topic %s: %v", topic, err)
		}
	}

	t.Logf("Created topic: %s", topicName)
}

// consumeSingleKafkaRecord consumes one message from the given topic.
func consumeSingleKafkaRecord(t *testing.T, brokers []string, topic string) *kgo.Record {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("test-consumer-"+uuid.New().String()),
		kgo.ConsumeTopics(topic),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer func() {
		client.Close()
		cancel()
	}()

	for {
		fetches := client.PollFetches(ctx)
		require.NoError(t, fetches.Err())

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			if rec.Topic == topic {
				return rec
			}
		}

		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for message from Kafka")
		default:
		}
	}
}
