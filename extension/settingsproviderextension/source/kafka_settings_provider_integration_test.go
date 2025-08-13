//go:build integration

package source

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal"
	"github.com/stretchr/testify/require"
	testContainersKafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

const (
	kafkaImageName = "confluentinc/confluent-local:7.5.0"
)

func TestKafkaSettingsProvider_MultipleSettingTypes(t *testing.T) {
	ctx, provider, writer, cleanup := setupTestInfra(t)
	defer cleanup()

	// --- ROUND 1: Publish initial state with two different setting types ---
	componentMappingId1 := "11111"
	otelComponentMappingSnapshot1 := newOtelComponentMappingSnapshot(t, uuid.New().String(), componentMappingId1, "Host component V1")
	relationMappingId1 := "22222"
	otelRelationMappingSnapshot := newOtelRelationMappingSnapshot(t, uuid.New().String(), relationMappingId1, "Runs on host")

	// Combine and write the first batch of messages
	initialMessages := append(otelComponentMappingSnapshot1, otelRelationMappingSnapshot...)
	err := writer.WriteMessages(ctx, initialMessages...)
	require.NoError(t, err, "Failed to write initial messages")

	// Wait for the provider to process the first batch
	select {
	case <-provider.RegisterForUpdates():
		t.Log("Update signal received after initial publish.")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for update signal after initial publish")
	}
	time.Sleep(2 * time.Second) // Settle time

	// Assert the initial state is correct
	currentSettings1 := provider.GetCurrentSettings()
	assert.Len(t, currentSettings1, 2, "Should be 2 settings after initial publish")
	_, ok := currentSettings1[componentMappingId1]
	require.True(t, ok, "Initial OtelComponentMapping should exist")
	_, ok = currentSettings1[relationMappingId1]
	require.True(t, ok, "OtelRelationMapping should exist")

	// --- ROUND 2: Publish an update for one setting type to test compaction ---
	componentMappingId2 := "33333"
	otelComponentMappingSnapshot2 := newOtelComponentMappingSnapshot(t, uuid.New().String(), componentMappingId2, "Host component V2")

	// Write the new snapshot, which should replace all previous OtelComponentMapping settings
	err = writer.WriteMessages(ctx, otelComponentMappingSnapshot2...)
	require.NoError(t, err, "Failed to write updated messages")

	// Wait for the provider to process the update
	select {
	case <-provider.RegisterForUpdates():
		t.Log("Update signal received after compaction publish.")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for update signal after compaction publish")
	}
	time.Sleep(2 * time.Second) // Settle time

	// --- FINAL ASSERT: Verify the state reflects the compaction ---
	finalSettings := provider.GetCurrentSettings()
	assert.Len(t, finalSettings, 2, "Should still be 2 settings after compaction, not 3")

	// 1. The OLD OtelComponentMapping should be GONE.
	_, ok = finalSettings[componentMappingId1]
	assert.False(t, ok, "Old OtelComponentMapping (V1) should have been removed")

	// 2. The NEW OtelComponentMapping should be PRESENT.
	newMapping, ok := finalSettings[componentMappingId2]
	require.True(t, ok, "New OtelComponentMapping (V2) should exist")
	newOtelCompMapping, err := newMapping.AsOtelComponentMapping()
	require.NoError(t, err)
	assert.Equal(t, "Host component V2", newOtelCompMapping.Name)

	// 3. The OTHER setting type (OtelRelationMapping) should be UNCHANGED.
	relationMapping, ok := finalSettings[relationMappingId1]
	require.True(t, ok, "OtelRelationMapping should be unaffected by the compaction of another type")
	otelRelationMapping, err := relationMapping.AsOtelRelationMapping()
	require.NoError(t, err)
	assert.Equal(t, "Runs on host", otelRelationMapping.Name)
}

// setupTestInfra initializes the test environment (Kafka container, topic, kafka settings provider, kafka writer).
// It returns the context, the kafka settings provider, the Kafka writer, and a cleanup function to be deferred.
func setupTestInfra(t *testing.T) (context.Context, *kafkaSettingProvider, *kafka.Writer, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	kafkaContainer, err := testContainersKafka.Run(ctx, kafkaImageName)
	require.NoError(t, err, "failed to start container")

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	topicName := fmt.Sprintf("sts-internal-settings-%s", uuid.New().String())
	createCompactedTopic(t, brokers[0], topicName)

	providerCfg := &stsSettingsConfig.KafkaSettingsProviderConfig{Brokers: brokers, Topic: topicName, BufferSize: 1000}
	logger, _ := zap.NewDevelopment()
	provider, err := NewKafkaSettingsProvider(providerCfg, logger)
	require.NoError(t, err)

	go provider.Start(ctx, componenttest.NewNopHost())
	time.Sleep(3 * time.Second)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topicName,
		Balancer: &kafka.Hash{},
	}

	// The cleanup function handles tearing down all resources.
	cleanup := func() {
		writer.Close()
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
		cancel()
	}

	return ctx, provider, writer, cleanup
}

func newOtelComponentMappingSnapshot(t *testing.T, snapshotId, mappingId, mappingName string) []kafka.Message {
	t.Helper()

	// Start Message
	snapshotStartMessageKey, snapshotStartPayload := newSnapshotStartMessageKeyAndPayload(t, stsSettingsModel.SettingTypeOtelComponentMapping, snapshotId)

	// Envelope Message
	otelComponentMapping := stsSettingsModel.OtelComponentMapping{
		Id:               mappingId,
		Name:             mappingName,
		CreatedTimeStamp: time.Now().Unix(),
		Type:             stsSettingsModel.OtelComponentMappingTypeOtelComponentMapping,
		Shard:            0,
	}
	setting := stsSettingsModel.Setting{}
	err := setting.FromOtelComponentMapping(otelComponentMapping)
	require.NoError(t, err)

	settingsEnvelopeMessageKey, settingsEnvelopePayload := newSettingsEnvelopeMessageKeyAndPayload(t, stsSettingsModel.SettingTypeOtelComponentMapping, setting, otelComponentMapping.Id, snapshotId)

	// Stop Message
	snapshotSopMessageKey, snapshotStopPayload := newSnapshotStopMessageKeyAndPayload(t, stsSettingsModel.SettingTypeOtelComponentMapping, snapshotId)

	return []kafka.Message{
		{Key: []byte(snapshotStartMessageKey), Value: snapshotStartPayload},
		{Key: []byte(settingsEnvelopeMessageKey), Value: settingsEnvelopePayload},
		{Key: []byte(snapshotSopMessageKey), Value: snapshotStopPayload},
	}
}

func newOtelRelationMappingSnapshot(t *testing.T, snapshotId, mappingId, mappingName string) []kafka.Message {
	t.Helper()

	// Start Message
	snapshotStartMessageKey, snapshotStartPayload := newSnapshotStartMessageKeyAndPayload(t, stsSettingsModel.SettingTypeOtelRelationMapping, snapshotId)

	// Envelope Message
	otelRelationMapping := stsSettingsModel.OtelRelationMapping{
		Id:               mappingId,
		Name:             mappingName,
		CreatedTimeStamp: time.Now().Unix(),
		Type:             stsSettingsModel.OtelRelationMappingTypeOtelRelationMapping,
		Shard:            0,
	}
	setting := stsSettingsModel.Setting{}
	err := setting.FromOtelRelationMapping(otelRelationMapping)
	require.NoError(t, err)

	settingsEnvelopeMessageKey, settingsEnvelopePayload := newSettingsEnvelopeMessageKeyAndPayload(t, stsSettingsModel.SettingTypeOtelRelationMapping, setting, otelRelationMapping.Id, snapshotId)

	// Stop Message
	snapshotSopMessageKey, snapshotStopPayload := newSnapshotStopMessageKeyAndPayload(t, stsSettingsModel.SettingTypeOtelRelationMapping, snapshotId)

	return []kafka.Message{
		{Key: []byte(snapshotStartMessageKey), Value: snapshotStartPayload},
		{Key: []byte(settingsEnvelopeMessageKey), Value: settingsEnvelopePayload},
		{Key: []byte(snapshotSopMessageKey), Value: snapshotStopPayload},
	}
}

func newSnapshotStartMessageKey(settingType stsSettingsModel.SettingType) string {
	return fmt.Sprintf("%s:%s", settingType, stsSettingsModel.SettingsSnapshotStartTypeSettingsSnapshotStart)
}

func newSnapshotStartMessageKeyAndPayload(t *testing.T, settingType stsSettingsModel.SettingType, snapshotId string) (string, []byte) {
	settingsSnapshotStart := stsSettingsModel.SettingsSnapshotStart{
		Id:          snapshotId,
		SettingType: settingType,
	}

	settingsProtocol := stsSettingsModel.SettingsProtocol{}
	err := settingsProtocol.FromSettingsSnapshotStart(settingsSnapshotStart)
	require.NoError(t, err)

	snapshotStartPayload, err := settingsProtocol.MarshalJSON()

	return newSnapshotStartMessageKey(settingType), snapshotStartPayload
}

func newSnapshotStopMessageKey(settingType stsSettingsModel.SettingType) string {
	return fmt.Sprintf("%s:%s", settingType, stsSettingsModel.SettingsSnapshotStopTypeSettingsSnapshotStop)
}

func newSnapshotStopMessageKeyAndPayload(t *testing.T, settingType stsSettingsModel.SettingType, snapshotId string) (string, []byte) {
	settingsSnapshotStop := stsSettingsModel.SettingsSnapshotStop{
		Id: snapshotId,
	}

	settingsProtocol := stsSettingsModel.SettingsProtocol{}
	err := settingsProtocol.FromSettingsSnapshotStop(settingsSnapshotStop)
	require.NoError(t, err)

	snapshotStopPayload, err := settingsProtocol.MarshalJSON()

	return newSnapshotStopMessageKey(settingType), snapshotStopPayload
}

func newSettingsEnvelopeMessageKey(settingType stsSettingsModel.SettingType, settingId stsSettingsModel.SettingId) string {
	return fmt.Sprintf("%s:setting:%s", settingType, settingId)
}

func newSettingsEnvelopeMessageKeyAndPayload(t *testing.T, settingType stsSettingsModel.SettingType, setting stsSettingsModel.Setting, settingId stsSettingsModel.SettingId, snapshotId string) (string, []byte) {
	settingsEnvelope := stsSettingsModel.SettingsEnvelope{
		Id:      snapshotId,
		Setting: setting,
	}

	settingsProtocol := stsSettingsModel.SettingsProtocol{}
	err := settingsProtocol.FromSettingsEnvelope(settingsEnvelope)
	require.NoError(t, err)

	settingsEnvelopePayload, err := settingsProtocol.MarshalJSON()
	require.NoError(t, err)

	return newSettingsEnvelopeMessageKey(settingType, settingId), settingsEnvelopePayload
}

// createCompactedTopic is a helper to create a topic with the correct compaction policy
// using the segmentio/kafka-go client.
func createCompactedTopic(t *testing.T, brokerAddress, topicName string) {
	conn, err := kafka.Dial("tcp", brokerAddress)
	require.NoError(t, err)
	defer conn.Close()

	controller, err := conn.Controller()
	require.NoError(t, err)

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	require.NoError(t, err)
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topicName,
			NumPartitions:     1,
			ReplicationFactor: 1,
			// the below config must align with the topic configuration for the sts-internal-settings topic as per the
			// job-kafka-topic-create.sh script in the helm-charts repo.
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "cleanup.policy",
					ConfigValue: "compact",
				},
				{
					ConfigName:  "min.cleanable.dirty.ratio",
					ConfigValue: "0.1",
				},
			},
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	require.NoError(t, err)
}
