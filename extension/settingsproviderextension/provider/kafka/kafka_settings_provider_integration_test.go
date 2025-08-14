//go:build integration

package kafka

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
	updateTimeout  = 10 * time.Second
	settleTime     = 2 * time.Second
)

type testContext struct {
	ctx      context.Context
	provider *kafkaSettingProvider
	writer   *kafka.Writer
	cleanup  func()
}

func TestKafkaSettingsProvider_InitialState(t *testing.T) {
	tc := setupTest(t)
	defer tc.cleanup()

	// Setup initial state
	componentMapping := publishComponentMapping(t, tc, "11111", "Host component V1")
	relationMapping := publishRelationMapping(t, tc, "22222", "Runs on host")

	// Verify initial state
	settings := waitForSettingsUpdate(t, tc)
	assertSettingsCount(t, settings, 2)
	assertComponentMapping(t, settings, componentMapping.id, componentMapping.name)
	assertRelationMapping(t, settings, relationMapping.id, relationMapping.name)
}

func TestKafkaSettingsProvider_Compaction(t *testing.T) {
	tc := setupTest(t)
	defer tc.cleanup()

	// Setup initial state
	publishComponentMapping(t, tc, "11111", "Host component V1")
	relationMapping := publishRelationMapping(t, tc, "22222", "Runs on host")
	waitForSettingsUpdate(t, tc) // Wait for initial state to be processed

	// Update component mapping
	updatedMapping := publishComponentMapping(t, tc, "11111", "Host component V2")

	// Verify state after update
	settings := waitForSettingsUpdate(t, tc)
	assertSettingsCount(t, settings, 2)
	assertComponentMapping(t, settings, updatedMapping.id, updatedMapping.name)
	assertRelationMapping(t, settings, relationMapping.id, relationMapping.name)
}

type mappingInfo struct {
	id   string
	name string
}

func publishComponentMapping(t *testing.T, tc *testContext, id, name string) mappingInfo {
	snapshotID := uuid.New().String()
	messages := newOtelComponentMappingSnapshot(t, snapshotID, id, name)
	require.NoError(t, tc.writer.WriteMessages(tc.ctx, messages...))
	return mappingInfo{id: id, name: name}
}

func publishRelationMapping(t *testing.T, tc *testContext, id, name string) mappingInfo {
	snapshotID := uuid.New().String()
	messages := newOtelRelationMappingSnapshot(t, snapshotID, id, name)
	require.NoError(t, tc.writer.WriteMessages(tc.ctx, messages...))
	return mappingInfo{id: id, name: name}
}

func waitForSettingsUpdate(t *testing.T, tc *testContext) map[stsSettingsModel.SettingId]stsSettingsModel.Setting {
	t.Helper()
	select {
	case <-tc.provider.RegisterForUpdates():
		time.Sleep(settleTime)
		return tc.provider.GetCurrentSettings()
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for settings update")
		return nil
	}
}

func assertSettingsCount(t *testing.T, settings map[stsSettingsModel.SettingId]stsSettingsModel.Setting, expected int) {
	t.Helper()
	assert.Len(t, settings, expected, "Unexpected number of settings")
}

func assertComponentMapping(t *testing.T, settings map[stsSettingsModel.SettingId]stsSettingsModel.Setting, id, expectedName string) {
	t.Helper()
	mapping, exists := settings[id]
	require.True(t, exists, "Component mapping should exist")

	compMapping, err := mapping.AsOtelComponentMapping()
	require.NoError(t, err)
	assert.Equal(t, expectedName, compMapping.Name)
}

func assertRelationMapping(t *testing.T, settings map[stsSettingsModel.SettingId]stsSettingsModel.Setting, id, expectedName string) {
	t.Helper()
	mapping, exists := settings[id]
	require.True(t, exists, "Relation mapping should exist")

	relMapping, err := mapping.AsOtelRelationMapping()
	require.NoError(t, err)
	assert.Equal(t, expectedName, relMapping.Name)
}

func setupTest(t *testing.T) *testContext {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	kafkaContainer, err := testContainersKafka.Run(ctx, kafkaImageName)
	require.NoError(t, err, "failed to start container")

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	topicName := fmt.Sprintf("sts-internal-settings-%s", uuid.New().String())
	createCompactedTopic(t, brokers[0], topicName)

	provider := createProvider(t, brokers, topicName)
	writer := createWriter(brokers, topicName)

	cleanup := func() {
		writer.Close()
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
		cancel()
	}

	return &testContext{
		ctx:      ctx,
		provider: provider,
		writer:   writer,
		cleanup:  cleanup,
	}
}

func createProvider(t *testing.T, brokers []string, topicName string) *kafkaSettingProvider {
	providerCfg := &stsSettingsConfig.KafkaSettingsProviderConfig{
		Brokers:    brokers,
		Topic:      topicName,
		BufferSize: 1000,
	}
	logger, _ := zap.NewDevelopment()
	provider, err := NewKafkaSettingsProvider(providerCfg, logger)
	require.NoError(t, err)

	go provider.Start(context.Background(), componenttest.NewNopHost())
	time.Sleep(3 * time.Second)

	return provider
}

func createWriter(brokers []string, topicName string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topicName,
		Balancer: &kafka.Hash{},
	}
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
