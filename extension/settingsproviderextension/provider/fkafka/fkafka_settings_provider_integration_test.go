//go:build integration

package fkafka_test

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap"
	"log"
	"testing"
	"time"

	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettings "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/config"
	stsSettingsKafka "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/provider/kafka"
	"github.com/stretchr/testify/require"
	testContainersKafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

const (
	kafkaImageName = "confluentinc/confluent-local:7.5.0"
	updateTimeout  = 10 * time.Second
)

type testContext struct {
	ctx       context.Context
	provider  *stsSettingsKafka.SettingsProvider
	client    *kgo.Client
	cleanup   func()
	topicName string
}

func TestKafkaSettingsProvider_InitialState(t *testing.T) {
	tc := setupTest(t)
	defer tc.cleanup()

	// Setup initial state
	componentMapping := publishComponentMapping(t, tc, "11111", "Host component V1")
	waitForSettingsUpdate[stsSettingsModel.OtelComponentMapping](t, tc, stsSettingsModel.SettingTypeOtelComponentMapping)
	relationMapping := publishRelationMapping(t, tc, "22222", "Runs on host")
	waitForSettingsUpdate[stsSettingsModel.OtelRelationMapping](t, tc, stsSettingsModel.SettingTypeOtelRelationMapping)

	otelComponentMappings, err := stsSettings.GetSettingsAs[stsSettingsModel.OtelComponentMapping](tc.provider, stsSettingsModel.SettingTypeOtelComponentMapping)
	require.NoError(t, err)
	otelRelationMappings, err := stsSettings.GetSettingsAs[stsSettingsModel.OtelRelationMapping](tc.provider, stsSettingsModel.SettingTypeOtelRelationMapping)
	require.NoError(t, err)
	assert.Equal(t, 2, len(otelComponentMappings)+len(otelRelationMappings), "Unexpected number of settings")
	assertComponentMapping(t, otelComponentMappings, componentMapping.id, componentMapping.name)
	assertRelationMapping(t, otelRelationMappings, relationMapping.id, relationMapping.name)
}

func TestKafkaSettingsProvider_Compaction(t *testing.T) {
	tc := setupTest(t)
	defer tc.cleanup()

	// Setup initial state
	publishComponentMapping(t, tc, "11111", "Host component V1")
	relationMapping := publishRelationMapping(t, tc, "22222", "Runs on host")
	// Wait for initial state to be processed
	waitForSettingsUpdate[stsSettingsModel.OtelComponentMapping](t, tc, stsSettingsModel.SettingTypeOtelComponentMapping)
	waitForSettingsUpdate[stsSettingsModel.OtelRelationMapping](t, tc, stsSettingsModel.SettingTypeOtelRelationMapping)

	// Update component mapping
	updatedMapping := publishComponentMapping(t, tc, "11111", "Host component V2")

	// Verify state after update
	otelComponentMappings := waitForSettingsUpdate[stsSettingsModel.OtelComponentMapping](t, tc, stsSettingsModel.SettingTypeOtelComponentMapping)
	assert.Len(t, otelComponentMappings, 1, "Unexpected number of settings")
	assertComponentMapping(t, otelComponentMappings, updatedMapping.id, updatedMapping.name)
	// GetConcreteSettings otel relation mappings again to check that they're the same
	otelRelationMappings, err := stsSettings.GetSettingsAs[stsSettingsModel.OtelRelationMapping](tc.provider, stsSettingsModel.SettingTypeOtelRelationMapping)
	require.NoError(t, err)
	assertRelationMapping(t, otelRelationMappings, relationMapping.id, relationMapping.name)
}

func TestKafkaSettingsProvider_Shutdown(t *testing.T) {
	tc := setupTest(t)
	defer tc.cleanup()

	err := tc.provider.Shutdown(tc.ctx)
	assert.NoError(t, err)

	done := make(chan struct{})
	go func() {
		tc.provider.ReaderCancelWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// the kafka reader in the provider should be stopped
	case <-time.After(5 * time.Second):
		t.Fatalf("Provider did not exit after being shut down")
	}
}

type mappingInfo struct {
	id   string
	name string
}

func publishComponentMapping(t *testing.T, tc *testContext, id, name string) mappingInfo {
	snapshotID := uuid.New().String()
	messages := newOtelComponentMappingSnapshot(t, snapshotID, id, name)
	produceMessages(t, tc.client, tc.topicName, messages)
	return mappingInfo{id: id, name: name}
}

func publishRelationMapping(t *testing.T, tc *testContext, id, name string) mappingInfo {
	snapshotID := uuid.New().String()
	messages := newOtelRelationMappingSnapshot(t, snapshotID, id, name)
	produceMessages(t, tc.client, tc.topicName, messages)
	return mappingInfo{id: id, name: name}
}

func produceMessages(t *testing.T, client *kgo.Client, topic string, messages []*kgo.Record) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Set the topic for all records
	for i := range messages {
		messages[i].Topic = topic
	}

	results := client.ProduceSync(ctx, messages...)
	require.NoError(t, results.FirstErr(), "failed to produce batch messages")

	// Log results
	successCount := 0
	for _, result := range results {
		if result.Err == nil {
			successCount++
		} else {
			t.Logf("Failed to produce message: %v", result.Err)
		}
	}
	t.Logf("Successfully produced %d/%d messages", successCount, len(messages))
}

func waitForSettingsUpdate[T any](t *testing.T, tc *testContext, settingType stsSettingsModel.SettingType) []T {
	t.Helper()

	// Subscribe only for the setting type we care about
	ch := tc.provider.RegisterForUpdates(settingType)
	defer tc.provider.Unregister(ch)

	select {
	case <-ch:
		as, err := stsSettings.GetSettingsAs[T](tc.provider, settingType)
		require.NoError(t, err)
		return as
	case <-time.After(updateTimeout):
		t.Fatalf("timed out waiting for settings update for type %v", settingType)
		return nil
	}
}

func assertComponentMapping(t *testing.T, settings []stsSettingsModel.OtelComponentMapping, id, expectedName string) {
	t.Helper()
	var mapping *stsSettingsModel.OtelComponentMapping
	for i := range settings {
		if settings[i].Id == id {
			mapping = &settings[i]
			break
		}
	}
	require.NotNil(t, mapping, "Component mapping should exist")
	assert.Equal(t, expectedName, mapping.Name)
}

func assertRelationMapping(t *testing.T, settings []stsSettingsModel.OtelRelationMapping, id, expectedName string) {
	t.Helper()
	var mapping *stsSettingsModel.OtelRelationMapping
	for i := range settings {
		if settings[i].Id == id {
			mapping = &settings[i]
			break
		}
	}
	require.NotNil(t, mapping, "Relation mapping should exist")
	assert.Equal(t, expectedName, mapping.Name)
}

func setupTest(t *testing.T) *testContext {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	kafkaContainer, err := testContainersKafka.Run(ctx, kafkaImageName)
	require.NoError(t, err, "failed to start container")

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	topicName := fmt.Sprintf("sts-internal-settings-%s", uuid.New().String())

	client := createProducerClient(t, brokers)
	createCompactedTopic(t, client, topicName)

	provider := createProvider(t, ctx, brokers, topicName)

	cleanup := func() {
		if err := provider.Shutdown(ctx); err != nil {
			log.Printf("failed to shutdown provider: %s", err)
		}
		client.Close()
		if err := kafkaContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
		cancel()
	}

	return &testContext{
		ctx:       ctx,
		provider:  provider,
		client:    client,
		cleanup:   cleanup,
		topicName: topicName,
	}
}

func createProvider(t *testing.T, ctx context.Context, brokers []string, topicName string) *stsSettingsKafka.SettingsProvider {
	providerCfg := &stsSettingsConfig.KafkaSettingsProviderConfig{
		Brokers:     brokers,
		Topic:       topicName,
		BufferSize:  1000,
		ReadTimeout: 60 * time.Second,
	}
	logger, _ := zap.NewDevelopment()
	provider, err := stsSettingsKafka.NewKafkaSettingsProvider(providerCfg, logger)
	require.NoError(t, err)

	go provider.Start(ctx, componenttest.NewNopHost())
	time.Sleep(3 * time.Second)

	return provider
}

func createProducerClient(t *testing.T, brokers []string) *kgo.Client {
	t.Helper()

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		// Producer-specific configurations
		kgo.RequiredAcks(kgo.AllISRAcks()), // Wait for all replicas (equivalent to acks=all)
		kgo.DisableIdempotentWrite(),       // Enable idempotent writes for exactly-once semantics
		kgo.ProducerBatchMaxBytes(16384),   // 16KB batches
		kgo.ProducerBatchCompression(kgo.GzipCompression()),
		kgo.ProducerLinger(5 * time.Millisecond), // Small linger for test responsiveness

		// Retry configuration
		kgo.RequestRetries(3),
		kgo.RetryBackoffFn(func(tries int) time.Duration {
			return time.Duration(tries) * 100 * time.Millisecond
		}),

		// Timeout configurations
		kgo.ProduceRequestTimeout(10 * time.Second),
		kgo.RequestTimeoutOverhead(2 * time.Second),
	}

	client, err := kgo.NewClient(opts...)
	require.NoError(t, err, "failed to create kafka producer client")

	return client
}

func createCompactedTopic(t *testing.T, client *kgo.Client, topicName string) {
	t.Helper()

	adminClient := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create topic with compaction enabled
	resp, err := adminClient.CreateTopics(ctx, 1, 1, map[string]*string{
		"cleanup.policy":            stringPtr("compact"),
		"segment.ms":                stringPtr("1000"), // 1 second for faster compaction in tests
		"min.cleanable.dirty.ratio": stringPtr("0.01"), // More aggressive compaction
	}, topicName)
	require.NoError(t, err, "failed to create topic")

	for topic, topicResponse := range resp {
		if topicResponse.Err != nil {
			t.Fatalf("failed to create topic %s: %v", topic, err)
		}
	}

	t.Logf("Created compacted topic: %s", topicName)
}

func stringPtr(s string) *string {
	return &s
}

func newOtelComponentMappingSnapshot(t *testing.T, snapshotId, mappingId, mappingName string) []*kgo.Record {
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

	return []*kgo.Record{
		{Key: []byte(snapshotStartMessageKey), Value: snapshotStartPayload},
		{Key: []byte(settingsEnvelopeMessageKey), Value: settingsEnvelopePayload},
		{Key: []byte(snapshotSopMessageKey), Value: snapshotStopPayload},
	}
}

func newOtelRelationMappingSnapshot(t *testing.T, snapshotId, mappingId, mappingName string) []*kgo.Record {
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

	return []*kgo.Record{
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
