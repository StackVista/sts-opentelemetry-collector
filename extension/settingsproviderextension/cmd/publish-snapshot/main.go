package main

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"os"
	"time"

	"github.com/google/uuid"

	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
)

const (
	topicName = "sts-internal-settings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <broker>\n", os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]

	snapshotId := uuid.New().String()
	mappingId := uuid.New().String()

	producer := createProducerClient([]string{broker})
	defer producer.Close()

	messages := newOtelComponentMappingSnapshot(snapshotId, mappingId, "host")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	produceMessages(ctx, producer, topicName, messages)
}

func createProducerClient(brokers []string) *kgo.Client {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),

		// Retry configuration
		kgo.RequestRetries(2),
		kgo.RetryBackoffFn(func(tries int) time.Duration {
			return time.Duration(tries) * 100 * time.Millisecond
		}),

		// Timeout configurations
		kgo.ProduceRequestTimeout(10 * time.Second),
		kgo.RequestTimeoutOverhead(2 * time.Second),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("failed to create kafka producer client: %v", err)
	}

	return client
}

func produceMessages(ctx context.Context, client *kgo.Client, topic string, messages []*kgo.Record) {
	// Set the topic for all records
	for i := range messages {
		messages[i].Topic = topic
	}

	results := client.ProduceSync(ctx, messages...)

	// Log results
	successCount := 0
	for _, result := range results {
		if result.Err == nil {
			successCount++
		} else {
			log.Printf("Failed to produce message: %v", result.Err)
		}
	}
	log.Printf("Successfully produced %d/%d messages", successCount, len(messages))
}

// This version doesn’t need *testing.T
func newOtelComponentMappingSnapshot(snapshotId, mappingId, mappingName string) []*kgo.Record {
	// Snapshot Start
	snapshotStartMessageKey, snapshotStartPayload := newSnapshotStartMessageKeyAndPayload(
		stsSettingsModel.SettingTypeOtelComponentMapping, snapshotId)

	// Envelope
	otelComponentMapping := stsSettingsModel.OtelComponentMapping{
		Id:               mappingId,
		Name:             mappingName,
		CreatedTimeStamp: time.Now().Unix(),
		Type:             stsSettingsModel.OtelComponentMappingTypeOtelComponentMapping,
		Shard:            0,
	}
	setting := stsSettingsModel.Setting{}
	if err := setting.FromOtelComponentMapping(otelComponentMapping); err != nil {
		log.Fatalf("failed to convert mapping to setting: %v", err)
	}
	settingsEnvelopeMessageKey, settingsEnvelopePayload := newSettingsEnvelopeMessageKeyAndPayload(
		stsSettingsModel.SettingTypeOtelComponentMapping, setting, otelComponentMapping.Id, snapshotId)

	// Snapshot Stop
	snapshotStopMessageKey, snapshotStopPayload := newSnapshotStopMessageKeyAndPayload(
		stsSettingsModel.SettingTypeOtelComponentMapping, snapshotId)

	return []*kgo.Record{
		{Key: []byte(snapshotStartMessageKey), Value: snapshotStartPayload},
		{Key: []byte(settingsEnvelopeMessageKey), Value: settingsEnvelopePayload},
		{Key: []byte(snapshotStopMessageKey), Value: snapshotStopPayload},
	}
}

func newSnapshotStartMessageKey(settingType stsSettingsModel.SettingType) string {
	return fmt.Sprintf("%s:%s", settingType, stsSettingsModel.SettingsSnapshotStartTypeSettingsSnapshotStart)
}

func newSnapshotStartMessageKeyAndPayload(settingType stsSettingsModel.SettingType, snapshotId string) (string, []byte) {
	settingsSnapshotStart := stsSettingsModel.SettingsSnapshotStart{
		Id:          snapshotId,
		SettingType: settingType,
	}

	settingsProtocol := stsSettingsModel.SettingsProtocol{}
	err := settingsProtocol.FromSettingsSnapshotStart(settingsSnapshotStart)
	if err != nil {
		log.Fatalf("failed to convert snapshot start to protocol: %v", err)
	}

	snapshotStartPayload, err := settingsProtocol.MarshalJSON()
	if err != nil {
		log.Fatalf("failed to marshal protocol: %v", err)
	}

	return newSnapshotStartMessageKey(settingType), snapshotStartPayload
}

func newSnapshotStopMessageKey(settingType stsSettingsModel.SettingType) string {
	return fmt.Sprintf("%s:%s", settingType, stsSettingsModel.SettingsSnapshotStopTypeSettingsSnapshotStop)
}

func newSnapshotStopMessageKeyAndPayload(settingType stsSettingsModel.SettingType, snapshotId string) (string, []byte) {
	settingsSnapshotStop := stsSettingsModel.SettingsSnapshotStop{
		Id: snapshotId,
	}

	settingsProtocol := stsSettingsModel.SettingsProtocol{}
	err := settingsProtocol.FromSettingsSnapshotStop(settingsSnapshotStop)
	if err != nil {
		log.Fatalf("failed to convert snapshot stop to protocol: %v", err)
	}

	snapshotStopPayload, err := settingsProtocol.MarshalJSON()
	if err != nil {
		log.Fatalf("failed to marshal protocol: %v", err)
	}

	return newSnapshotStopMessageKey(settingType), snapshotStopPayload
}

func newSettingsEnvelopeMessageKey(settingType stsSettingsModel.SettingType, settingId stsSettingsModel.SettingId) string {
	return fmt.Sprintf("%s:setting:%s", settingType, settingId)
}

func newSettingsEnvelopeMessageKeyAndPayload(settingType stsSettingsModel.SettingType, setting stsSettingsModel.Setting, settingId stsSettingsModel.SettingId, snapshotId string) (string, []byte) {
	settingsEnvelope := stsSettingsModel.SettingsEnvelope{
		Id:      snapshotId,
		Setting: setting,
	}

	settingsProtocol := stsSettingsModel.SettingsProtocol{}
	err := settingsProtocol.FromSettingsEnvelope(settingsEnvelope)
	if err != nil {
		log.Fatalf("failed to convert settings envelope to protocol: %v", err)
	}

	settingsEnvelopePayload, err := settingsProtocol.MarshalJSON()
	if err != nil {
		log.Fatalf("failed to marshal protocol: %v", err)
	}

	return newSettingsEnvelopeMessageKey(settingType, settingId), settingsEnvelopePayload
}
