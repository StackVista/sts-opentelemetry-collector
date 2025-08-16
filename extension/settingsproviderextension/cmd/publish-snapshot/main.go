package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

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

	// Use fixed IDs or generate new ones for each run
	snapshotId := uuid.New().String()
	mappingId := uuid.New().String()

	writer := createWriter([]string{broker}, topicName)
	defer writer.Close()

	messages := newOtelComponentMappingSnapshot(snapshotId, mappingId, "host")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := writer.WriteMessages(ctx, messages...); err != nil {
		log.Fatalf("failed to write messages: %v", err)
	}

	log.Printf("Successfully published %d messages to %s", len(messages), topicName)
}

func createWriter(brokers []string, topicName string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topicName,
		Balancer: &kafka.Hash{},
	}
}

// This version doesn’t need *testing.T
func newOtelComponentMappingSnapshot(snapshotId, mappingId, mappingName string) []kafka.Message {
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

	return []kafka.Message{
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
