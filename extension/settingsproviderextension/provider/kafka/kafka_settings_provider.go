package kafka

import (
	"context"
	"encoding/json"
	"errors"
	stsProviderCommon "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/provider/common"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	"sync"
	"time"

	"fmt"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal"
)

// A helper struct to hold the state of a snapshot being received.
type inProgressSnapshot struct {
	settingType stsSettingsModel.SettingType
	settings    []stsSettingsModel.Setting
}

type kafkaSettingProvider struct {
	cfg    *stsSettingsConfig.KafkaSettingsProviderConfig
	logger *zap.Logger
	client *kafka.Client
	conn   *kafka.Conn
	reader *kafka.Reader

	// Mutex for concurrent access to settings
	settingsLock    sync.RWMutex
	currentSettings map[stsSettingsModel.SettingId]stsSettingsModel.Setting

	// Channel to signal updates
	updateChannel chan struct{}

	// A map to store snapshots that are currently in the process of being received.
	// The key is the snapshot UUID (from the message 'id' field).
	inProgressSnapshots map[string]*inProgressSnapshot
}

func NewKafkaSettingsProvider(cfg *stsSettingsConfig.KafkaSettingsProviderConfig, logger *zap.Logger) (*kafkaSettingProvider, error) {
	provider := &kafkaSettingProvider{
		cfg:                 cfg,
		logger:              logger,
		currentSettings:     make(map[stsSettingsModel.SettingId]stsSettingsModel.Setting),
		updateChannel:       make(chan struct{}, 1),
		inProgressSnapshots: make(map[string]*inProgressSnapshot),
	}

	// Generate a random consumer group ID to ensure full topic processing.
	consumerGroupID := fmt.Sprintf("sts-otel-collector-internal-settings-%s", uuid.New().String())

	provider.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        consumerGroupID,
		MaxBytes:       cfg.BufferSize,
		MinBytes:       1,
		CommitInterval: 1 * time.Second,
		MaxWait:        100 * time.Millisecond,
		StartOffset:    kafka.FirstOffset,
	})

	provider.client = &kafka.Client{
		Addr: kafka.TCP(cfg.Brokers...),
	}

	return provider, nil
}

func (k *kafkaSettingProvider) Start(ctx context.Context, host component.Host) error {
	k.logger.Info("Starting Kafka settings provider.", zap.Strings("brokers", k.cfg.Brokers), zap.String("topic", k.cfg.Topic))

	go k.readMessages(ctx)

	return nil
}

func (k *kafkaSettingProvider) Shutdown(ctx context.Context) error {
	k.logger.Info("Shutting down Kafka settings provider.")

	// Close the reader.
	return k.reader.Close()
}

func (k *kafkaSettingProvider) readMessages(ctx context.Context) {
	// A simple check to see if the topic exists.
	// You can implement more robust checks if needed.
	_, err := k.client.Metadata(ctx, &kafka.MetadataRequest{
		Topics: []string{k.cfg.Topic},
	})
	if err != nil {
		k.logger.Warn("Kafka topic might not exist or is unavailable.", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			k.logger.Info("Kafka reader context done, stopping message consumption.")
			return
		default:
			msg, err := k.reader.ReadMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || err.Error() == "kafka: context canceled" {
					return // Graceful shutdown
				}
				k.logger.Error("Failed to read message from Kafka.", zap.Error(err))
				// Implement backoff or retry logic here
				time.Sleep(1 * time.Second)
				continue
			}

			// Process the message.
			var message stsSettingsModel.SettingsProtocol
			if err := json.Unmarshal(msg.Value, &message); err != nil {
				k.logger.Error("Failed to unmarshal Kafka message.", zap.Error(err))
				continue
			}

			actualMessage, err := message.ValueByDiscriminator()
			if err != nil {
				k.logger.Error("Error getting settings protocol message by discriminator.", zap.Error(err))
				continue
			}

			// Use a type switch to handle the specific message type.
			switch v := actualMessage.(type) {

			case stsSettingsModel.SettingsSnapshotStart:
				k.logger.Info("Received snapshot start.", zap.String("snapshotId", v.Id), zap.String("settingType", string(v.SettingType)))
				k.inProgressSnapshots[v.Id] = &inProgressSnapshot{
					settingType: v.SettingType,
					settings:    make([]stsSettingsModel.Setting, 0),
				}

			case stsSettingsModel.SettingsEnvelope:
				if snapshot, found := k.inProgressSnapshots[v.Id]; found {
					snapshot.settings = append(snapshot.settings, v.Setting)
				} else {
					k.logger.Warn("Received an orphan settings envelope for an unknown snapshot.", zap.String("snapshotId", v.Id))
				}

			case stsSettingsModel.SettingsSnapshotStop:
				if snapshot, found := k.inProgressSnapshots[v.Id]; found {
					k.logger.Info(
						"Received snapshot stop. Processing complete snapshot.",
						zap.String("snapshotId", v.Id),
						zap.String("settingType", string(k.inProgressSnapshots[v.Id].settingType)),
						zap.Int("settingCount", len(snapshot.settings)))

					newSettings := make(map[stsSettingsModel.SettingId]stsSettingsModel.Setting)
					for _, setting := range snapshot.settings {
						settingId, err := stsProviderCommon.GetSettingId(setting)
						if err != nil {
							k.logger.Error("Failed to get setting id.", zap.Error(err))
							continue
						}
						newSettings[settingId] = setting
					}

					k.settingsLock.Lock()
					mergedSettings := make(map[stsSettingsModel.SettingId]stsSettingsModel.Setting)

					// Copy all settings from the old map that are NOT of the setting type contained in the new snapshot.
					for id, setting := range k.currentSettings {
						if setting.Type != string(snapshot.settingType) {
							mergedSettings[id] = setting
						}
					}

					// Add all the new settings from the processed snapshot. This effectively replaces all settings of the given type.
					for id, setting := range newSettings {
						mergedSettings[id] = setting
					}

					// Atomically swap the old map with the newly merged one.
					k.currentSettings = mergedSettings

					k.settingsLock.Unlock()

					delete(k.inProgressSnapshots, v.Id)

					select {
					case k.updateChannel <- struct{}{}:
					default:
						// Channel is full, meaning clients are still processing the last update.
						// This decouples the consumption rate from the update rate.
					}
				} else {
					k.logger.Warn("Received an orphan snapshot stop for an unknown snapshot.", zap.String("snapshotId", v.Id))
				}
			}
		}
	}
}

func (k *kafkaSettingProvider) RegisterForUpdates() <-chan struct{} {
	return k.updateChannel
}

func (k *kafkaSettingProvider) GetCurrentSettings() map[stsSettingsModel.SettingId]stsSettingsModel.Setting {
	k.settingsLock.RLock()
	defer k.settingsLock.RUnlock()

	// Return a copy to prevent external modification.
	copiedSettings := make(map[stsSettingsModel.SettingId]stsSettingsModel.Setting, len(k.currentSettings))
	for k, v := range k.currentSettings {
		copiedSettings[k] = v
	}
	return copiedSettings
}
