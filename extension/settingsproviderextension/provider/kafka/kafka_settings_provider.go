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
	cfg         *stsSettingsConfig.KafkaSettingsProviderConfig
	logger      *zap.Logger
	client      *kafka.Client
	reader      *kafka.Reader
	readTimeout time.Duration

	// Mutex for concurrent access to settings
	settingsLock    sync.RWMutex
	currentSettings map[stsSettingsModel.SettingId]stsSettingsModel.Setting

	// Channel to signal updates
	updateChannel chan struct{}

	// Mutex for concurrent access to settings
	snapshotsLock sync.RWMutex
	// A map to store snapshots that are currently in the process of being received.
	// The key is the snapshot UUID (from the SnapshotStart/SettingsEnvelope/SnapshotStop's 'Id' field).
	inProgressSnapshots map[string]*inProgressSnapshot

	cleanup func()
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

func (k *kafkaSettingProvider) Start(ctx context.Context, host component.Host) error {
	k.logger.Info("Starting Kafka settings provider.",
		zap.Strings("brokers", k.cfg.Brokers),
		zap.String("topic", k.cfg.Topic))

	// Check initial connection and topic existence
	if err := k.checkTopicExists(ctx); err != nil {
		return fmt.Errorf("failed to start kafka provider: %w", err)
	}

	errChan := make(chan error, 1)

	// Create parent context with timeout for the reader
	readerCtx, cancelReader := context.WithTimeout(ctx, k.cfg.ReadTimeout)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer cancelReader()

		if err := k.readMessages(readerCtx); err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				select {
				case errChan <- err:
				default:
				}
			}
			k.logger.Error("Kafka reader stopped",
				zap.Error(err),
				zap.Bool("timeout", errors.Is(err, context.DeadlineExceeded)))
		}
	}()

	// Wait for potential initial errors
	select {
	case err := <-errChan:
		return fmt.Errorf("failed to start message consumption: %w", err)
	case <-time.After(5 * time.Second):
		// Successfully started
	}

	k.cleanup = func() {
		cancelReader()

		// Wait for goroutine to finish with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			k.logger.Info("Kafka reader goroutine cleaned up successfully")
		case <-time.After(30 * time.Second):
			k.logger.Warn("Timeout waiting for Kafka reader goroutine to finish")
		}

		if err := k.reader.Close(); err != nil {
			k.logger.Error("Failed to close kafka reader", zap.Error(err))
		}
	}

	return nil
}

func (k *kafkaSettingProvider) Shutdown(ctx context.Context) error {
	k.logger.Info("Shutting down Kafka settings provider.")

	// Create a timeout context for shutdown
	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Call cleanup function that was set in Start()
	if k.cleanup != nil {
		k.cleanup()
	}

	// Attempt to close the reader with timeout
	errChan := make(chan error, 1)
	go func() {
		errChan <- k.reader.Close()
	}()

	select {
	case err := <-errChan:
		if err != nil {
			return fmt.Errorf("failed to close kafka reader: %w", err)
		}
		return nil
	case <-shutdownCtx.Done():
		return fmt.Errorf("shutdown timed out: %w", shutdownCtx.Err())
	}
}

func (k *kafkaSettingProvider) readMessages(ctx context.Context) error {
	// Check topic existence with timeout
	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := k.checkTopicExists(checkCtx); err != nil {
		return fmt.Errorf("kafka topic unavailable: %w", err)
	}

	backoff := time.Second
	maxBackoff := time.Minute

	for {
		select {
		case <-ctx.Done():
			k.logger.Info("Kafka reader context done, stopping message consumption.")
			return ctx.Err()

		default:
			// Create timeout context for each message processing attempt
			msgCtx, cancelMsg := context.WithTimeout(ctx, 30*time.Second)
			err := k.processNextMessage(msgCtx)
			cancelMsg()

			if err != nil {
				if isShutdownError(err) || errors.Is(err, context.Canceled) {
					return err
				}

				if errors.Is(err, context.DeadlineExceeded) {
					k.logger.Warn("Message processing timed out, retrying",
						zap.Duration("backoff", backoff))
				} else {
					k.logger.Error("Failed to process message",
						zap.Error(err),
						zap.Duration("backoff", backoff))
				}

				// Apply backoff with context awareness
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
					backoff = min(backoff*2, maxBackoff)
					continue
				}
			}

			// Reset backoff on successful processing
			backoff = time.Second
		}
	}
}

func (k *kafkaSettingProvider) checkTopicExists(ctx context.Context) error {
	metadata, err := k.client.Metadata(ctx, &kafka.MetadataRequest{
		Topics: []string{k.cfg.Topic},
	})
	if err != nil {
		return fmt.Errorf("failed to get topic metadata: %w", err)
	}

	// Additional check for topic existence in metadata
	if len(metadata.Topics) == 0 {
		return fmt.Errorf("topic %s not found", k.cfg.Topic)
	}

	for _, topic := range metadata.Topics {
		if topic.Error != nil {
			return fmt.Errorf("topic %s error: %w", k.cfg.Topic, topic.Error)
		}
	}

	return nil
}

func (k *kafkaSettingProvider) processNextMessage(ctx context.Context) error {
	msg, err := k.reader.ReadMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to read message: %w", err)
	}

	message, err := k.unmarshalMessage(msg.Value)
	if err != nil {
		return err
	}

	return k.handleMessage(message)
}

func (k *kafkaSettingProvider) unmarshalMessage(value []byte) (*stsSettingsModel.SettingsProtocol, error) {
	var message stsSettingsModel.SettingsProtocol
	if err := json.Unmarshal(value, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}
	return &message, nil
}

func (k *kafkaSettingProvider) handleMessage(message *stsSettingsModel.SettingsProtocol) error {
	actualMessage, err := message.ValueByDiscriminator()
	if err != nil {
		return fmt.Errorf("error getting message by discriminator: %w", err)
	}

	switch v := actualMessage.(type) {
	case stsSettingsModel.SettingsSnapshotStart:
		return k.handleSnapshotStart(v)
	case stsSettingsModel.SettingsEnvelope:
		return k.handleSettingsEnvelope(v)
	case stsSettingsModel.SettingsSnapshotStop:
		return k.handleSnapshotStop(v)
	default:
		return fmt.Errorf("unknown message type: %T", actualMessage)
	}
}

func (k *kafkaSettingProvider) handleSnapshotStart(msg stsSettingsModel.SettingsSnapshotStart) error {
	k.logger.Info("Received snapshot start.",
		zap.String("snapshotId", msg.Id),
		zap.String("settingType", string(msg.SettingType)))

	k.snapshotsLock.Lock()
	defer k.snapshotsLock.Unlock()

	k.inProgressSnapshots[msg.Id] = &inProgressSnapshot{
		settingType: msg.SettingType,
		settings:    make([]stsSettingsModel.Setting, 0),
	}
	return nil
}

func (k *kafkaSettingProvider) handleSettingsEnvelope(msg stsSettingsModel.SettingsEnvelope) error {
	k.snapshotsLock.Lock()
	defer k.snapshotsLock.Unlock()

	snapshot, found := k.inProgressSnapshots[msg.Id]
	if !found {
		k.logger.Warn("Received an orphan settings envelope for an unknown snapshot.",
			zap.String("snapshotId", msg.Id))
		// TODO: add a metric for this case
		return nil
	}

	snapshot.settings = append(snapshot.settings, msg.Setting)
	return nil
}

func (k *kafkaSettingProvider) handleSnapshotStop(msg stsSettingsModel.SettingsSnapshotStop) error {
	k.snapshotsLock.Lock()
	snapshot, found := k.inProgressSnapshots[msg.Id]
	if !found {
		k.snapshotsLock.Unlock()
		k.logger.Warn("Received an orphan snapshot stop for an unknown snapshot.",
			zap.String("snapshotId", msg.Id))
		return nil
	}

	// Make a local snapshotCopy so we can unlock before processing
	snapshotCopy := *snapshot
	delete(k.inProgressSnapshots, msg.Id)
	k.snapshotsLock.Unlock()

	k.logger.Info("Received snapshot stop. Processing complete snapshot.",
		zap.String("snapshotId", msg.Id),
		zap.String("settingType", string(snapshotCopy.settingType)),
		zap.Int("settingCount", len(snapshotCopy.settings)))

	if err := k.updateSettings(&snapshotCopy); err != nil {
		return fmt.Errorf("failed to update settings: %w", err)
	}

	k.notifyUpdate()
	return nil
}

func (k *kafkaSettingProvider) updateSettings(snapshot *inProgressSnapshot) error {
	newSettings, err := k.processSnapshotSettings(snapshot.settings)
	if err != nil {
		return err
	}

	k.settingsLock.Lock()
	defer k.settingsLock.Unlock()

	mergedSettings := k.mergeSettings(snapshot.settingType, newSettings)
	k.currentSettings = mergedSettings
	return nil
}

func (k *kafkaSettingProvider) processSnapshotSettings(settings []stsSettingsModel.Setting) (map[stsSettingsModel.SettingId]stsSettingsModel.Setting, error) {
	newSettings := make(map[stsSettingsModel.SettingId]stsSettingsModel.Setting)
	for _, setting := range settings {
		settingId, err := stsProviderCommon.GetSettingId(setting)
		if err != nil {
			return nil, fmt.Errorf("failed to get setting id: %w", err)
		}
		newSettings[settingId] = setting
	}
	return newSettings, nil
}

func (k *kafkaSettingProvider) mergeSettings(settingType stsSettingsModel.SettingType, newSettings map[stsSettingsModel.SettingId]stsSettingsModel.Setting) map[stsSettingsModel.SettingId]stsSettingsModel.Setting {
	mergedSettings := make(map[stsSettingsModel.SettingId]stsSettingsModel.Setting)

	// Copy settings of different types
	for id, setting := range k.currentSettings {
		if setting.Type != string(settingType) {
			mergedSettings[id] = setting
		}
	}

	// Add new settings
	for id, setting := range newSettings {
		mergedSettings[id] = setting
	}

	return mergedSettings
}

func (k *kafkaSettingProvider) notifyUpdate() {
	select {
	case k.updateChannel <- struct{}{}:
	default:
		// Channel is full, clients are still processing the last update
	}
}

func isShutdownError(err error) bool {
	return errors.Is(err, context.Canceled) || err.Error() == "kafka: context canceled"
}
