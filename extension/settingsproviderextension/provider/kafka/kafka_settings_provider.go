package kafka

import (
	"context"
	"encoding/json"
	"errors"
	stsProviderCommon "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/common"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/config"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/subscribers"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	"sync"
	"time"

	"fmt"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
)

type cachedSetting struct {
	raw                 stsSettingsModel.Setting
	concreteSettingType any // holds typed/concrete struct, lazy populated
}

// A helper struct to hold the state of a snapshot being received.
type inProgressSnapshot struct {
	settingType stsSettingsModel.SettingType
	settings    []stsSettingsModel.Setting
}

type SettingsProvider struct {
	cfg         *stsSettingsConfig.KafkaSettingsProviderConfig
	logger      *zap.Logger
	client      *kafka.Client
	reader      *kafka.Reader
	readTimeout time.Duration

	subscriberHub *subscribers.SubscriberHub

	// Mutex for concurrent access to settings
	settingsLock sync.RWMutex
	// A map where the key is the SettingType and the value is a slice of all currently active settings of that type.
	currentSettings map[stsSettingsModel.SettingType][]cachedSetting

	// Mutex for concurrent access to inProgressSnapshots
	snapshotsLock sync.RWMutex
	// A map to store snapshots that are currently in the process of being received.
	// The key is the snapshot UUID (from the SnapshotStart/SettingsEnvelope/SnapshotStop's 'Id' field).
	inProgressSnapshots map[string]*inProgressSnapshot

	readerCancelFunc context.CancelFunc
	ReaderCancelWg   sync.WaitGroup
}

func NewKafkaSettingsProvider(cfg *stsSettingsConfig.KafkaSettingsProviderConfig, logger *zap.Logger) (*SettingsProvider, error) {
	consumerGroupID := fmt.Sprintf("sts-otel-collector-internal-settings-%s", uuid.New().String())

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        consumerGroupID,
		MaxBytes:       cfg.BufferSize,
		MinBytes:       1,
		CommitInterval: time.Second,
		MaxWait:        100 * time.Millisecond,
		StartOffset:    kafka.FirstOffset,
	})

	return &SettingsProvider{
		cfg:                 cfg,
		logger:              logger,
		reader:              reader,
		client:              &kafka.Client{Addr: kafka.TCP(cfg.Brokers...)},
		subscriberHub:       subscribers.NewSubscriberHub(),
		currentSettings:     make(map[stsSettingsModel.SettingType][]cachedSetting),
		inProgressSnapshots: make(map[string]*inProgressSnapshot),
	}, nil
}

func (k *SettingsProvider) RegisterForUpdates(types ...stsSettingsModel.SettingType) <-chan stsSettingsEvents.UpdateSettingsEvent {
	return k.subscriberHub.Register(types...)
}

func (k *SettingsProvider) Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool {
	return k.subscriberHub.Unregister(ch)
}

func (k *SettingsProvider) GetCurrentSettingsByType(settingType stsSettingsModel.SettingType) (any, error) {
	k.settingsLock.RLock()
	defer k.settingsLock.RUnlock()

	cachedSetting, ok := k.currentSettings[settingType]
	if !ok {
		return nil, fmt.Errorf("no settings for type %s", settingType)
	}

	converterFor, ok := stsProviderCommon.ConverterFor(settingType)
	if !ok {
		return nil, fmt.Errorf("no converter registered for type %s", settingType)
	}

	// hydrate cache on demand
	out := make([]any, 0, len(cachedSetting))
	for i := range cachedSetting {
		if cachedSetting[i].concreteSettingType == nil {
			val, err := converterFor(cachedSetting[i].raw)
			if err != nil {
				return nil, err
			}
			cachedSetting[i].concreteSettingType = val
		}
		out = append(out, cachedSetting[i].concreteSettingType)
	}

	return out, nil
}

func (k *SettingsProvider) Start(ctx context.Context, host component.Host) error {
	k.logger.Info("Starting Kafka settings provider",
		zap.Strings("brokers", k.cfg.Brokers),
		zap.String("topic", k.cfg.Topic))

	// Fail fast if topic doesn't exist
	if err := k.checkTopicExists(ctx); err != nil {
		return fmt.Errorf("failed to start kafka settings provider: %w", err)
	}

	readerCtx, readerCancelFunc := context.WithCancel(ctx)
	k.readerCancelFunc = readerCancelFunc

	errChan := make(chan error, 1)
	k.ReaderCancelWg.Add(1)

	go func() {
		defer k.ReaderCancelWg.Done()
		if err := k.readMessages(readerCtx); err != nil && !errors.Is(err, context.Canceled) {
			select {
			case errChan <- err:
			default:
			}
		}
	}()

	// Wait a short time for startup errors
	select {
	case err := <-errChan:
		readerCancelFunc()
		return fmt.Errorf("failed to start message consumption: %w", err)
	case <-time.After(5 * time.Second):
		return nil // started successfully
	}
}

func (k *SettingsProvider) Shutdown(ctx context.Context) error {
	k.logger.Info("Shutting down Kafka settings provider")
	if k.readerCancelFunc != nil {
		k.readerCancelFunc()
	}

	done := make(chan struct{})
	go func() {
		k.ReaderCancelWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		k.subscriberHub.Shutdown()
	case <-time.After(30 * time.Second):
		k.logger.Warn("Timeout waiting for Kafka reader to exit")
	}

	if err := k.reader.Close(); err != nil {
		return fmt.Errorf("failed to close kafka reader: %w", err)
	}
	return nil
}

func (k *SettingsProvider) readMessages(ctx context.Context) error {
	backoff := time.Second
	maxBackoff := time.Minute

	for {
		select {
		case <-ctx.Done():
			k.logger.Info("Reader context done, stopping consumption")
			return ctx.Err()
		default:
			// Per-message timeout
			msgCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err := k.processNextMessage(msgCtx)
			cancel()

			if err != nil {
				if isShutdownError(err) || errors.Is(err, context.Canceled) {
					return err
				}

				if errors.Is(err, context.DeadlineExceeded) {
					k.logger.Warn("Message processing timed out", zap.Duration("backoff", backoff))
				} else {
					k.logger.Error("Failed to process message", zap.Error(err), zap.Duration("backoff", backoff))
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
					backoff = min(backoff*2, maxBackoff)
					continue
				}
			}
			backoff = time.Second
		}
	}
}

func (k *SettingsProvider) checkTopicExists(ctx context.Context) error {
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

func (k *SettingsProvider) processNextMessage(ctx context.Context) error {
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

func (k *SettingsProvider) unmarshalMessage(value []byte) (*stsSettingsModel.SettingsProtocol, error) {
	var message stsSettingsModel.SettingsProtocol
	if err := json.Unmarshal(value, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}
	return &message, nil
}

func (k *SettingsProvider) handleMessage(message *stsSettingsModel.SettingsProtocol) error {
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

func (k *SettingsProvider) handleSnapshotStart(msg stsSettingsModel.SettingsSnapshotStart) error {
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

func (k *SettingsProvider) handleSettingsEnvelope(msg stsSettingsModel.SettingsEnvelope) error {
	k.snapshotsLock.Lock()
	defer k.snapshotsLock.Unlock()

	snapshot, found := k.inProgressSnapshots[msg.Id]
	if !found {
		k.logger.Warn("Received an orphan settings envelope for a not in progress snapshot.",
			zap.String("snapshotId", msg.Id))
		// TODO: add a metric for this case
		return nil
	}

	snapshot.settings = append(snapshot.settings, msg.Setting)
	return nil
}

func (k *SettingsProvider) handleSnapshotStop(msg stsSettingsModel.SettingsSnapshotStop) error {
	k.snapshotsLock.Lock()
	snapshot, found := k.inProgressSnapshots[msg.Id]
	if !found {
		k.snapshotsLock.Unlock()
		k.logger.Warn("Received an orphan snapshot stop for an unknown snapshot.",
			zap.String("snapshotId", msg.Id))
		return nil
	}
	delete(k.inProgressSnapshots, msg.Id)
	k.snapshotsLock.Unlock()

	k.logger.Info("Received snapshot stop. Processing complete snapshot.",
		zap.String("snapshotId", msg.Id),
		zap.String("settingType", string(snapshot.settingType)),
		zap.Int("settingCount", len(snapshot.settings)))

	cachedSettings := make([]cachedSetting, len(snapshot.settings))
	for i, s := range snapshot.settings {
		cachedSettings[i] = cachedSetting{raw: s, concreteSettingType: nil}
	}

	k.settingsLock.Lock()
	k.currentSettings[snapshot.settingType] = cachedSettings
	k.settingsLock.Unlock()

	k.subscriberHub.Notify(stsSettingsEvents.UpdateSettingsEvent{
		Type: snapshot.settingType,
	})
	return nil
}

func isShutdownError(err error) bool {
	return errors.Is(err, context.Canceled) || err.Error() == "kafka: context canceled"
}
