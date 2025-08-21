package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsCommon "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/common"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/config"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"sync"
	"time"

	"context"
	"errors"
	"go.opentelemetry.io/collector/component"
)

// A helper struct to hold the state of a snapshot being received.
type inProgressSnapshot struct {
	settingType stsSettingsModel.SettingType
	settings    []stsSettingsModel.Setting
}

type SettingsProvider struct {
	cfg    *stsSettingsConfig.KafkaSettingsProviderConfig
	logger *zap.Logger

	client      *kgo.Client
	adminClient *kadm.Client
	readTimeout time.Duration

	settingsCache *stsSettingsCommon.SettingsCache

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

	// Create Franz-go client options
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(consumerGroupID),
		kgo.ConsumeTopics(cfg.Topic),

		// Session and rebalancing configuration
		kgo.SessionTimeout(30 * time.Second),
		kgo.RebalanceTimeout(30 * time.Second),

		kgo.Balancers(kgo.CooperativeStickyBalancer()),

		// Enable auto-commit of processed records
		kgo.AutoCommitMarks(),

		// Fetch configuration
		kgo.FetchMaxBytes(int32(cfg.BufferSize)),
	}

	// Create the client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &SettingsProvider{
		cfg:                 cfg,
		logger:              logger,
		client:              client,
		adminClient:         kadm.NewClient(client),
		settingsCache:       stsSettingsCommon.NewSettingsCache(logger),
		inProgressSnapshots: make(map[string]*inProgressSnapshot),
		readTimeout:         30 * time.Second,
	}, nil
}

func (k *SettingsProvider) RegisterForUpdates(types ...stsSettingsModel.SettingType) <-chan stsSettingsEvents.UpdateSettingsEvent {
	return k.settingsCache.RegisterForUpdates(types...)
}

func (k *SettingsProvider) Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool {
	return k.settingsCache.Unregister(ch)
}

func (k *SettingsProvider) GetCurrentSettingsByType(settingType stsSettingsModel.SettingType) ([]any, error) {
	return k.settingsCache.GetConcreteSettingsByType(settingType)
}

func (k *SettingsProvider) Start(ctx context.Context, host component.Host) error {
	k.logger.Info("Starting Kafka settings provider",
		zap.Strings("brokers", k.cfg.Brokers),
		zap.String("topic", k.cfg.Topic))

	// Fail fast: check if topic exists
	if err := k.checkTopicExists(ctx); err != nil {
		return fmt.Errorf("failed to start kafka settings provider: %w", err)
	}

	// Setup cancellation
	readerCtx, readerCancel := context.WithCancel(ctx)
	k.readerCancelFunc = readerCancel

	k.ReaderCancelWg.Add(1)
	go func() {
		defer k.ReaderCancelWg.Done()
		if err := k.consumeMessages(readerCtx); err != nil && !errors.Is(err, context.Canceled) {
			k.logger.Error("Kafka reader failed", zap.Error(err))
		}
	}()

	return nil // started successfully
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
	case <-time.After(30 * time.Second):
		k.logger.Warn("Timeout waiting for Kafka reader to exit")
	}

	if k.settingsCache != nil {
		k.settingsCache.Shutdown()
	}

	k.client.Close()
	k.adminClient.Close()

	return nil
}

func (k *SettingsProvider) checkTopicExists(ctx context.Context) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, k.readTimeout)
	defer cancel()

	// Use admin client to check topic metadata
	topicDetails, err := k.adminClient.ListTopics(timeoutCtx, k.cfg.Topic)
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	if len(topicDetails) == 0 {
		return fmt.Errorf("topic %s not found", k.cfg.Topic)
	}

	for topic, detail := range topicDetails {
		if detail.Err != nil {
			return fmt.Errorf("topic %s error: %w", topic, detail.Err)
		}

		k.logger.Info("Topic found",
			zap.String("topic", topic),
			zap.Int32("partitions", int32(len(detail.Partitions))))
	}

	return nil
}

func (k *SettingsProvider) consumeMessages(ctx context.Context) error {
	k.logger.Info("Starting message consumption")

	for {
		select {
		case <-ctx.Done():
			k.logger.Info("Consumer context done, stopping consumption")
			return ctx.Err()
		default:
		}

		// Simple poll - franz-go handles retries and backoff internally
		fetches := k.client.PollFetches(ctx)

		// Check for context cancellation in fetch errors
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				if errors.Is(err.Err, context.Canceled) {
					return err.Err
				}
				k.logger.Error("Fetch error", zap.Error(err.Err))
			}
			continue // franz-go will handle backoff
		}

		// Process records
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, record := range p.Records {
				if err := k.processRecord(record); err != nil {
					k.logger.Error("Failed to process record", zap.Error(err))
					// Continue processing other records
				}
			}
		})
	}
}

func (k *SettingsProvider) processRecord(record *kgo.Record) error {
	message, err := k.unmarshalMessage(record.Value)
	if err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return k.handleMessage(message)
}

func (k *SettingsProvider) unmarshalMessage(data []byte) (*stsSettingsModel.SettingsProtocol, error) {
	var message stsSettingsModel.SettingsProtocol
	if err := json.Unmarshal(data, &message); err != nil {
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

	settingEntries := make([]stsSettingsCommon.SettingEntry, len(snapshot.settings))
	for i, s := range snapshot.settings {
		settingEntries[i] = stsSettingsCommon.NewSettingEntry(s)
	}

	k.settingsCache.UpdateSettingsForType(snapshot.settingType, settingEntries)

	return nil
}
