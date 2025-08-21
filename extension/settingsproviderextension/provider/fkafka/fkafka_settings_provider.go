package fkafka

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
	readerCancelWg   sync.WaitGroup
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
		kgo.HeartbeatInterval(3 * time.Second),

		// Use cooperative-sticky balancer for better rebalancing
		kgo.Balancers(kgo.CooperativeStickyBalancer()),

		// Manual commit control for better reliability
		kgo.DisableAutoCommit(),

		// Fetch configuration equivalent to your original settings
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(int32(cfg.BufferSize)), // Use your configured buffer size
		kgo.FetchMaxWait(100 * time.Millisecond), // Match your original MaxWait

		// Start from the beginning of the topic (equivalent to FirstOffset)
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),

		// Enable partition discovery for dynamic topic changes
		kgo.MetadataMaxAge(5 * time.Minute),
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
		readTimeout:         30 * time.Second, // Default read timeout
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

	k.readerCancelWg.Add(1)
	go func() {
		defer k.readerCancelWg.Done()
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
		k.readerCancelWg.Wait()
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

//func (k *SettingsProvider) Start(ctx context.Context, host component.Host) error {
//	k.logger.Info("Starting Franz-go Kafka settings provider",
//		zap.Strings("brokers", k.cfg.Brokers),
//		zap.String("topic", k.cfg.Topic))
//
//	// Check if topic exists before starting consumption
//	if err := k.checkTopicExists(ctx); err != nil {
//		k.client.Close()
//		return fmt.Errorf("failed to start kafka settings provider: %w", err)
//	}
//
//	// Start consuming in a separate goroutine
//	consumerCtx, readerCancelFunc := context.WithCancel(ctx)
//	k.readerCancelFunc = readerCancelFunc
//
//	errChan := make(chan error, 1)
//	k.readerCancelWg.Add(1)
//
//	go func() {
//		defer k.readerCancelWg.Done()
//		if err := k.consumeMessages(consumerCtx); err != nil && !errors.Is(err, context.Canceled) {
//			select {
//			case errChan <- err:
//			default:
//			}
//		}
//	}()
//
//	// Wait for startup errors or successful initialization
//	select {
//	case err := <-errChan:
//		readerCancelFunc()
//		k.client.Close()
//		return fmt.Errorf("failed to start message consumption: %w", err)
//	case <-time.After(5 * time.Second):
//		k.logger.Info("Kafka settings provider started successfully")
//		return nil
//	}
//}

func (k *SettingsProvider) consumeMessages(ctx context.Context) error {
	k.logger.Info("Starting message consumption")

	backoff := time.Second
	maxBackoff := time.Minute

	for {
		select {
		case <-ctx.Done():
			k.logger.Info("Consumer context done, stopping consumption")
			return ctx.Err()
		default:
		}

		// Poll for records with context
		fetches := k.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			// Handle fetch errors with backoff similar to your original implementation
			hasNonCancelErrors := false
			for _, err := range errs {
				if errors.Is(err.Err, context.Canceled) {
					return err.Err
				}
				k.logger.Error("Fetch error",
					zap.String("topic", err.Topic),
					zap.Int32("partition", err.Partition),
					zap.Error(err.Err))
				hasNonCancelErrors = true
			}

			if hasNonCancelErrors {
				k.logger.Warn("Backing off due to fetch errors", zap.Duration("backoff", backoff))
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
					backoff = min(backoff*2, maxBackoff)
					continue
				}
			}
			continue
		}

		// Reset backoff on successful fetch
		backoff = time.Second

		// Process each record
		var processErr error
		recordsProcessed := 0

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, record := range p.Records {
				if err := k.processRecord(ctx, record); err != nil {
					k.logger.Error("Failed to process record",
						zap.String("topic", record.Topic),
						zap.Int32("partition", record.Partition),
						zap.Int64("offset", record.Offset),
						zap.Error(err))
					processErr = err
					return
				}
				recordsProcessed++
			}
		})

		if recordsProcessed > 0 {
			k.logger.Debug("Processed records", zap.Int("count", recordsProcessed))
		}

		// Handle processing errors similar to your original backoff logic
		if processErr != nil {
			if isShutdownError(processErr) || errors.Is(processErr, context.Canceled) {
				return processErr
			}

			k.logger.Error("Failed to process messages", zap.Error(processErr), zap.Duration("backoff", backoff))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
				continue
			}
		}

		// Commit processed messages (equivalent to your CommitInterval behavior)
		if recordsProcessed > 0 {
			if err := k.client.CommitUncommittedOffsets(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				k.logger.Error("Failed to commit offsets", zap.Error(err))
			}
		}
	}
}

func (k *SettingsProvider) processRecord(ctx context.Context, record *kgo.Record) error {
	// Create a timeout context for processing individual messages
	msgCtx, cancel := context.WithTimeout(ctx, k.readTimeout)
	defer cancel()

	message, err := k.unmarshalMessage(record.Value)
	if err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	if err := k.handleMessageWithContext(msgCtx, message); err != nil {
		return fmt.Errorf("failed to handle message: %w", err)
	}

	k.logger.Debug("Successfully processed message",
		zap.String("topic", record.Topic),
		zap.Int32("partition", record.Partition),
		zap.Int64("offset", record.Offset))

	return nil
}

func (k *SettingsProvider) checkTopicExists(ctx context.Context) error {
	// Use admin client to check topic metadata
	topicDetails, err := k.adminClient.ListTopics(ctx, k.cfg.Topic)
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

// Your existing message handling methods - these remain mostly unchanged
func (k *SettingsProvider) unmarshalMessage(data []byte) (*stsSettingsModel.SettingsProtocol, error) {
	var message stsSettingsModel.SettingsProtocol
	if err := json.Unmarshal(data, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}
	return &message, nil
}

func (k *SettingsProvider) handleMessageWithContext(ctx context.Context, message *stsSettingsModel.SettingsProtocol) error {
	// Implement your message handling logic here with context support
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return k.handleMessage(message)
	}
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

// Utility function to check for shutdown errors
func isShutdownError(err error) bool {
	// Add your logic to detect shutdown-related errors
	// This might include specific Kafka errors that indicate the consumer is shutting down
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
