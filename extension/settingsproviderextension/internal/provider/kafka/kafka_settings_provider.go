package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsConfig "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/config"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	stsSettingsCore "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal/core"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"sync"
	"time"

	"context"
	"errors"
	"go.opentelemetry.io/collector/component"
)

type SettingsProvider struct {
	cfg    *stsSettingsConfig.KafkaSettingsProviderConfig
	logger *zap.Logger

	client      *kgo.Client
	adminClient *kadm.Client
	readTimeout time.Duration

	settingsCache             stsSettingsCore.SettingsCache
	settingsSnapshotProcessor SettingsSnapshotProcessor

	// Mutex for concurrent access to inProgressSnapshots
	snapshotsLock sync.RWMutex
	// A map to store snapshots that are currently in the process of being received.
	// The key is the snapshot UUID (from the SnapshotStart/SettingsEnvelope/SnapshotStop's 'Id' field).
	inProgressSnapshots map[string]*InProgressSnapshot

	readerCancelFunc context.CancelFunc
	ReaderCancelWg   sync.WaitGroup
}

func NewKafkaSettingsProvider(ctx context.Context, cfg *stsSettingsConfig.KafkaSettingsProviderConfig, telemetrySettings component.TelemetrySettings, logger *zap.Logger) (*SettingsProvider, error) {
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

	settingsCache := stsSettingsCore.NewDefaultSettingsCache(logger)

	processor, err := NewDefaultSettingsSnapshotProcessor(ctx, logger, telemetrySettings, settingsCache)
	if err != nil {
		return nil, fmt.Errorf("failed to create settings snapshot processor: %w", err)
	}

	return &SettingsProvider{
		cfg:                       cfg,
		logger:                    logger,
		client:                    client,
		adminClient:               kadm.NewClient(client),
		settingsCache:             settingsCache,
		settingsSnapshotProcessor: processor,
		inProgressSnapshots:       make(map[string]*InProgressSnapshot),
		readTimeout:               30 * time.Second,
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
	settingsProtocol, err := k.unmarshalMessage(record.Value)
	if err != nil {
		return fmt.Errorf("failed to unmarshal settingsProtocol: %w", err)
	}

	return k.settingsSnapshotProcessor.ProcessSettingsProtocol(settingsProtocol)
}

func (k *SettingsProvider) unmarshalMessage(data []byte) (*stsSettingsModel.SettingsProtocol, error) {
	var message stsSettingsModel.SettingsProtocol
	if err := json.Unmarshal(data, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}
	return &message, nil
}
