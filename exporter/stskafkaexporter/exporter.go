package stskafkaexporter

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

const KafkaMessageKey = "stskafka.key"

// InternalExporterComponent is the interface that both production and stub exporters implement.
type InternalExporterComponent interface {
	ExportData(ctx context.Context, ld plog.Logs) error
	// export functions for other data types go here

	Start(ctx context.Context, host component.Host) error
	Shutdown(ctx context.Context) error
}

type KafkaExporter struct {
	cfg         *Config
	logger      *zap.Logger
	client      *kgo.Client
	adminClient *kadm.Client
}

func NewKafkaExporter(cfg Config, set exporter.CreateSettings) (*KafkaExporter, error) {
	clientID := fmt.Sprintf("stskafkaexporter-%s", uuid.New().String())
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(clientID),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	}
	opts = append(opts, requiredAcksFromConfig(cfg.RequiredAcks)...)

	set.Logger.Info("Creating Kafka exporter", zap.String("clientID", clientID))
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed creating franz-go (kafka) client: %w", err)
	}

	return &KafkaExporter{
		cfg:         &cfg,
		logger:      set.Logger,
		client:      client,
		adminClient: kadm.NewClient(client),
	}, nil
}

func requiredAcksFromConfig(val string) []kgo.Opt {
	switch val {
	case "none":
		return []kgo.Opt{
			kgo.RequiredAcks(kgo.NoAck()),
			kgo.DisableIdempotentWrite(),
		}
	case "leader":
		return []kgo.Opt{
			kgo.RequiredAcks(kgo.LeaderAck()),
			kgo.DisableIdempotentWrite(),
		}
	case "all":
		// Idempotency requires acks=all, so we don’t disable it here
		return []kgo.Opt{
			kgo.RequiredAcks(kgo.AllISRAcks()),
		}
	default:
		// Fallback — should not happen due to validation, but safe default
		return []kgo.Opt{
			kgo.RequiredAcks(kgo.LeaderAck()),
			kgo.DisableIdempotentWrite(),
		}
	}
}

func (e *KafkaExporter) Start(ctx context.Context, _ component.Host) error {
	e.logger.Info("Starting STS Kafka exporter",
		zap.Strings("brokers", e.cfg.Brokers),
		zap.String("topic", e.cfg.Topic))

	// Fail fast: check if topic exists
	if err := e.checkTopicExists(ctx); err != nil {
		// [debug] returning an error doesn't seem to surface as a log message
		e.logger.Debug("failed to start STS Kafka exporter", zap.Error(err))
		return fmt.Errorf("failed to start STS Kafka exporter: %w", err)
	}

	return nil
}

func (e *KafkaExporter) Shutdown(_ context.Context) error {
	e.client.Close() // also closes the underlying client for the adminClient
	return nil
}

func (e *KafkaExporter) checkTopicExists(ctx context.Context) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, e.cfg.ReadTimeout)
	defer cancel()

	// Use admin client to check topic metadata
	topicDetails, err := e.adminClient.ListTopics(timeoutCtx, e.cfg.Topic)
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	if len(topicDetails) == 0 {
		return fmt.Errorf("topic %s not found", e.cfg.Topic)
	}

	for topic, detail := range topicDetails {
		if detail.Err != nil {
			return fmt.Errorf("topic %s error: %w", topic, detail.Err)
		}

		e.logger.Info("Topic found",
			zap.String("topic", topic),
			zap.Int("partitions", len(detail.Partitions)))
	}

	return nil
}

func (e *KafkaExporter) ExportData(ctx context.Context, ld plog.Logs) error {
	// Doing synchronous sends with a bounded context timeout.
	// Retry handling is left to exporterhelper - only return on fatal produce errors.
	deadlineCtx, cancel := context.WithTimeout(ctx, e.cfg.ProduceTimeout)
	defer cancel()

	return iterateLogRecords(ld, func(lr plog.LogRecord) error {
		// Extract Kafka message key
		key, err := extractKey(lr.Attributes())
		if err != nil {
			e.logger.Warn("failed to build Kafka message key; dropping", zap.Error(err))
			return err
		}

		// Extract Kafka message value
		value, err := e.extractValue(lr)
		if err != nil {
			e.logger.Warn("failed to build Kafka message value; dropping", zap.Error(err))
			return err
		}

		record := &kgo.Record{
			Topic: e.cfg.Topic,
			Key:   key,
			Value: value,
		}

		// ProduceSync blocks until the record is acked or errored.
		// FirstErr returns the first error if multiple records failed.
		if err := e.client.ProduceSync(deadlineCtx, record).FirstErr(); err != nil {
			// Return immediately so exporterhelper can retry.
			e.logger.Warn("produce failed", zap.Error(err))
			return err
		}
		return nil
	})
}

// iterateLogRecords calls fn for every LogRecord along with its ResourceLogs and ScopeLogs.
func iterateLogRecords(ld plog.Logs, fn func(lr plog.LogRecord) error) error {
	resLogs := ld.ResourceLogs()
	var firstErr error

	for i := 0; i < resLogs.Len(); i++ {
		rl := resLogs.At(i)
		scopeLogs := rl.ScopeLogs()

		for j := 0; j < scopeLogs.Len(); j++ {
			sl := scopeLogs.At(j)
			logs := sl.LogRecords()

			for k := 0; k < logs.Len(); k++ {
				lr := logs.At(k)
				if err := fn(lr); err != nil && firstErr == nil {
					firstErr = err
				}
			}
		}
	}
	return firstErr
}

// extractKey retrieves the logical Kafka key from attributes
func extractKey(attrs pcommon.Map) ([]byte, error) {
	key, ok := attrs.Get(KafkaMessageKey)
	if !ok {
		return nil, fmt.Errorf("missing %s attribute", KafkaMessageKey)
	}

	return append([]byte(nil), key.Bytes().AsRaw()...), nil
}

// extractValue retrieves the message body from the plog.LogRecord.
func (e *KafkaExporter) extractValue(lr plog.LogRecord) ([]byte, error) {
	body := lr.Body()

	//nolint:exhaustive,gocritic
	if body.Type() == pcommon.ValueTypeBytes {
		return append([]byte(nil), body.Bytes().AsRaw()...), nil
	}
	return nil, errors.New("unsupported log record body type")
}
