package stskafkaexporter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

const kafkaMessageKey = "stskafka.key"

// exporterComponent is the interface that both production and stub exporters implement.
type exporterComponent interface {
	exportData(ctx context.Context, ld plog.Logs) error
	// export functions for other data types go here

	start(ctx context.Context, host component.Host) error
	shutdown(ctx context.Context) error
}

type kafkaExporter struct {
	cfg         *Config
	logger      *zap.Logger
	client      *kgo.Client
	adminClient *kadm.Client
}

func newKafkaExporter(cfg Config, set exporter.CreateSettings) (*kafkaExporter, error) {
	clientID := fmt.Sprintf("stskafkaexporter-%s", uuid.New().String())
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(clientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	}

	set.Logger.Info("Creating Kafka exporter", zap.String("clientID", clientID))
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed creating franz-go (kafka) client: %w", err)
	}

	return &kafkaExporter{
		cfg:         &cfg,
		logger:      set.Logger,
		client:      client,
		adminClient: kadm.NewClient(client),
	}, nil
}

func (e *kafkaExporter) start(ctx context.Context, _ component.Host) error {
	e.logger.Info("Starting Kafka settings provider",
		zap.Strings("brokers", e.cfg.Brokers),
		zap.String("topic", e.cfg.Topic))

	// Fail fast: check if topic exists
	if err := e.checkTopicExists(ctx); err != nil {
		return fmt.Errorf("failed to start kafka settings provider: %w", err)
	}

	return nil
}

func (e *kafkaExporter) shutdown(_ context.Context) error {
	e.client.Close() // also closes the underlying client for the adminClient
	return nil
}

func (e *kafkaExporter) checkTopicExists(ctx context.Context) error {
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

func (e *kafkaExporter) exportData(ctx context.Context, ld plog.Logs) error {
	// Doing synchronous sends with a bounded context timeout.
	// Retry handling is left to exporterhelper - only return on fatal produce errors.
	deadlineCtx, cancel := context.WithTimeout(ctx, e.cfg.ProduceTimeout)
	defer cancel()

	return iterateLogRecords(ld, func(lr plog.LogRecord) error {
		// Extract Kafka message key (hashed into []byte for stable partitioning).
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

// extractKey retrieves the logical Kafka key from attributes and hashes it.
// This ensures stable partitioning across producers.
func extractKey(attrs pcommon.Map) ([]byte, error) {
	key, ok := attrs.Get(kafkaMessageKey)
	if !ok {
		return nil, fmt.Errorf("missing %s attribute", kafkaMessageKey)
	}

	h := xxhash.Sum64([]byte(key.Str()))
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, h)
	return buf, nil
}

// extractValue retrieves the message body from the plog.LogRecord.
func (e *kafkaExporter) extractValue(lr plog.LogRecord) ([]byte, error) {
	body := lr.Body()
	switch body.Type() {
	case pcommon.ValueTypeBytes:
		// Clone to avoid aliasing shared memory.
		return append([]byte(nil), body.Bytes().AsRaw()...), nil
	}
	return nil, errors.New("unsupported log record body type")
}
