package stskafkaexporter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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

func NewKafkaExporter(cfg Config, set exporter.Settings) (*KafkaExporter, error) {
	clientID := fmt.Sprintf("stskafkaexporter-%s", uuid.New().String())
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(clientID),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.ProducerLinger(10 * time.Millisecond),
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
	case AcksNone:
		return []kgo.Opt{
			kgo.RequiredAcks(kgo.NoAck()),
			kgo.DisableIdempotentWrite(),
		}
	case AcksAll:
		// Idempotency requires acks=all, so we don’t disable it here
		return []kgo.Opt{
			kgo.RequiredAcks(kgo.AllISRAcks()),
		}
	default: // "leader"
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

// ExportData exports OpenTelemetry logs to Kafka.
// Returns an error only on fatal produce failures; retries are handled by exporterhelper.
func (e *KafkaExporter) ExportData(ctx context.Context, ld plog.Logs) error {
	records, err := e.buildKafkaRecords(ld)

	if len(records) == 0 {
		e.logger.Debug("no Kafka records to export")
		return nil
	}

	if err != nil {
		e.logger.Warn("failed to build some Kafka records; partial logs dropped", zap.Error(err))
	}

	return e.produceRecords(ctx, records)
}

// produceRecords sends records to Kafka based on the configured acknowledgment mode.
func (e *KafkaExporter) produceRecords(ctx context.Context, records []*kgo.Record) error {
	e.logger.Debug(fmt.Sprintf("producing Kafka records with acks=%s", e.cfg.RequiredAcks),
		zap.Int("recordCount", len(records)))

	if e.cfg.RequiredAcks == AcksNone {
		return e.produceFireAndForget(records)
	}

	return e.produceWithAcks(ctx, records)
}

// buildKafkaRecords transforms OpenTelemetry logs into Kafka records.
func (e *KafkaExporter) buildKafkaRecords(ld plog.Logs) ([]*kgo.Record, error) {
	var records []*kgo.Record

	err := iterateLogRecords(ld, func(lr plog.LogRecord) error {
		key, err := extractMessageKey(lr.Attributes())
		if err != nil {
			return err
		}

		value, err := e.extractMessageValue(lr)
		if err != nil {
			return err
		}

		records = append(records, &kgo.Record{
			Topic: e.cfg.Topic,
			Key:   key,
			Value: value,
		})
		return nil
	})

	return records, err
}

// produceFireAndForget sends records without waiting for acknowledgments (acks=none).
// Uses context.Background() to prevent premature cancellation of async operations.
func (e *KafkaExporter) produceFireAndForget(records []*kgo.Record) error {
	for _, rec := range records {
		e.client.Produce(context.Background(), rec, func(_ *kgo.Record, err error) {
			if err != nil {
				e.logger.Warn("kafka async produce failed (acks=none)", zap.Error(err))
			}
		})
	}

	return nil
}

// produceWithAcks sends records and waits for acknowledgments (acks=leader/all).
// Bounded by ProduceTimeout to prevent indefinite hangs.
func (e *KafkaExporter) produceWithAcks(ctx context.Context, records []*kgo.Record) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, e.cfg.ProduceTimeout)
	defer cancel()

	return produceAndWait(deadlineCtx, e.client, e.logger, records)
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

// extractMessageKey retrieves the logical Kafka key from attributes
func extractMessageKey(attrs pcommon.Map) ([]byte, error) {
	key, ok := attrs.Get(KafkaMessageKey)
	if !ok {
		return nil, fmt.Errorf("missing required attribute: %s", KafkaMessageKey)
	}

	return append([]byte(nil), key.Bytes().AsRaw()...), nil
}

// extractMessageValue retrieves the message body from the plog.LogRecord.
func (e *KafkaExporter) extractMessageValue(lr plog.LogRecord) ([]byte, error) {
	body := lr.Body()

	//nolint:exhaustive,gocritic
	if body.Type() == pcommon.ValueTypeBytes {
		return append([]byte(nil), body.Bytes().AsRaw()...), nil
	}
	return nil, errors.New("unsupported log record body type (expected bytes)")
}

// produceAndWait sends all records asynchronously and waits for completion.
// Collects and returns the first error encountered.
func produceAndWait(ctx context.Context, client *kgo.Client, logger *zap.Logger, records []*kgo.Record) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(records))

	for _, rec := range records {
		wg.Add(1)
		client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				errCh <- err
			}
		})
	}

	wg.Wait()
	close(errCh)

	// Return the first error encountered
	firstErr := <-errCh
	if firstErr != nil {
		logger.Warn("one or more Kafka produce operations failed", zap.Error(firstErr))
	}

	return firstErr
}
