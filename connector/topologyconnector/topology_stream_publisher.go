package topologyconnector

import (
	"context"
	"fmt"
	"sync"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// streamKey identifies a single topology stream. A stream is the unit the platform keys on:
// (dataSource, shardId). dataSource is the mapping identifier; shardId is the stable
// per-element shard.
type streamKey struct {
	dataSource string
	shardID    string
}

// TopologyStreamPublisher is the single funnel through which all topology stream messages are
// published to the downstream logs consumer. Because every message flows through here, it is
// also the natural place to observe topology stream lifecycle: it logs (exactly once) when a
// stream is first seen and when a mapping's streams are removed.
//
// It is constructed once (a shared singleton) and used by all per-signal connector instances.
type TopologyStreamPublisher struct {
	logger *zap.Logger

	// mu guards both logsConsumer and known.
	mu           sync.Mutex
	logsConsumer consumer.Logs
	// known tracks the stream keys we have already emitted data for, so creation is logged once.
	known map[streamKey]struct{}
}

func NewTopologyStreamPublisher(logger *zap.Logger) *TopologyStreamPublisher {
	return &TopologyStreamPublisher{
		logger: logger,
		known:  make(map[streamKey]struct{}),
	}
}

// SetLogsConsumer wires the downstream logs consumer.
func (p *TopologyStreamPublisher) SetLogsConsumer(c consumer.Logs) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.logsConsumer == nil {
		p.logsConsumer = c
	}
}

// Publish serializes the given messages into a single plog batch and forwards them to the
// downstream logs consumer. As a side effect it logs newly-created streams.
func (p *TopologyStreamPublisher) Publish(ctx context.Context, messages []internal.MessageWithKey) {
	if len(messages) == 0 {
		return
	}

	p.mu.Lock()
	c := p.logsConsumer
	p.mu.Unlock()
	if c == nil {
		p.logger.Debug("TopologyStreamPublisher: no logs consumer set yet, dropping messages")
		return
	}

	log := plog.NewLogs()
	scopeLog := log.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	for _, mwk := range messages {
		if err := addEvent(&scopeLog, mwk); err != nil {
			p.logger.Error("failed to add event to scope log", zap.Error(err))
			continue
		}
		p.logger.Debug("added event to scope log", zap.Any("key", mwk.Key))
	}

	if log.LogRecordCount() == 0 {
		return
	}

	if err := c.ConsumeLogs(ctx, log); err != nil {
		p.logger.Error("Error sending logs to the next component", zap.Error(err))
	}

	p.trackCreations(messages)
}

// trackCreations records any stream keys seen for the first time, logging each once. Only
// element/data payloads count as a stream creation; removal payloads and the "unknown" error
// pseudo-shard are ignored.
func (p *TopologyStreamPublisher) trackCreations(messages []internal.MessageWithKey) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, mwk := range messages {
		if !isStreamDataPayload(mwk.Message) {
			continue
		}
		if mwk.Key.GetShardId() == internal.UnknownShardID {
			continue
		}
		k := streamKey{
			dataSource: mwk.Key.GetDataSource(),
			shardID:    mwk.Key.GetShardId(),
		}
		if _, ok := p.known[k]; ok {
			continue
		}
		p.known[k] = struct{}{}
		p.logger.Info("Topology stream created",
			zap.String("dataSource", k.dataSource),
			zap.String("shardId", k.shardID))
	}
}

// OnMappingRemoved logs and evicts every tracked stream belonging to the given removed
// mappings (dataSources). Evicting the keys means a later re-creation of the same stream will log again.
func (p *TopologyStreamPublisher) OnMappingRemoved(dataSources []string) {
	if len(dataSources) == 0 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, ds := range dataSources {
		evicted := 0
		for k := range p.known {
			if k.dataSource == ds {
				delete(p.known, k)
				evicted++
			}
		}
		p.logger.Info("Topology stream removed",
			zap.String("dataSource", ds),
			zap.Int("shardStreamsEvicted", evicted))
	}
}

// isStreamDataPayload reports whether the message carries element/data (RepeatElements) as
// opposed to a removal or other lifecycle payload.
func isStreamDataPayload(m *topostreamv1.TopologyStreamMessage) bool {
	_, ok := m.GetPayload().(*topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData)
	return ok
}

// addEvent adds a new event to the scope log. The event contains the body with serialized
// TopologyStreamMessage and an attribute with the serialized TopologyStreamMessageKey.
func addEvent(scopeLog *plog.ScopeLogs, mwk internal.MessageWithKey) error {
	msgAsBytes, err := proto.Marshal(mwk.Message)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	keyAsBytes, err := proto.Marshal(mwk.Key)
	if err != nil {
		return fmt.Errorf("marshal key: %w", err)
	}

	logRecord := scopeLog.LogRecords().AppendEmpty()
	logRecord.Body().SetEmptyBytes().FromRaw(msgAsBytes)
	logRecord.Attributes().PutEmptyBytes(stskafkaexporter.KafkaMessageKey).FromRaw(keyAsBytes)
	return nil
}
