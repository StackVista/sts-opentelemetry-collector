package topologyconnector

import (
	"context"
	"sync"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const metadataTopic = "sts_topology_stream_metadata"

// MetadataPublisher publishes topology stream metadata to a compacted Kafka topic
// so that the SyncService can enrich components with human-readable mapping names.
// It implements SnapshotUpdateListener and publishes on every snapshot update.
// The topic is compacted, so repeated publishes for unchanged mappings are harmless
// and provide self-healing if topic state is ever lost.
type MetadataPublisher struct {
	logger       *zap.Logger
	mu           sync.RWMutex
	logsConsumer consumer.Logs
}

func NewMetadataPublisher(logger *zap.Logger) *MetadataPublisher {
	return &MetadataPublisher{
		logger: logger,
	}
}

// SetLogsConsumer sets the downstream logs consumer used for publishing.
// Called by the connector when it is created, before any snapshot updates arrive.
func (p *MetadataPublisher) SetLogsConsumer(c consumer.Logs) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.logsConsumer == nil {
		p.logsConsumer = c
	}
}

// Update implements SnapshotUpdateListener. It publishes metadata for all current
// component and relation mappings on every snapshot update.
func (p *MetadataPublisher) Update(
	_ []settingsproto.OtelInputSignal,
	componentMappings map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping,
	relationMappings map[settingsproto.OtelInputSignal][]settingsproto.OtelRelationMapping,
) {
	p.mu.RLock()
	c := p.logsConsumer
	p.mu.RUnlock()

	if c == nil {
		p.logger.Debug("MetadataPublisher: no logs consumer set yet, skipping metadata publish")
		return
	}

	seen := make(map[string]struct{})
	logs := plog.NewLogs()
	scopeLog := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()

	for _, mappings := range componentMappings {
		for _, m := range mappings {
			if _, ok := seen[m.GetIdentifier()]; ok {
				continue
			}
			seen[m.GetIdentifier()] = struct{}{}
			if err := addMetadataRecord(&scopeLog, m.GetIdentifier(), m.GetName()); err != nil {
				p.logger.Error("failed to build metadata record", zap.String("mapping", m.GetIdentifier()), zap.Error(err))
			}
		}
	}

	for _, mappings := range relationMappings {
		for _, m := range mappings {
			if _, ok := seen[m.GetIdentifier()]; ok {
				continue
			}
			seen[m.GetIdentifier()] = struct{}{}
			if err := addMetadataRecord(&scopeLog, m.GetIdentifier(), m.GetName()); err != nil {
				p.logger.Error("failed to build metadata record", zap.String("mapping", m.GetIdentifier()), zap.Error(err))
			}
		}
	}

	if logs.LogRecordCount() == 0 {
		return
	}

	p.logger.Info("Publishing topology stream metadata", zap.Int("mappings", logs.LogRecordCount()))
	if err := c.ConsumeLogs(context.Background(), logs); err != nil {
		p.logger.Error("failed to publish metadata logs", zap.Error(err))
	}
}

// PublishTombstones publishes tombstone (null-value) records for removed mappings.
// On the compacted metadata topic, this causes Kafka to eventually delete the record.
// On the StackState side, the consumer deletes the corresponding ExtTopoStreamMetadata vertex.
func (p *MetadataPublisher) PublishTombstones(removedMappings []settingsproto.SettingExtension) {
	p.mu.RLock()
	c := p.logsConsumer
	p.mu.RUnlock()

	if c == nil || len(removedMappings) == 0 {
		return
	}

	logs := plog.NewLogs()
	scopeLog := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()

	for _, m := range removedMappings {
		if err := addMetadataTombstone(&scopeLog, m.GetIdentifier()); err != nil {
			p.logger.Error("failed to build metadata tombstone", zap.String("mapping", m.GetIdentifier()), zap.Error(err))
		}
	}

	if logs.LogRecordCount() == 0 {
		return
	}

	p.logger.Info("Publishing metadata tombstones for removed mappings", zap.Int("count", logs.LogRecordCount()))
	if err := c.ConsumeLogs(context.Background(), logs); err != nil {
		p.logger.Error("failed to publish metadata tombstones", zap.Error(err))
	}
}

func addMetadataTombstone(scopeLog *plog.ScopeLogs, identifier string) error {
	key := &topostreamv1.TopologyStreamMetadataMessageKey{
		Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
		DataSource: identifier,
	}

	keyBytes, err := proto.Marshal(key)
	if err != nil {
		return err
	}

	lr := scopeLog.LogRecords().AppendEmpty()
	lr.Body().SetEmptyBytes() // empty body → nil Kafka value → tombstone
	lr.Attributes().PutEmptyBytes(stskafkaexporter.KafkaMessageKey).FromRaw(keyBytes)
	lr.Attributes().PutStr(stskafkaexporter.KafkaMessageTopic, metadataTopic)
	return nil
}

func addMetadataRecord(scopeLog *plog.ScopeLogs, identifier, name string) error {
	selectorProperties, err := buildSelectorProperties(identifier, name)
	if err != nil {
		return err
	}

	key := &topostreamv1.TopologyStreamMetadataMessageKey{
		Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
		DataSource: identifier,
	}
	msg := &topostreamv1.TopologyStreamMetadataMessage{
		DisplayName:        name,
		SelectorProperties: selectorProperties,
	}

	keyBytes, err := proto.Marshal(key)
	if err != nil {
		return err
	}
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	lr := scopeLog.LogRecords().AppendEmpty()
	lr.Body().SetEmptyBytes().FromRaw(msgBytes)
	lr.Attributes().PutEmptyBytes(stskafkaexporter.KafkaMessageKey).FromRaw(keyBytes)
	lr.Attributes().PutStr(stskafkaexporter.KafkaMessageTopic, metadataTopic)
	return nil
}

func buildSelectorProperties(identifier, name string) (*structpb.Struct, error) {
	return structpb.NewStruct(map[string]interface{}{
		"mapping": map[string]interface{}{
			"identifier": identifier,
			"name":       name,
		},
	})
}
