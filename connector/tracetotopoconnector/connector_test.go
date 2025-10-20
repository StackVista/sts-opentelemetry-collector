//nolint:testpackage
package tracetotopoconnector

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	stsSettingsApi "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type noopMetrics struct{}

func (n *noopMetrics) IncSpansProcessed(_ context.Context, _ int64) {}
func (n *noopMetrics) IncMappingsProduced(_ context.Context, _ int64, _ settings.SettingType, _ ...attribute.KeyValue) {
}
func (n *noopMetrics) IncSettingsRemoved(_ context.Context, _ int64, _ settings.SettingType) {}
func (n *noopMetrics) IncMappingErrors(_ context.Context, _ int64, _ settings.SettingType)   {}
func (n *noopMetrics) RecordMappingDuration(_ context.Context, _ time.Duration, _ ...attribute.KeyValue) {
}

func TestConnectorStart(t *testing.T) {
	logConsumer := &consumertest.LogsSink{}

	t.Run("return an error if not found settings provider", func(t *testing.T) {
		connector, err := newConnector(
			context.Background(), Config{}, zap.NewNop(), componenttest.NewNopTelemetrySettings(), logConsumer,
		)
		require.NoError(t, err)
		err = connector.Start(context.Background(), componenttest.NewNopHost())
		require.ErrorContains(t, err, "sts_settings_provider extension not found")
	})

	t.Run("start with initial mappings and observe changes", func(t *testing.T) {
		connector, _ := newConnector(
			context.Background(), Config{}, zap.NewNop(), componenttest.NewNopTelemetrySettings(), logConsumer,
		)
		provider := newMockStsSettingsProvider(
			[]settings.OtelComponentMapping{
				createSimpleComponentMapping("mapping1"),
			},
			[]settings.OtelRelationMapping{
				createSimpleRelationMapping("mapping2"),
				createSimpleRelationMapping("mapping3"),
			},
		)
		var extensions = map[component.ID]component.Component{
			component.MustNewID(stsSettingsApi.Type.String()): provider,
		}
		host := &mockHost{ext: extensions}
		err := connector.Start(context.Background(), host)
		require.NoError(t, err)
		componentMappings, relationMappings := connector.snapshotManager.Current()
		assert.Len(t, componentMappings, 1)
		assert.Len(t, relationMappings, 2)

		provider.componentMappings = []settings.OtelComponentMapping{
			createSimpleComponentMapping("mapping1"),
			createSimpleComponentMapping("mapping2"),
			createSimpleComponentMapping("mapping3"),
		}
		provider.relationMappings = []settings.OtelRelationMapping{
			createSimpleRelationMapping("mapping4"),
		}
		componentMappings, relationMappings = connector.snapshotManager.Current()
		assert.Len(t, componentMappings, 1)
		assert.Len(t, relationMappings, 2)

		provider.channel <- stsSettingsEvents.UpdateSettingsEvent{}
		time.Sleep(100 * time.Millisecond) // wait for snapshot manager to update
		componentMappings, relationMappings = connector.snapshotManager.Current()
		assert.Len(t, componentMappings, 3)
		assert.Len(t, relationMappings, 1)
	})

	t.Run("emits removal messages when mappings are deleted", func(t *testing.T) {
		ctx := context.Background()
		logConsumer := &consumertest.LogsSink{}
		metrics := &noopMetrics{}
		connector, _ := newConnector(Config{}, zap.NewNop(), metrics, logConsumer)

		provider := newMockStsSettingsProvider(
			[]settings.OtelComponentMapping{
				createSimpleComponentMapping("mapping1"),
			},
			[]settings.OtelRelationMapping{
				createSimpleRelationMapping("mapping2"),
			},
		)
		var extensions = map[component.ID]component.Component{
			component.MustNewID(stsSettingsApi.Type.String()): provider,
		}
		host := &mockHost{ext: extensions}
		require.NoError(t, connector.Start(ctx, host))

		// Verify initial state has mappings
		componentMappings, relationMappings := connector.snapshotManager.Current()
		require.Len(t, componentMappings, 1)
		require.Len(t, relationMappings, 1)

		// Simulate removal (empty provider)
		provider.componentMappings = nil
		provider.relationMappings = nil
		provider.channel <- stsSettingsEvents.UpdateSettingsEvent{}

		expectedLogRecordsCount := (len(componentMappings) + len(relationMappings)) * 4 // 4 shards

		// Wait for async update to propagate and removal logs to be emitted
		require.Eventually(t, func() bool {
			return logConsumer.LogRecordCount() >= expectedLogRecordsCount
		}, 2*time.Second, 100*time.Millisecond)

		allLogs := logConsumer.AllLogs()
		require.NotEmpty(t, allLogs)

		// Extract all log records
		var records []plog.LogRecord
		for i := 0; i < len(allLogs); i++ {
			rl := allLogs[i].ResourceLogs()
			for j := 0; j < rl.Len(); j++ {
				sl := rl.At(j).ScopeLogs()
				for k := 0; k < sl.Len(); k++ {
					lr := sl.At(k).LogRecords()
					for n := 0; n < lr.Len(); n++ {
						records = append(records, lr.At(n))
					}
				}
			}
		}
		require.Len(t, records, expectedLogRecordsCount)

		// message content is validated by the pipeline_test.go
	})
}

func TestConnectorConsumeTraces(t *testing.T) {
	logConsumer := &consumertest.LogsSink{}
	connector, _ := newConnector(context.Background(), Config{}, zap.NewNop(), componenttest.NewNopTelemetrySettings(), logConsumer)
	provider := newMockStsSettingsProvider(
		[]settings.OtelComponentMapping{
			createSimpleComponentMapping("mapping1"),
		},
		[]settings.OtelRelationMapping{
			createSimpleRelationMapping("mapping2"),
		},
	)
	var extensions = map[component.ID]component.Component{
		component.MustNewID(stsSettingsApi.Type.String()): provider,
	}
	host := &mockHost{ext: extensions}
	err := connector.Start(context.Background(), host)
	require.NoError(t, err)

	t.Run("skip spans which don't match to conditions", func(t *testing.T) {
		logConsumer.Reset()
		submittedTime := uint64(1756851083000)
		traces := ptrace.NewTraces()
		rs := traces.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "checkout-service")
		rs.Resource().Attributes().PutStr("service.instance.id", "627cc493")
		rs.Resource().Attributes().PutStr("service.namespace", "shop")
		rs.Resource().Attributes().PutStr("service.version", "1.2.3")
		rs.Resource().Attributes().PutStr("host.name", "ip-10-1-2-3.ec2.internal")
		rs.Resource().Attributes().PutStr("os.type", "linux")
		rs.Resource().Attributes().PutStr("process.pid", "12345")
		rs.Resource().Attributes().PutStr("cloud.provider", "aws")
		rs.Resource().Attributes().PutStr("k8s.pod.name", "checkout-service-8675309")
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().Attributes().PutStr("otel.scope.name", "io.opentelemetry.instrumentation.http")
		ss.Scope().Attributes().PutStr("otel.scope.version", "1.17.0")
		span := ss.Spans().AppendEmpty()
		span.SetEndTimestamp(pcommon.Timestamp(submittedTime))
		span.Attributes().PutStr("http.method", "POST")     // doesn't match component conditions
		span.Attributes().PutStr("http.status_code", "404") // doesn't match relation conditions
		span.Attributes().PutStr("db.system", "postgresql")
		span.Attributes().PutStr("db.statement", "SELECT * FROM users WHERE id = 123")
		span.Attributes().PutStr("net.peer.name", "api.example.com")
		span.Attributes().PutStr("user.id", "123")
		span.Attributes().PutStr("service.name", "web-service")

		assert.Equal(t, 0, logConsumer.LogRecordCount())

		err := connector.ConsumeTraces(context.Background(), traces)
		require.NoError(t, err)

		assert.Equal(t, 0, logConsumer.LogRecordCount()) // all conditions doesn't match so it is empty
		assert.Empty(t, logConsumer.AllLogs())
	})

	t.Run("start with initial mappings and observe changes", func(t *testing.T) {
		logConsumer.Reset()
		submittedTime := uint64(1756851083000)
		traces := ptrace.NewTraces()
		rs := traces.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "checkout-service")
		rs.Resource().Attributes().PutStr("service.instance.id", "627cc493")
		rs.Resource().Attributes().PutStr("service.namespace", "shop")
		rs.Resource().Attributes().PutStr("service.version", "1.2.3")
		rs.Resource().Attributes().PutStr("host.name", "ip-10-1-2-3.ec2.internal")
		rs.Resource().Attributes().PutStr("os.type", "linux")
		rs.Resource().Attributes().PutStr("process.pid", "12345")
		rs.Resource().Attributes().PutStr("cloud.provider", "aws")
		rs.Resource().Attributes().PutStr("k8s.pod.name", "checkout-service-8675309")
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().Attributes().PutStr("otel.scope.name", "io.opentelemetry.instrumentation.http")
		ss.Scope().Attributes().PutStr("otel.scope.version", "1.17.0")
		span := ss.Spans().AppendEmpty()
		span.SetEndTimestamp(pcommon.Timestamp(submittedTime))
		span.Attributes().PutStr("http.method", "GET")
		span.Attributes().PutStr("http.status_code", "200")
		span.Attributes().PutStr("db.system", "postgresql")
		span.Attributes().PutStr("db.statement", "SELECT * FROM users WHERE id = 123")
		span.Attributes().PutStr("net.peer.name", "api.example.com")
		span.Attributes().PutStr("user.id", "123")
		span.Attributes().PutStr("service.name", "web-service")

		assert.Equal(t, 0, logConsumer.LogRecordCount())

		err := connector.ConsumeTraces(context.Background(), traces)
		require.NoError(t, err)

		assert.Equal(t, 2, logConsumer.LogRecordCount()) // one for component and one for relation
		actual := logConsumer.AllLogs()
		assert.Len(t, actual, 1)
		assert.Equal(t, 1, actual[0].ResourceLogs().Len())
		assert.Equal(t, 1, actual[0].ResourceLogs().At(0).ScopeLogs().Len())
		assert.Equal(t, 2, actual[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().Len())

		componentLogRecord := actual[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
		componentKeyRaw, _ := componentLogRecord.Attributes().Get(stskafkaexporter.KafkaMessageKey)
		componentKey := topostreamv1.TopologyStreamMessageKey{}
		err = proto.Unmarshal(componentKeyRaw.Bytes().AsRaw(), &componentKey)
		require.NoError(t, err)
		assert.Equal(t, topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL, componentKey.Owner)
		assert.Equal(t, "urn:otel-component-mapping:mapping1", componentKey.DataSource)
		assert.Equal(t, "0", componentKey.ShardId)
		componentMassage := topostreamv1.TopologyStreamMessage{}
		err = proto.Unmarshal(componentLogRecord.Body().Bytes().AsRaw(), &componentMassage)
		require.NoError(t, err)
		componentPayload, _ := componentMassage.Payload.(*topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData)
		assert.Equal(t, int64(60000), componentPayload.TopologyStreamRepeatElementsData.ExpiryIntervalMs)
		assert.Equal(t, 1, len(componentPayload.TopologyStreamRepeatElementsData.Components))
		assert.Equal(t, "627cc493", componentPayload.TopologyStreamRepeatElementsData.Components[0].ExternalId)
		assert.Equal(t, "checkout-service", componentPayload.TopologyStreamRepeatElementsData.Components[0].Name)
		assert.Equal(t, "service-instance", componentPayload.TopologyStreamRepeatElementsData.Components[0].TypeName)
		assert.Equal(t, "shop", componentPayload.TopologyStreamRepeatElementsData.Components[0].DomainName)
		assert.Equal(t, "backend", componentPayload.TopologyStreamRepeatElementsData.Components[0].LayerName)

		relationLogRecord := actual[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(1)
		relationKeyRaw, _ := relationLogRecord.Attributes().Get(stskafkaexporter.KafkaMessageKey)
		relationKey := topostreamv1.TopologyStreamMessageKey{}
		err = proto.Unmarshal(relationKeyRaw.Bytes().AsRaw(), &relationKey)
		require.NoError(t, err)
		assert.Equal(t, topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL, relationKey.Owner)
		assert.Equal(t, "urn:otel-relation-mapping:mapping2", relationKey.DataSource)
		assert.Equal(t, "2", relationKey.ShardId)
		relationMessage := topostreamv1.TopologyStreamMessage{}
		err = proto.Unmarshal(relationLogRecord.Body().Bytes().AsRaw(), &relationMessage)
		require.NoError(t, err)
		relationPayload, _ := relationMessage.Payload.(*topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData)
		assert.Equal(t, int64(300000), relationPayload.TopologyStreamRepeatElementsData.ExpiryIntervalMs)
		assert.Equal(t, 1, len(relationPayload.TopologyStreamRepeatElementsData.Relations))
		assert.Equal(t, "checkout-service", relationPayload.TopologyStreamRepeatElementsData.Relations[0].SourceIdentifier)
		assert.Equal(t, "web-service", relationPayload.TopologyStreamRepeatElementsData.Relations[0].TargetIdentifier)
		assert.Equal(t, "http-request", relationPayload.TopologyStreamRepeatElementsData.Relations[0].TypeName)

	})
}

func TestPublishTopologyMessagesAsLogs(t *testing.T) {
	ctx := context.Background()
	logConsumer := &consumertest.LogsSink{}
	logger := zap.NewNop()

	conn := &connectorImpl{
		logger:       logger,
		logsConsumer: logConsumer,
	}

	t.Run("publishes valid messages", func(t *testing.T) {
		logConsumer.Reset()
		key := &topostreamv1.TopologyStreamMessageKey{
			Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: "urn:test:component",
			ShardId:    "1",
		}
		message := &topostreamv1.TopologyStreamMessage{
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
					ExpiryIntervalMs: 60000,
				},
			},
		}

		conn.publishMessagesAsLogs(ctx, []internal.MessageWithKey{{
			Key:     key,
			Message: message,
		}})

		require.Equal(t, 1, logConsumer.LogRecordCount())

		allLogs := logConsumer.AllLogs()
		require.Len(t, allLogs, 1)
		scopeLogs := allLogs[0].ResourceLogs().At(0).ScopeLogs().At(0)
		require.Equal(t, 1, scopeLogs.LogRecords().Len())

		logRecord := scopeLogs.LogRecords().At(0)
		keyRaw, _ := logRecord.Attributes().Get(stskafkaexporter.KafkaMessageKey)
		var decodedKey topostreamv1.TopologyStreamMessageKey
		err := proto.Unmarshal(keyRaw.Bytes().AsRaw(), &decodedKey)
		require.NoError(t, err)
		assert.Equal(t, "urn:test:component", decodedKey.DataSource)
	})

	t.Run("handles empty messages gracefully", func(t *testing.T) {
		logConsumer.Reset()
		conn.publishMessagesAsLogs(ctx, nil)
		assert.Equal(t, 0, logConsumer.LogRecordCount())
		conn.publishMessagesAsLogs(ctx, []internal.MessageWithKey{})
		assert.Equal(t, 0, logConsumer.LogRecordCount())
	})
}

type mockHost struct {
	component.Host
	ext map[component.ID]component.Component
}

func (nh *mockHost) GetExtensions() map[component.ID]component.Component {
	return nh.ext
}

type mockStsSettingsProvider struct {
	componentMappings []settings.OtelComponentMapping
	relationMappings  []settings.OtelRelationMapping
	channel           chan stsSettingsEvents.UpdateSettingsEvent
}

func newMockStsSettingsProvider(componentMappings []settings.OtelComponentMapping, relationMappings []settings.OtelRelationMapping) *mockStsSettingsProvider {
	return &mockStsSettingsProvider{
		componentMappings: componentMappings,
		relationMappings:  relationMappings,
		channel:           make(chan stsSettingsEvents.UpdateSettingsEvent),
	}
}

func (m *mockStsSettingsProvider) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (m *mockStsSettingsProvider) Shutdown(_ context.Context) error {
	return nil
}

func (m *mockStsSettingsProvider) RegisterForUpdates(_ ...settings.SettingType) (<-chan stsSettingsEvents.UpdateSettingsEvent, error) {
	return m.channel, nil
}

func (m *mockStsSettingsProvider) Unregister(_ <-chan stsSettingsEvents.UpdateSettingsEvent) bool {
	return true
}

func toAnySlice[T any](slice []T) []any {
	result := make([]any, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}

func (m *mockStsSettingsProvider) UnsafeGetCurrentSettingsByType(typ settings.SettingType) ([]any, error) {
	//nolint:exhaustive
	switch typ {
	case settings.SettingTypeOtelComponentMapping:
		return toAnySlice(m.componentMappings), nil
	case settings.SettingTypeOtelRelationMapping:
		return toAnySlice(m.relationMappings), nil
	default:
		return nil, errors.New("not supported type of settings")
	}
}

func strExpr(s string) settings.OtelStringExpression {
	return settings.OtelStringExpression{
		Expression: s,
	}
}

func boolExpr(s string) settings.OtelBooleanExpression {
	return settings.OtelBooleanExpression{
		Expression: s,
	}
}

func createSimpleComponentMapping(id string) settings.OtelComponentMapping {
	return settings.OtelComponentMapping{
		Id:            id,
		Identifier:    fmt.Sprintf("urn:otel-component-mapping:%s", id),
		ExpireAfterMs: 60000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: boolExpr(`spanAttributes["http.method"] == "GET"`)},
		},
		Output: settings.OtelComponentMappingOutput{
			Identifier: strExpr("${resourceAttributes[\"service.instance.id\"]}"),
			Name:       strExpr(`${resourceAttributes["service.name"]}`),
			TypeName:   strExpr("service-instance"),
			DomainName: strExpr(`${resourceAttributes["service.namespace"]}`),
			LayerName:  strExpr("backend"),
		},
	}
}

func createSimpleRelationMapping(id string) settings.OtelRelationMapping {
	return settings.OtelRelationMapping{
		Id:            id,
		Identifier:    fmt.Sprintf("urn:otel-relation-mapping:%s", id),
		ExpireAfterMs: 300000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: boolExpr(`spanAttributes["http.status_code"] == "200"`)},
		},
		Output: settings.OtelRelationMappingOutput{
			SourceId: strExpr(`${resourceAttributes["service.name"]}`),
			TargetId: strExpr(`${spanAttributes["service.name"]}`),
			TypeName: strExpr("http-request"),
		},
	}
}
