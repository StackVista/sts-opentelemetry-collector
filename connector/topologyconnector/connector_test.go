//nolint:testpackage
package topologyconnector

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	stsSettingsApi "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap/zaptest"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

// ptr is a helper to get a pointer to any value.
type MessageWithKey struct {
	Key     *topostreamv1.TopologyStreamMessageKey
	Message *topostreamv1.TopologyStreamMessage
}

// Helper to create a TopologyStreamMessageKey
func extractMessagesWithKey(t *testing.T, logs plog.Logs) []*MessageWithKey {
	var messages []*MessageWithKey
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rl := logs.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				messages = append(messages, extractMessageWithKey(t, sl.LogRecords().At(k)))
			}
		}
	}
	return messages
}

func extractMessageWithKey(t *testing.T, lr plog.LogRecord) *MessageWithKey {
	keyBytes, ok := lr.Attributes().Get(stskafkaexporter.KafkaMessageKey)
	require.True(t, ok)
	msgBytes := lr.Body().Bytes().AsRaw()

	var key topostreamv1.TopologyStreamMessageKey
	err := proto.Unmarshal(keyBytes.Bytes().AsRaw(), &key)
	require.NoError(t, err)

	var msg topostreamv1.TopologyStreamMessage
	err = proto.Unmarshal(msgBytes, &msg)
	require.NoError(t, err)

	return &MessageWithKey{Key: &key, Message: &msg}
}

type connectorTestEnv struct {
	// nolint:containedctx
	ctx          context.Context
	logsConsumer *consumertest.LogsSink // we want the test interface
	connector    *connectorImpl
}

func newConnectorEnv(t *testing.T, signal settings.OtelInputSignal) *connectorTestEnv {
	t.Helper()

	ctx := context.Background()
	logsConsumer := &consumertest.LogsSink{}
	logger := zaptest.NewLogger(t)

	snapshotManager := NewSnapshotManager(logger, []settings.OtelInputSignal{settings.TRACES, settings.METRICS})
	celEvaluator, _ := internal.NewCELEvaluator(
		ctx,
		metrics.MeteredCacheSettings{
			Name:          "expression_cache",
			EnableMetrics: false,
		},
	)
	mapper := internal.NewMapper(
		ctx,
		metrics.MeteredCacheSettings{
			Name:          "tag_regex_cache",
			EnableMetrics: false,
		},
		metrics.MeteredCacheSettings{
			Name:          "tag_template_cache",
			EnableMetrics: false,
		},
	)

	conn := newConnector(
		ctx,
		Config{},
		logger,
		componenttest.NewNopTelemetrySettings(),
		logsConsumer,
		snapshotManager,
		celEvaluator,
		mapper,
		signal,
	)

	return &connectorTestEnv{
		ctx:          ctx,
		logsConsumer: logsConsumer,
		connector:    conn,
	}
}

func TestConnectorStart(t *testing.T) {
	t.Run("return an error if not found settings provider", func(t *testing.T) {
		connectorEnv := newConnectorEnv(t, settings.TRACES)
		err := connectorEnv.connector.Start(connectorEnv.ctx, componenttest.NewNopHost())
		require.ErrorContains(t, err, "sts_settings_provider extension not found")
	})

	t.Run("start with initial mappings and observe changes", func(t *testing.T) {
		connectorEnv := newConnectorEnv(t, settings.TRACES)
		provider := NewMockStsSettingsProvider(
			[]settings.OtelComponentMapping{
				createSimpleTraceComponentMapping("mapping1"),
			},
			[]settings.OtelRelationMapping{
				createSimpleTraceRelationMapping("mapping2"),
				createSimpleTraceRelationMapping("mapping3"),
			},
		)
		var extensions = map[component.ID]component.Component{
			component.MustNewID(stsSettingsApi.Type.String()): provider,
		}
		host := &mockHost{ext: extensions}
		err := connectorEnv.connector.Start(connectorEnv.ctx, host)
		require.NoError(t, err)
		componentMappings, relationMappings := connectorEnv.connector.snapshotManager.Current(settings.TRACES)
		assert.Len(t, componentMappings, 1)
		assert.Len(t, relationMappings, 2)

		provider.ComponentMappings = []settings.OtelComponentMapping{
			createSimpleTraceComponentMapping("mapping1"),
			createSimpleTraceComponentMapping("mapping2"),
			createSimpleTraceComponentMapping("mapping3"),
		}
		provider.RelationMappings = []settings.OtelRelationMapping{
			createSimpleTraceRelationMapping("mapping4"),
		}
		componentMappings, relationMappings = connectorEnv.connector.snapshotManager.Current(settings.TRACES)
		assert.Len(t, componentMappings, 1)
		assert.Len(t, relationMappings, 2)

		provider.SettingUpdatesCh <- stsSettingsEvents.UpdateSettingsEvent{}
		time.Sleep(100 * time.Millisecond) // wait for snapshot manager to update
		componentMappings, relationMappings = connectorEnv.connector.snapshotManager.Current(settings.TRACES)
		assert.Len(t, componentMappings, 3)
		assert.Len(t, relationMappings, 1)
	})

	t.Run("emits removal messages when mappings are deleted", func(t *testing.T) {
		connectorEnv := newConnectorEnv(t, settings.TRACES)

		provider := NewMockStsSettingsProvider(
			[]settings.OtelComponentMapping{
				createSimpleTraceComponentMapping("mapping1"),
			},
			[]settings.OtelRelationMapping{
				createSimpleTraceRelationMapping("mapping2"),
			},
		)
		var extensions = map[component.ID]component.Component{
			component.MustNewID(stsSettingsApi.Type.String()): provider,
		}
		host := &mockHost{ext: extensions}
		require.NoError(t, connectorEnv.connector.Start(connectorEnv.ctx, host))

		// Verify initial state has mappings
		componentMappings, relationMappings := connectorEnv.connector.snapshotManager.Current(settings.TRACES)
		require.Len(t, componentMappings, 1)
		require.Len(t, relationMappings, 1)

		// Simulate removal (empty provider)
		provider.ComponentMappings = nil
		provider.RelationMappings = nil
		provider.SettingUpdatesCh <- stsSettingsEvents.UpdateSettingsEvent{}

		expectedLogRecordsCount := (len(componentMappings) + len(relationMappings)) * 4 // 4 shards

		// Wait for async update to propagate and removal logs to be emitted
		require.Eventually(t, func() bool {
			return connectorEnv.logsConsumer.LogRecordCount() >= expectedLogRecordsCount
		}, 2*time.Second, 100*time.Millisecond)

		allLogs := connectorEnv.logsConsumer.AllLogs()
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
	connectorEnv := newConnectorEnv(t, settings.TRACES)
	provider := NewMockStsSettingsProvider(
		[]settings.OtelComponentMapping{
			createSimpleTraceComponentMapping("mapping1"),
		},
		[]settings.OtelRelationMapping{
			createSimpleTraceRelationMapping("mapping2"),
		},
	)

	var extensions = map[component.ID]component.Component{
		component.MustNewID(stsSettingsApi.Type.String()): provider,
	}
	host := &mockHost{ext: extensions}
	err := connectorEnv.connector.Start(connectorEnv.ctx, host)
	require.NoError(t, err)

	t.Run("skip spans which don't match to conditions", func(t *testing.T) {
		connectorEnv.logsConsumer.Reset()
		submittedTime := uint64(1756851083000)
		traces := ptrace.NewTraces()
		rs := traces.ResourceSpans().AppendEmpty()
		_ = rs.Resource().Attributes().FromRaw(map[string]any{
			"service.name":        "checkout-service",
			"service.instance.id": "627cc493",
			"service.namespace":   "shop",
			"service.version":     "1.2.3",
			"host.name":           "ip-10-1-2-3.ec2.internal",
			"os.type":             "linux",
			"process.pid":         "12345",
			"cloud.provider":      "aws",
			"k8s.pod.name":        "checkout-service-8675309",
		})
		ss := rs.ScopeSpans().AppendEmpty()
		_ = ss.Scope().Attributes().FromRaw(map[string]any{"otel.scope.name": "io.opentelemetry.instrumentation.http", "otel.scope.version": "1.17.0"})
		span := ss.Spans().AppendEmpty()
		span.SetEndTimestamp(pcommon.Timestamp(submittedTime))
		_ = span.Attributes().FromRaw(map[string]any{
			"http.method":      "POST", // doesn't match component conditions
			"http.status_code": "404",  // doesn't match relation conditions
			"db.system":        "postgresql",
			"db.statement":     "SELECT * FROM users WHERE id = 123",
			"net.peer.name":    "api.example.com",
			"user.id":          "123",
			"service.name":     "web-service",
		})

		assert.Equal(t, 0, connectorEnv.logsConsumer.LogRecordCount())

		err := connectorEnv.connector.ConsumeTraces(connectorEnv.ctx, traces)
		require.NoError(t, err)

		assert.Equal(t, 0, connectorEnv.logsConsumer.LogRecordCount()) // all conditions doesn't match so it is empty
		assert.Empty(t, connectorEnv.logsConsumer.AllLogs())
	})

	t.Run("start with initial mappings and observe changes", func(t *testing.T) {
		connectorEnv.logsConsumer.Reset()
		traces := ptrace.NewTraces()
		rs := traces.ResourceSpans().AppendEmpty()
		_ = rs.Resource().Attributes().FromRaw(map[string]any{
			"service.name":        "checkout-service",
			"service.instance.id": "627cc493",
			"service.namespace":   "shop",
			"service.version":     "1.2.3",
			"host.name":           "ip-10-1-2-3.ec2.internal",
			"os.type":             "linux",
			"process.pid":         "12345",
			"cloud.provider":      "aws",
			"k8s.pod.name":        "checkout-service-8675309",
		})
		ss := rs.ScopeSpans().AppendEmpty()
		_ = ss.Scope().Attributes().FromRaw(map[string]any{"otel.scope.name": "io.opentelemetry.instrumentation.http", "otel.scope.version": "1.17.0"})
		span := ss.Spans().AppendEmpty()
		span.SetEndTimestamp(pcommon.Timestamp(time.Now().UnixMilli()))
		_ = span.Attributes().FromRaw(map[string]any{
			"http.method":      "GET",
			"http.status_code": "200",
			"db.system":        "postgresql",
			"db.statement":     "SELECT * FROM users WHERE id = 123",
			"net.peer.name":    "api.example.com",
			"user.id":          "123",
			"service.name":     "web-service",
		})

		assert.Equal(t, 0, connectorEnv.logsConsumer.LogRecordCount())

		err = connectorEnv.connector.ConsumeTraces(connectorEnv.ctx, traces)
		require.NoError(t, err)

		actualLogs := connectorEnv.logsConsumer.AllLogs()
		require.Len(t, actualLogs, 1)
		messages := extractMessagesWithKey(t, actualLogs[0])
		require.Len(t, messages, 2)

		var componentMsg, relationMsg *MessageWithKey
		for _, msg := range messages {
			if msg.Message.GetTopologyStreamRepeatElementsData().GetComponents() != nil {
				componentMsg = msg
			}
			if msg.Message.GetTopologyStreamRepeatElementsData().GetRelations() != nil {
				relationMsg = msg
			}
		}
		require.NotNil(t, componentMsg, "component message not found")
		require.NotNil(t, relationMsg, "relation message not found")

		// Assert Component Message
		assert.Equal(t, "urn:otel-component-mapping:mapping1", componentMsg.Key.DataSource)
		assert.InDelta(t, time.Now().UnixMilli(), componentMsg.Message.SubmittedTimestamp, 1000,
			"SubmittedTimestamp should be within 1 second of now")
		compData := componentMsg.Message.GetTopologyStreamRepeatElementsData()
		require.NotNil(t, compData)
		assert.Equal(t, int64(60000), compData.ExpiryIntervalMs)
		require.Len(t, compData.Components, 1)
		component := compData.Components[0]
		assert.Equal(t, "627cc493", component.ExternalId)
		assert.Equal(t, "checkout-service", component.Name)
		assert.Equal(t, "service-instance", component.TypeName)
		assert.Equal(t, "shop", component.DomainName)
		assert.Equal(t, "backend", component.LayerName)
		assert.Equal(t, "status_code:200", compData.Components[0].Tags[0])

		// Assert Relation Message
		assert.Equal(t, "urn:otel-relation-mapping:mapping2", relationMsg.Key.DataSource)
		assert.InDelta(t, time.Now().UnixMilli(), componentMsg.Message.SubmittedTimestamp, 1000,
			"SubmittedTimestamp should be within 1 second of now")
		relData := relationMsg.Message.GetTopologyStreamRepeatElementsData()
		require.NotNil(t, relData)
		assert.Equal(t, int64(300000), relData.ExpiryIntervalMs)
		require.Len(t, relData.Relations, 1)
		relation := relData.Relations[0]
		assert.Equal(t, "checkout-service-web-service", relation.ExternalId)
		assert.Equal(t, "checkout-service", relation.SourceIdentifier)
		assert.Equal(t, "web-service", relation.TargetIdentifier)
		assert.Equal(t, "http-request", relation.TypeName)
	})
}

// TODO: Add tests that trace mappings are excluded for metrics connector and vice versa
func TestConnectorConsumeMetrics(t *testing.T) {
	connectorEnv := newConnectorEnv(t, settings.METRICS)

	provider := NewMockStsSettingsProvider(
		[]settings.OtelComponentMapping{
			createSimpleMetricComponentMapping("mapping1"),
		},
		[]settings.OtelRelationMapping{
			createSimpleMetricRelationMapping("mapping2"),
		},
	)
	var extensions = map[component.ID]component.Component{
		component.MustNewID(stsSettingsApi.Type.String()): provider,
	}
	host := &mockHost{ext: extensions}
	err := connectorEnv.connector.Start(connectorEnv.ctx, host)
	require.NoError(t, err)

	t.Run("skip metrics which don't match to conditions", func(t *testing.T) {
		connectorEnv.logsConsumer.Reset()
		submittedTime := uint64(1756851083000)
		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		_ = rm.Resource().Attributes().FromRaw(map[string]any{
			"service.name":        "checkout-service",
			"service.instance.id": "627cc493",
			"service.namespace":   "shop",
		})
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("http.server.duration")
		sum := m.SetEmptySum()
		dp := sum.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(submittedTime))
		_ = dp.Attributes().FromRaw(map[string]any{
			"http.method":      "POST", // doesn't match component conditions
			"http.status_code": "404",  // doesn't match relation conditions
			"service.name":     "web-service",
		})

		assert.Equal(t, 0, connectorEnv.logsConsumer.LogRecordCount())

		err := connectorEnv.connector.ConsumeMetrics(connectorEnv.ctx, metrics)
		require.NoError(t, err)

		assert.Equal(t, 0, connectorEnv.logsConsumer.LogRecordCount()) // all conditions doesn't match so it is empty
		assert.Empty(t, connectorEnv.logsConsumer.AllLogs())
	})

	t.Run("start with initial mappings and observe changes", func(t *testing.T) {
		connectorEnv.logsConsumer.Reset()
		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		_ = rm.Resource().Attributes().FromRaw(map[string]any{
			"service.name":        "checkout-service",
			"service.instance.id": "627cc493",
			"service.namespace":   "shop",
		})
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("http.server.duration")
		sum := m.SetEmptySum()
		dp := sum.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixMilli()))
		_ = dp.Attributes().FromRaw(map[string]any{
			"http.method":      "GET",
			"http.status_code": "200",
			"service.name":     "web-service",
		})

		assert.Equal(t, 0, connectorEnv.logsConsumer.LogRecordCount())

		err = connectorEnv.connector.ConsumeMetrics(connectorEnv.ctx, metrics)
		require.NoError(t, err)

		actualLogs := connectorEnv.logsConsumer.AllLogs()
		require.Len(t, actualLogs, 1)
		messages := extractMessagesWithKey(t, actualLogs[0])
		require.Len(t, messages, 2)

		var componentMsg, relationMsg *MessageWithKey
		for _, msg := range messages {
			if msg.Message.GetTopologyStreamRepeatElementsData().GetComponents() != nil {
				componentMsg = msg
			}
			if msg.Message.GetTopologyStreamRepeatElementsData().GetRelations() != nil {
				relationMsg = msg
			}
		}
		require.NotNil(t, componentMsg, "component message not found")
		require.NotNil(t, relationMsg, "relation message not found")

		// Assert Component Message
		assert.Equal(t, "urn:otel-component-mapping:mapping1", componentMsg.Key.DataSource)
		assert.InDelta(t, time.Now().UnixMilli(), componentMsg.Message.SubmittedTimestamp, 1000,
			"SubmittedTimestamp should be within 1 second of now")
		compData := componentMsg.Message.GetTopologyStreamRepeatElementsData()
		require.NotNil(t, compData)
		require.Len(t, compData.Components, 1)
		component := compData.Components[0]
		assert.Equal(t, "627cc493", component.ExternalId)
		assert.Equal(t, "checkout-service", component.Name)
		assert.Equal(t, "service-instance", component.TypeName)
		assert.Equal(t, "shop", component.DomainName)
		assert.Equal(t, "backend", component.LayerName)

		assert.Equal(t, "status_code:200", compData.Components[0].Tags[0])

		// Assert Relation Message
		assert.Equal(t, "urn:otel-relation-mapping:mapping2", relationMsg.Key.DataSource)
		assert.InDelta(t, time.Now().UnixMilli(), componentMsg.Message.SubmittedTimestamp, 1000,
			"SubmittedTimestamp should be within 1 second of now")
		relData := relationMsg.Message.GetTopologyStreamRepeatElementsData()
		require.NotNil(t, relData)
		require.Len(t, relData.Relations, 1)
		relation := relData.Relations[0]
		assert.Equal(t, "checkout-service-web-service", relation.ExternalId)
		assert.Equal(t, "checkout-service", relation.SourceIdentifier)
		assert.Equal(t, "web-service", relation.TargetIdentifier)
		assert.Equal(t, "http-request", relation.TypeName)
	})
}

func TestPublishTopologyMessagesAsLogs(t *testing.T) {
	connectorEnv := newConnectorEnv(t, settings.METRICS)

	t.Run("publishes valid messages", func(t *testing.T) {
		connectorEnv.logsConsumer.Reset()
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

		connectorEnv.connector.publishMessagesAsLogs(connectorEnv.ctx, []internal.MessageWithKey{{
			Key:     key,
			Message: message,
		}})

		require.Equal(t, 1, connectorEnv.logsConsumer.LogRecordCount())

		allLogs := connectorEnv.logsConsumer.AllLogs()
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
		connectorEnv.logsConsumer.Reset()
		connectorEnv.connector.publishMessagesAsLogs(connectorEnv.ctx, nil)
		assert.Equal(t, 0, connectorEnv.logsConsumer.LogRecordCount())
		connectorEnv.connector.publishMessagesAsLogs(connectorEnv.ctx, []internal.MessageWithKey{})
		assert.Equal(t, 0, connectorEnv.logsConsumer.LogRecordCount())
	})
}

type mockHost struct {
	component.Host
	ext map[component.ID]component.Component
}

func (nh *mockHost) GetExtensions() map[component.ID]component.Component {
	return nh.ext
}

type MockStsSettingsProvider struct {
	ComponentMappings []settings.OtelComponentMapping
	RelationMappings  []settings.OtelRelationMapping
	SettingUpdatesCh  chan stsSettingsEvents.UpdateSettingsEvent
}

func NewMockStsSettingsProvider(componentMappings []settings.OtelComponentMapping, relationMappings []settings.OtelRelationMapping) *MockStsSettingsProvider {
	return &MockStsSettingsProvider{
		ComponentMappings: componentMappings,
		RelationMappings:  relationMappings,
		SettingUpdatesCh:  make(chan stsSettingsEvents.UpdateSettingsEvent),
	}
}

func (m *MockStsSettingsProvider) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (m *MockStsSettingsProvider) Shutdown(_ context.Context) error {
	return nil
}

func (m *MockStsSettingsProvider) RegisterForUpdates(_ ...settings.SettingType) (<-chan stsSettingsEvents.UpdateSettingsEvent, error) {
	return m.SettingUpdatesCh, nil
}

func (m *MockStsSettingsProvider) Unregister(_ <-chan stsSettingsEvents.UpdateSettingsEvent) bool {
	return true
}

func toAnySlice[T any](slice []T) []any {
	result := make([]any, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}

func (m *MockStsSettingsProvider) UnsafeGetCurrentSettingsByType(typ settings.SettingType) ([]any, error) {
	//nolint:exhaustive
	switch typ {
	case settings.SettingTypeOtelComponentMapping:
		return toAnySlice(m.ComponentMappings), nil
	case settings.SettingTypeOtelRelationMapping:
		return toAnySlice(m.RelationMappings), nil
	default:
		return nil, errors.New("not supported type of settings")
	}
}

func strExpr(s string) settings.OtelStringExpression {
	return settings.OtelStringExpression{
		Expression: s,
	}
}

func anyExpr(s string) settings.OtelAnyExpression {
	return settings.OtelAnyExpression{
		Expression: s,
	}
}

func boolExpr(s string) settings.OtelBooleanExpression {
	return settings.OtelBooleanExpression{
		Expression: s,
	}
}

func createSimpleTraceComponentMapping(id string) settings.OtelComponentMapping {
	return settings.OtelComponentMapping{
		Id:            id,
		Identifier:    fmt.Sprintf("urn:otel-component-mapping:%s", id),
		ExpireAfterMs: 60000,
		Input: settings.OtelInput{
			Signal: settings.OtelInputSignalList{
				settings.TRACES,
			},
			Resource: settings.OtelInputResource{
				Condition: boolExpr(`true`),
				Action:    settings.CONTINUE,
				Scope: &settings.OtelInputScope{
					Condition: boolExpr(`true`),
					Action:    settings.CONTINUE,
					Span: &settings.OtelInputSpan{
						Condition: boolExpr(`spanAttributes["http.method"] == "GET"`),
						Action:    settings.CREATE,
					},
				},
			},
		},
		Output: settings.OtelComponentMappingOutput{
			Identifier: strExpr("${resourceAttributes[\"service.instance.id\"]}"),
			Name:       strExpr(`${resourceAttributes["service.name"]}`),
			TypeName:   strExpr("service-instance"),
			DomainName: strExpr(`${resourceAttributes["service.namespace"]}`),
			LayerName:  strExpr("backend"),
			Required: &settings.OtelComponentMappingFieldMapping{
				Tags: &[]settings.OtelTagMapping{
					{
						Source: anyExpr(`${spanAttributes["http.status_code"]}`),
						Target: "status_code",
					},
				},
			},
		},
	}
}

func createSimpleTraceRelationMapping(id string) settings.OtelRelationMapping {
	return settings.OtelRelationMapping{
		Id:            id,
		Identifier:    fmt.Sprintf("urn:otel-relation-mapping:%s", id),
		ExpireAfterMs: 300000,
		Input: settings.OtelInput{
			Signal: settings.OtelInputSignalList{
				settings.TRACES,
			},
			Resource: settings.OtelInputResource{
				Condition: boolExpr(`true`),
				Action:    settings.CONTINUE,
				Scope: &settings.OtelInputScope{
					Condition: boolExpr(`true`),
					Action:    settings.CONTINUE,
					Span: &settings.OtelInputSpan{
						Condition: boolExpr(`spanAttributes["http.status_code"] == "200"`),
						Action:    settings.CREATE,
					},
				},
			},
		},
		Output: settings.OtelRelationMappingOutput{
			SourceId: strExpr(`${resourceAttributes["service.name"]}`),
			TargetId: strExpr(`${spanAttributes["service.name"]}`),
			TypeName: strExpr("http-request"),
		},
	}
}

func createSimpleMetricComponentMapping(id string) settings.OtelComponentMapping {
	return settings.OtelComponentMapping{
		Id:            id,
		Identifier:    fmt.Sprintf("urn:otel-component-mapping:%s", id),
		ExpireAfterMs: 60000,
		Input: settings.OtelInput{
			Signal: settings.OtelInputSignalList{
				settings.METRICS,
			},
			Resource: settings.OtelInputResource{
				Condition: boolExpr(`true`),
				Action:    settings.CONTINUE,
				Scope: &settings.OtelInputScope{
					Condition: boolExpr(`true`),
					Action:    settings.CONTINUE,
					Metric: &settings.OtelInputMetric{
						Condition: boolExpr(`true`),
						Action:    settings.CONTINUE,
						Datapoint: &settings.OtelInputDatapoint{
							Condition: boolExpr(`datapointAttributes["http.method"] == "GET"`),
							Action:    settings.CREATE,
						},
					},
				},
			},
		},
		Output: settings.OtelComponentMappingOutput{
			Identifier: strExpr("${resourceAttributes[\"service.instance.id\"]}"),
			Name:       strExpr(`${resourceAttributes["service.name"]}`),
			TypeName:   strExpr("service-instance"),
			DomainName: strExpr(`${resourceAttributes["service.namespace"]}`),
			LayerName:  strExpr("backend"),
			Required: &settings.OtelComponentMappingFieldMapping{
				Tags: &[]settings.OtelTagMapping{
					{
						Source: anyExpr(`${datapointAttributes["http.status_code"]}`),
						Target: "status_code",
					},
				},
			},
		},
	}
}

func createSimpleMetricRelationMapping(id string) settings.OtelRelationMapping {
	return settings.OtelRelationMapping{
		Id:            id,
		Identifier:    fmt.Sprintf("urn:otel-relation-mapping:%s", id),
		ExpireAfterMs: 300000,
		Input: settings.OtelInput{
			Signal: settings.OtelInputSignalList{
				settings.METRICS,
			},
			Resource: settings.OtelInputResource{
				Condition: boolExpr(`true`),
				Action:    settings.CONTINUE,
				Scope: &settings.OtelInputScope{
					Condition: boolExpr(`true`),
					Action:    settings.CONTINUE,
					Metric: &settings.OtelInputMetric{
						Condition: boolExpr(`true`),
						Action:    settings.CONTINUE,
						Datapoint: &settings.OtelInputDatapoint{
							Condition: boolExpr(`datapointAttributes["http.status_code"] == "200"`),
							Action:    settings.CREATE,
						},
					},
				},
			},
		},
		Output: settings.OtelRelationMappingOutput{
			SourceId: strExpr(`${resourceAttributes["service.name"]}`),
			TargetId: strExpr(`${datapointAttributes["service.name"]}`),
			TypeName: strExpr("http-request"),
		},
	}
}
