package tracetotopoconnector

import (
	"context"
	"testing"

	topo_stream_v1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestConnectorStart(t *testing.T) {
	logConsumer := &consumertest.LogsSink{}

	t.Run("return an error if not found settings provider", func(t *testing.T) {
		connector, err := newConnector(zap.NewNop(), Config{}, logConsumer)
		require.NoError(t, err)
		err = connector.Start(context.Background(), componenttest.NewNopHost())
		require.ErrorContains(t, err, "sts_settings_provider extension not found")
	})

	t.Run("start with initial mappings and observe changes", func(t *testing.T) {
		connector, _ := newConnector(zap.NewNop(), Config{}, logConsumer)
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
			settingsProviderExtensionId: provider,
		}
		host := &mockHost{ext: extensions}
		err := connector.Start(context.Background(), host)
		require.NoError(t, err)
		assert.Len(t, connector.componentMappings, 1)
		assert.Len(t, connector.relationMappings, 2)

		provider.componentMappings = []settings.OtelComponentMapping{
			createSimpleComponentMapping("mapping1"),
			createSimpleComponentMapping("mapping2"),
			createSimpleComponentMapping("mapping3"),
		}
		provider.relationMappings = []settings.OtelRelationMapping{
			createSimpleRelationMapping("mapping4"),
		}
		assert.Len(t, connector.componentMappings, 1)
		assert.Len(t, connector.relationMappings, 2)

		provider.channel <- stsSettingsEvents.UpdateSettingsEvent{}
		assert.Len(t, connector.componentMappings, 3)
		assert.Len(t, connector.relationMappings, 1)
	})
}
func TestConnectorConsumeTraces(t *testing.T) {
	logConsumer := &consumertest.LogsSink{}
	connector, _ := newConnector(zap.NewNop(), Config{}, logConsumer)
	provider := newMockStsSettingsProvider(
		[]settings.OtelComponentMapping{
			createSimpleComponentMapping("mapping1"),
		},
		[]settings.OtelRelationMapping{
			createSimpleRelationMapping("mapping2"),
		},
	)
	var extensions = map[component.ID]component.Component{
		settingsProviderExtensionId: provider,
	}
	host := &mockHost{ext: extensions}
	err := connector.Start(context.Background(), host)
	require.NoError(t, err)

	t.Run("skip spans which don't match to conditions", func(t *testing.T) {
		logConsumer.Reset()
		submittedTime := int64(1756851083000)
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

		assert.Equal(t, logConsumer.LogRecordCount(), 0)

		err := connector.ConsumeTraces(context.Background(), traces)
		require.NoError(t, err)

		assert.Equal(t, logConsumer.LogRecordCount(), 0) // all conditions doesn't match so it is empty
		assert.Empty(t, logConsumer.AllLogs())
	})

	t.Run("start with initial mappings and observe changes", func(t *testing.T) {
		logConsumer.Reset()
		submittedTime := int64(1756851083000)
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

		assert.Equal(t, logConsumer.LogRecordCount(), 0)

		err := connector.ConsumeTraces(context.Background(), traces)
		require.NoError(t, err)

		assert.Equal(t, logConsumer.LogRecordCount(), 2) // one for component and one for relation
		actual := logConsumer.AllLogs()
		assert.Len(t, actual, 1)
		assert.Equal(t, actual[0].ResourceLogs().Len(), 1)
		assert.Equal(t, actual[0].ResourceLogs().At(0).ScopeLogs().Len(), 1)
		assert.Equal(t, actual[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().Len(), 2)

		componentLogRecord := actual[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
		componentKeyRaw, _ := componentLogRecord.Attributes().Get(stskafkaexporter.KafkaMessageKey)
		componentKey := topo_stream_v1.TopologyStreamMessageKey{}
		proto.Unmarshal(componentKeyRaw.Bytes().AsRaw(), &componentKey)
		assert.Equal(t, componentKey.Owner, topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL)
		assert.Equal(t, componentKey.DataSource, "mapping1")
		assert.Equal(t, componentKey.ShardId, "627cc493")
		componentMassage := topo_stream_v1.TopologyStreamMessage{}
		proto.Unmarshal(componentLogRecord.Body().Bytes().AsRaw(), &componentMassage)
		componentPayload := componentMassage.Payload.(*topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData)
		assert.Equal(t, componentPayload.TopologyStreamRepeatElementsData.ExpiryIntervalMs, int64(60000))
		assert.Equal(t, len(componentPayload.TopologyStreamRepeatElementsData.Components), 1)
		assert.Equal(t, componentPayload.TopologyStreamRepeatElementsData.Components[0].ExternalId, "627cc493")
		assert.Equal(t, componentPayload.TopologyStreamRepeatElementsData.Components[0].Name, "checkout-service")
		assert.Equal(t, componentPayload.TopologyStreamRepeatElementsData.Components[0].TypeName, "service-instance")
		assert.Equal(t, componentPayload.TopologyStreamRepeatElementsData.Components[0].DomainName, "shop")
		assert.Equal(t, componentPayload.TopologyStreamRepeatElementsData.Components[0].LayerName, "backend")

		relationLogRecord := actual[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(1)
		relationKeyRaw, _ := relationLogRecord.Attributes().Get(stskafkaexporter.KafkaMessageKey)
		relationKey := topo_stream_v1.TopologyStreamMessageKey{}
		proto.Unmarshal(relationKeyRaw.Bytes().AsRaw(), &relationKey)
		assert.Equal(t, relationKey.Owner, topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL)
		assert.Equal(t, relationKey.DataSource, "mapping2")
		assert.Equal(t, relationKey.ShardId, "checkout-service-web-service")
		relationMessage := topo_stream_v1.TopologyStreamMessage{}
		proto.Unmarshal(relationLogRecord.Body().Bytes().AsRaw(), &relationMessage)
		relationPayload := relationMessage.Payload.(*topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData)
		assert.Equal(t, relationPayload.TopologyStreamRepeatElementsData.ExpiryIntervalMs, int64(300000))
		assert.Equal(t, len(relationPayload.TopologyStreamRepeatElementsData.Relations), 1)
		assert.Equal(t, relationPayload.TopologyStreamRepeatElementsData.Relations[0].SourceIdentifier, "checkout-service")
		assert.Equal(t, relationPayload.TopologyStreamRepeatElementsData.Relations[0].TargetIdentifier, "web-service")
		assert.Equal(t, relationPayload.TopologyStreamRepeatElementsData.Relations[0].TypeName, "http-request")

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

func (m *mockStsSettingsProvider) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (m *mockStsSettingsProvider) Shutdown(ctx context.Context) error {
	return nil
}

func (m *mockStsSettingsProvider) RegisterForUpdates(types ...settings.SettingType) (<-chan stsSettingsEvents.UpdateSettingsEvent, error) {
	return m.channel, nil
}

func (m *mockStsSettingsProvider) Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool {
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
	switch typ {
	case settings.SettingTypeOtelComponentMapping:
		return toAnySlice(m.componentMappings), nil
	case settings.SettingTypeOtelRelationMapping:
		return toAnySlice(m.relationMappings), nil
	}
	return nil, nil
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
		ExpireAfterMs: 60000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: boolExpr(`spanAttributes["http.method"] == "GET"`)},
		},
		Output: settings.OtelComponentMappingOutput{
			Identifier: strExpr("resourceAttributes[\"service.instance.id\"]"),
			Name:       strExpr(`resourceAttributes["service.name"]`),
			TypeName:   strExpr(`"service-instance"`),
			DomainName: strExpr(`resourceAttributes["service.namespace"]`),
			LayerName:  strExpr(`"backend"`),
		},
	}
}

func createSimpleRelationMapping(id string) settings.OtelRelationMapping {
	return settings.OtelRelationMapping{
		Id:            id,
		ExpireAfterMs: 300000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: boolExpr(`spanAttributes["http.status_code"] == "200"`)},
		},
		Output: settings.OtelRelationMappingOutput{
			SourceId: strExpr(`resourceAttributes["service.name"]`),
			TargetId: strExpr(`spanAttributes["service.name"]`),
			TypeName: strExpr(`"http-request"`),
		},
	}
}
