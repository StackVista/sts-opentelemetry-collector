//nolint:testpackage
package tracetotopoconnector

import (
	"context"
	"errors"
	"fmt"
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
		connector, err := newConnector(Config{}, zap.NewNop(), logConsumer)
		require.NoError(t, err)
		err = connector.Start(context.Background(), componenttest.NewNopHost())
		require.ErrorContains(t, err, "sts_settings_provider extension not found")
	})

	t.Run("start with initial mappings and observe changes", func(t *testing.T) {
		connector, _ := newConnector(Config{}, zap.NewNop(), logConsumer)
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
			component.MustNewID(SettingsProviderExtensionID): provider,
		}
		host := &mockHost{ext: extensions}
		err := connector.Start(context.Background(), host)
		require.NoError(t, err)
		assert.Len(t, *connector.componentMappings, 1)
		assert.Len(t, *connector.relationMappings, 2)

		provider.componentMappings = []settings.OtelComponentMapping{
			createSimpleComponentMapping("mapping1"),
			createSimpleComponentMapping("mapping2"),
			createSimpleComponentMapping("mapping3"),
		}
		provider.relationMappings = []settings.OtelRelationMapping{
			createSimpleRelationMapping("mapping4"),
		}
		assert.Len(t, *connector.componentMappings, 1)
		assert.Len(t, *connector.relationMappings, 2)

		provider.channel <- stsSettingsEvents.UpdateSettingsEvent{}
		assert.Len(t, *connector.componentMappings, 3)
		assert.Len(t, *connector.relationMappings, 1)
	})
}

func TestConnectorConsumeTraces(t *testing.T) {
	logConsumer := &consumertest.LogsSink{}
	connector, _ := newConnector(Config{}, zap.NewNop(), logConsumer)
	provider := newMockStsSettingsProvider(
		[]settings.OtelComponentMapping{
			createSimpleComponentMapping("mapping1"),
		},
		[]settings.OtelRelationMapping{
			createSimpleRelationMapping("mapping2"),
		},
	)
	var extensions = map[component.ID]component.Component{
		component.MustNewID(SettingsProviderExtensionID): provider,
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
		componentKey := topo_stream_v1.TopologyStreamMessageKey{}
		err = proto.Unmarshal(componentKeyRaw.Bytes().AsRaw(), &componentKey)
		require.NoError(t, err)
		assert.Equal(t, topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL, componentKey.Owner)
		assert.Equal(t, "urn:otel-component-mapping:mapping1", componentKey.DataSource)
		assert.Equal(t, "0", componentKey.ShardId)
		componentMassage := topo_stream_v1.TopologyStreamMessage{}
		err = proto.Unmarshal(componentLogRecord.Body().Bytes().AsRaw(), &componentMassage)
		require.NoError(t, err)
		componentPayload, _ := componentMassage.Payload.(*topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData)
		assert.Equal(t, int64(60000), componentPayload.TopologyStreamRepeatElementsData.ExpiryIntervalMs)
		assert.Equal(t, 1, len(componentPayload.TopologyStreamRepeatElementsData.Components))
		assert.Equal(t, "627cc493", componentPayload.TopologyStreamRepeatElementsData.Components[0].ExternalId)
		assert.Equal(t, "checkout-service", componentPayload.TopologyStreamRepeatElementsData.Components[0].Name)
		assert.Equal(t, "service-instance", componentPayload.TopologyStreamRepeatElementsData.Components[0].TypeName)
		assert.Equal(t, "shop", componentPayload.TopologyStreamRepeatElementsData.Components[0].DomainName)
		assert.Equal(t, "backend", componentPayload.TopologyStreamRepeatElementsData.Components[0].LayerName)

		relationLogRecord := actual[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(1)
		relationKeyRaw, _ := relationLogRecord.Attributes().Get(stskafkaexporter.KafkaMessageKey)
		relationKey := topo_stream_v1.TopologyStreamMessageKey{}
		err = proto.Unmarshal(relationKeyRaw.Bytes().AsRaw(), &relationKey)
		require.NoError(t, err)
		assert.Equal(t, topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL, relationKey.Owner)
		assert.Equal(t, "urn:otel-relation-mapping:mapping2", relationKey.DataSource)
		assert.Equal(t, "2", relationKey.ShardId)
		relationMessage := topo_stream_v1.TopologyStreamMessage{}
		err = proto.Unmarshal(relationLogRecord.Body().Bytes().AsRaw(), &relationMessage)
		require.NoError(t, err)
		relationPayload, _ := relationMessage.Payload.(*topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData)
		assert.Equal(t, int64(300000), relationPayload.TopologyStreamRepeatElementsData.ExpiryIntervalMs)
		assert.Equal(t, 1, len(relationPayload.TopologyStreamRepeatElementsData.Relations))
		assert.Equal(t, "checkout-service", relationPayload.TopologyStreamRepeatElementsData.Relations[0].SourceIdentifier)
		assert.Equal(t, "web-service", relationPayload.TopologyStreamRepeatElementsData.Relations[0].TargetIdentifier)
		assert.Equal(t, "http-request", relationPayload.TopologyStreamRepeatElementsData.Relations[0].TypeName)

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

func ptr[T any](v T) *T { return &v }

func createSimpleComponentMapping(id string) settings.OtelComponentMapping {
	return settings.OtelComponentMapping{
		Id:            id,
		Identifier:    ptr(fmt.Sprintf("urn:otel-component-mapping:%s", id)),
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
		Identifier:    ptr(fmt.Sprintf("urn:otel-relation-mapping:%s", id)),
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
