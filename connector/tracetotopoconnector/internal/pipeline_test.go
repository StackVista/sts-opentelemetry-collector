//nolint:testpackage
package internal

import (
	"sort"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/pcommon"

	topo_stream_v1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func ptr[T any](v T) *T { return &v }

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

func TestPipeline_ConvertSpanToTopologyStreamMessage(t *testing.T) {

	now := time.Now().UnixMilli()
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
	//nolint:gosec
	span.SetEndTimestamp(pcommon.Timestamp(submittedTime))
	span.Attributes().PutStr("http.method", "GET")
	span.Attributes().PutStr("http.status_code", "200")
	span.Attributes().PutStr("db.system", "postgresql")
	span.Attributes().PutStr("db.statement", "SELECT * FROM users WHERE id = 123")
	span.Attributes().PutStr("net.peer.name", "api.example.com")
	span.Attributes().PutStr("user.id", "123")
	span.Attributes().PutStr("service.name", "web-service")

	//nolint:dupl,govet
	tests := []struct {
		name              string
		componentMappings []settings.OtelComponentMapping
		relationMappings  []settings.OtelRelationMapping
		expected          []MessageWithKey
	}{
		{
			name: "Convert traces to Components and Relation",
			componentMappings: []settings.OtelComponentMapping{
				{
					Id:            "mapping1a",
					ExpireAfterMs: 60000,
					Conditions: []settings.OtelConditionMapping{
						{Action: settings.CREATE, Expression: boolExpr(`vars.instanceId == "627cc493"`)},
					},
					Output: settings.OtelComponentMappingOutput{
						Identifier:     strExpr("${vars.instanceId}"),
						Name:           strExpr(`${resourceAttributes["service.name"]}`),
						TypeName:       strExpr(`service-instance`),
						TypeIdentifier: ptr(strExpr(`service_instance_id`)),
						DomainName:     strExpr(`${resourceAttributes["service.namespace"]}`),
						LayerName:      strExpr("backend"),
						Required: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `${resourceAttributes["k8s.pod.name"]}`},
							},
							Tags: &map[string]settings.OtelStringExpression{
								"host": {"${resourceAttributes['host.name']}"},
							},
						},
						Optional: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `${resourceAttributes["service.instance.id"]}`},
							},
							Tags: &map[string]settings.OtelStringExpression{
								"instrumentation-lib":      {`${scopeAttributes["otel.scope.name"]}`},
								"instrumentation-version":  {`${scopeAttributes["otel.scope.version"]}`},
								"instrumentation-provider": {`${scopeAttributes["otel.scope.provider"]}`}, // this attribute doesn't exist expr the span but it is Optional tag so it ignored by mapping
							},
						},
					},
					Vars: &[]settings.OtelVariableMapping{
						{
							Name:  "instanceId",
							Value: strExpr(`${resourceAttributes["service.instance.id"]}`),
						},
					},
				},
			},
			relationMappings: []settings.OtelRelationMapping{
				{
					Id:            "mapping1b",
					ExpireAfterMs: 300000,
					Conditions: []settings.OtelConditionMapping{
						{Action: settings.CREATE, Expression: boolExpr(`spanAttributes["http.method"] == "GET"`)},
					},
					Output: settings.OtelRelationMappingOutput{
						SourceId: strExpr(`${resourceAttributes["service.name"]}`),
						TargetId: strExpr(`${spanAttributes["service.name"]}`),
						TypeName: strExpr("http-request"),
					},
				},
			},
			expected: []MessageWithKey{
				{
					Key: &topo_stream_v1.TopologyStreamMessageKey{
						Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
						DataSource: "mapping1a",
						ShardId:    "627cc493",
					},
					Message: &topo_stream_v1.TopologyStreamMessage{
						CollectionTimestamp: now,
						SubmittedTimestamp:  submittedTime,
						Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
							TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
								ExpiryIntervalMs: 60000,
								Components: []*topo_stream_v1.TopologyStreamComponent{
									{
										ExternalId:       "627cc493",
										Identifiers:      []string{"627cc493", "checkout-service-8675309"},
										Name:             "checkout-service",
										TypeName:         "service-instance",
										TypeIdentifier:   ptr("service_instance_id"),
										DomainName:       "shop",
										DomainIdentifier: nil,
										LayerName:        "backend",
										Tags: []string{
											"instrumentation-lib:io.opentelemetry.instrumentation.http",
											"instrumentation-version:1.17.0",
											"host:ip-10-1-2-3.ec2.internal",
										},
									},
								},
							},
						},
					},
				},
				{
					Key: &topo_stream_v1.TopologyStreamMessageKey{
						Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
						DataSource: "mapping1b",
						ShardId:    "checkout-service-web-service",
					},
					Message: &topo_stream_v1.TopologyStreamMessage{
						CollectionTimestamp: now,
						SubmittedTimestamp:  submittedTime,
						Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
							TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
								ExpiryIntervalMs: 300000,
								Relations: []*topo_stream_v1.TopologyStreamRelation{
									{
										ExternalId:       "checkout-service-web-service",
										SourceIdentifier: "checkout-service",
										TargetIdentifier: "web-service",
										Name:             "",
										TypeName:         "http-request",
										TypeIdentifier:   nil,
										Tags:             nil,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Reject components with unmatched conditions",
			componentMappings: []settings.OtelComponentMapping{
				{
					Id: "mapping2",
					Conditions: []settings.OtelConditionMapping{
						{Action: settings.REJECT, Expression: boolExpr(`vars.instanceId == "627cc493"`)},
					},
					Output: settings.OtelComponentMappingOutput{
						Identifier:     strExpr("${vars.instanceId}"),
						Name:           strExpr(`${resourceAttributes["service.name"]}`),
						TypeName:       strExpr(`service-instance`),
						TypeIdentifier: ptr(strExpr(`service_instance_id`)),
						DomainName:     strExpr(`${resourceAttributes["service.namespace"]}`),
						LayerName:      strExpr(`backend`),
						Required: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `${resourceAttributes["k8s.pod.name"]}`},
							},
							Tags: &map[string]settings.OtelStringExpression{
								"host": {`${resourceAttributes["host.name"]}`},
							},
						},
						Optional: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `${resourceAttributes["service.instance.id"]}`},
							},
							Tags: &map[string]settings.OtelStringExpression{
								"instrumentation-lib":      {`${scopeAttributes["otel.scope.name"]}`},
								"instrumentation-version":  {`${scopeAttributes["otel.scope.version"]}`},
								"instrumentation-provider": {`${scopeAttributes["otel.scope.provider"]}`},
							},
						},
					},
					Vars: &[]settings.OtelVariableMapping{
						{
							Name:  "instanceId",
							Value: strExpr(`${resourceAttributes["service.instance.id"]}`),
						},
					},
				},
			},
			relationMappings: []settings.OtelRelationMapping{
				{
					Conditions: []settings.OtelConditionMapping{
						{Action: settings.REJECT, Expression: boolExpr(`spanAttributes["http.method"] == "WRONG"`)},
					},
					Output: settings.OtelRelationMappingOutput{
						SourceId: strExpr(`${resourceAttributes["service.name"]}`),
						TargetId: strExpr(`${spanAttributes["service.name"]}`),
						TypeName: strExpr("http-request"),
					},
				},
			},
			expected: []MessageWithKey{},
		},
		{
			name: "Returns error when an attribute is missing",
			componentMappings: []settings.OtelComponentMapping{
				{
					Id: "mapping3a",
					Conditions: []settings.OtelConditionMapping{
						{Action: settings.CREATE, Expression: boolExpr(`vars.instanceId == "627cc493"`)},
					},
					Output: settings.OtelComponentMappingOutput{
						Identifier:     strExpr("${vars.instanceId}"),
						Name:           strExpr(`${resourceAttributes["not-existing-attr"]}`),
						TypeName:       strExpr(`service-instance`),
						TypeIdentifier: ptr(strExpr(`service_instance_id`)),
						DomainName:     strExpr(`${resourceAttributes["service.namespace"]}`),
						LayerName:      strExpr(`backend`),
						Required: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `${resourceAttributes["k8s.pod.name"]}`},
							},
							Tags: &map[string]settings.OtelStringExpression{
								"host": {`${resourceAttributes["host.name"]}`},
							},
						},
						Optional: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `${resourceAttributes["service.instance.id"]}`},
							},
							Tags: &map[string]settings.OtelStringExpression{
								"instrumentation-lib":      {`${scopeAttributes["otel.scope.name"]}`},
								"instrumentation-version":  {`${scopeAttributes["otel.scope.version"]}`},
								"instrumentation-provider": {`${scopeAttributes["otel.scope.provider"]}`}, // this attribute doesn't exist expr the span but it is Optional tag so it ignored by mapping
							},
						},
					},
					Vars: &[]settings.OtelVariableMapping{
						{
							Name:  "instanceId",
							Value: strExpr(`${resourceAttributes["service.instance.id"]}`),
						},
					},
				},
			},
			relationMappings: []settings.OtelRelationMapping{
				{
					Id: "mapping3b",
					Conditions: []settings.OtelConditionMapping{
						{Action: settings.CREATE, Expression: boolExpr(`spanAttributes["http.method"] == "GET"`)},
					},
					Output: settings.OtelRelationMappingOutput{
						SourceId: strExpr(`${resourceAttributes["not-existing-attr"]}`),
						TargetId: strExpr(`${spanAttributes["service.name"]}`),
						TypeName: strExpr(`http-request`),
					},
				},
			},
			expected: []MessageWithKey{
				{
					Key: &topo_stream_v1.TopologyStreamMessageKey{
						Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
						DataSource: "mapping3a",
						ShardId:    "unknown",
					},
					Message: &topo_stream_v1.TopologyStreamMessage{
						CollectionTimestamp: now,
						SubmittedTimestamp:  submittedTime,
						Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
							TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
								ExpiryIntervalMs: 0,
								Errors:           []string{"CEL evaluation error: no such key: not-existing-attr"},
							},
						},
					},
				},
				{
					Key: &topo_stream_v1.TopologyStreamMessageKey{
						Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
						DataSource: "mapping3b",
						ShardId:    "unknown",
					},
					Message: &topo_stream_v1.TopologyStreamMessage{
						CollectionTimestamp: now,
						SubmittedTimestamp:  submittedTime,
						Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
							TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
								ExpiryIntervalMs: 0,
								Errors:           []string{"CEL evaluation error: no such key: not-existing-attr"},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval, _ := NewCELEvaluator(CacheSettings{Size: 100, TTL: 30 * time.Second})
			result := ConvertSpanToTopologyStreamMessage(eval, traces, tt.componentMappings, tt.relationMappings, now)
			unify(&result)
			//nolint:gosec
			unify(&tt.expected)
			assert.Equal(t, tt.expected, result)
		})
	}

	t.Run("Convert one span to multiple components and relations", func(t *testing.T) {
		componentMappings := []settings.OtelComponentMapping{
			createSimpleComponentMapping("cm1"),
			createSimpleComponentMapping("cm2"),
		}
		relationMappings := []settings.OtelRelationMapping{
			createSimpleRelationMapping("rm1"),
			createSimpleRelationMapping("rm2"),
		}
		eval, _ := NewCELEvaluator(CacheSettings{Size: 100, TTL: 30 * time.Second})
		result := ConvertSpanToTopologyStreamMessage(eval, traces, componentMappings, relationMappings, now)
		actualKeys := make([]topo_stream_v1.TopologyStreamMessageKey, 0)
		for _, message := range result {
			//nolint:govet
			actualKeys = append(actualKeys, *message.Key)
		}
		expectedKeys := []topo_stream_v1.TopologyStreamMessageKey{
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "cm1",
				ShardId:    "627cc493",
			},
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "cm2",
				ShardId:    "627cc493",
			},
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "rm1",
				ShardId:    "checkout-service-web-service",
			},
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "rm2",
				ShardId:    "checkout-service-web-service",
			},
		}
		assert.Equal(t, expectedKeys, actualKeys)
	})

	t.Run("Convert a trace with two span to multiple components and relations", func(t *testing.T) {
		traceWithSpans := ptrace.NewTraces()
		rsWithSpans := traceWithSpans.ResourceSpans().AppendEmpty()
		rsWithSpans.Resource().Attributes().PutStr("service.name", "checkout-service")
		rsWithSpans.Resource().Attributes().PutStr("service.instance.id", "627cc493")
		rsWithSpans.Resource().Attributes().PutStr("service.namespace", "shop")
		rsWithSpans.Resource().Attributes().PutStr("service.version", "1.2.3")
		rsWithSpans.Resource().Attributes().PutStr("host.name", "ip-10-1-2-3.ec2.internal")
		rsWithSpans.Resource().Attributes().PutStr("os.type", "linux")
		rsWithSpans.Resource().Attributes().PutStr("process.pid", "12345")
		rsWithSpans.Resource().Attributes().PutStr("cloud.provider", "aws")
		rsWithSpans.Resource().Attributes().PutStr("k8s.pod.name", "checkout-service-8675309")
		ssWithSpans := rsWithSpans.ScopeSpans().AppendEmpty()
		ssWithSpans.Scope().Attributes().PutStr("otel.scope.name", "io.opentelemetry.instrumentation.http")
		ssWithSpans.Scope().Attributes().PutStr("otel.scope.version", "1.17.0")
		span1 := ssWithSpans.Spans().AppendEmpty()
		//nolint:gosec
		span1.SetEndTimestamp(pcommon.Timestamp(submittedTime))
		span1.Attributes().PutStr("http.method", "GET")
		span1.Attributes().PutStr("http.status_code", "200")
		span1.Attributes().PutStr("service.name", "web-service")
		span2 := ssWithSpans.Spans().AppendEmpty()
		//nolint:gosec
		span2.SetEndTimestamp(pcommon.Timestamp(submittedTime))
		span2.Attributes().PutStr("http.method", "GET")
		span2.Attributes().PutStr("http.status_code", "200")
		span2.Attributes().PutStr("service.name", "payment-service")

		componentMappings := []settings.OtelComponentMapping{
			createSimpleComponentMapping("cm1"),
		}
		relationMappings := []settings.OtelRelationMapping{
			createSimpleRelationMapping("rm1"),
		}
		eval, _ := NewCELEvaluator(CacheSettings{Size: 100, TTL: 30 * time.Second})
		result := ConvertSpanToTopologyStreamMessage(eval, traceWithSpans, componentMappings, relationMappings, now)
		actualKeys := make([]topo_stream_v1.TopologyStreamMessageKey, 0)
		for _, message := range result {
			//nolint:govet
			actualKeys = append(actualKeys, *message.Key)
		}
		expectedKeys := []topo_stream_v1.TopologyStreamMessageKey{
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "cm1",
				ShardId:    "627cc493",
			},
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "cm1",
				ShardId:    "627cc493",
			},
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "rm1",
				ShardId:    "checkout-service-web-service",
			},
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "rm1",
				ShardId:    "checkout-service-payment-service",
			},
		}
		assert.Equal(t, expectedKeys, actualKeys)
	})
}

func createSimpleComponentMapping(id string) settings.OtelComponentMapping {
	return settings.OtelComponentMapping{
		Id:            id,
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
		ExpireAfterMs: 300000,
		Conditions: []settings.OtelConditionMapping{
			{Action: settings.CREATE, Expression: boolExpr(`spanAttributes["http.method"] == "GET"`)},
		},
		Output: settings.OtelRelationMappingOutput{
			SourceId: strExpr(`${resourceAttributes["service.name"]}`),
			TargetId: strExpr(`${spanAttributes["service.name"]}`),
			TypeName: strExpr(`http-request`),
		},
	}
}

func unify(data *[]MessageWithKey) {
	for _, message := range *data {
		//nolint:forcetypeassert
		for _, component := range message.Message.Payload.(*topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData).TopologyStreamRepeatElementsData.Components {
			sort.Strings(component.Tags)
		}
	}
}
