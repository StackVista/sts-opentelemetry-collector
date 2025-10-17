//nolint:testpackage
package internal

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap/zaptest"

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

type noopMetrics struct{}

func (n *noopMetrics) IncSpansProcessed(_ context.Context, _ int64)                              {}
func (n *noopMetrics) IncComponentsProduced(_ context.Context, _ int64, _ ...attribute.KeyValue) {}
func (n *noopMetrics) IncRelationsProduced(_ context.Context, _ int64, _ ...attribute.KeyValue)  {}
func (n *noopMetrics) IncErrors(_ context.Context, _ int64, _ string)                            {}
func (n *noopMetrics) RecordMappingDuration(_ context.Context, _ time.Duration, _ ...attribute.KeyValue) {
}

func TestPipeline_ConvertSpanToTopologyStreamMessage(t *testing.T) {

	collectionTimestampMs := time.Now().UnixMilli()
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
	_ = rs.Resource().Attributes().PutEmptySlice("process.command_args").FromRaw([]any{"ls", "-la", "/home"})
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
					Identifier: "urn:otel-component-mapping:service",
					Output: settings.OtelComponentMappingOutput{
						Identifier:       strExpr("${vars.instanceId}"),
						Name:             strExpr(`${resourceAttributes["service.name"]}`),
						TypeName:         strExpr(`service-instance`),
						TypeIdentifier:   ptr(strExpr(`service_instance_id`)),
						DomainName:       strExpr(`${resourceAttributes["service.namespace"]}`),
						DomainIdentifier: ptr(strExpr(`${resourceAttributes["missing"]}`)), // Optional field, so no error on this
						LayerName:        strExpr("backend"),
						Required: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `${resourceAttributes["k8s.pod.name"]}`},
							},
							Tags: &[]settings.OtelTagMapping{
								{
									Source: anyExpr(`${resourceAttributes["host.name"]}`),
									Target: "host",
								},
							},
						},
						Optional: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `urn:process:${resourceAttributes["process.pid"]}`},
							},
							Tags: &[]settings.OtelTagMapping{
								{
									Source: anyExpr(`${scopeAttributes["otel.scope.name"]}`),
									Target: "instrumentation-lib",
								},
								{
									Source: anyExpr(`${scopeAttributes["otel.scope.version"]}`),
									Target: "instrumentation-version",
								},
								{
									// this attribute doesn't exist expr the span but it is Optional tag so it ignored by mapping
									Source: anyExpr(`${scopeAttributes["otel.scope.provider"]}`),
									Target: "instrumentation-provider",
								},
							},
						},
					},
					Vars: &[]settings.OtelVariableMapping{
						{
							Name:  "instanceId",
							Value: anyExpr(`${resourceAttributes["service.instance.id"]}`),
						},
					},
				},
			},
			relationMappings: []settings.OtelRelationMapping{
				{
					Id:            "mapping1b",
					Identifier:    "urn:otel-relation-mapping:synchronous",
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
						DataSource: "urn:otel-component-mapping:service",
						ShardId:    "0",
					},
					Message: &topo_stream_v1.TopologyStreamMessage{
						CollectionTimestamp: collectionTimestampMs,
						SubmittedTimestamp:  submittedTime,
						Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
							TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
								ExpiryIntervalMs: 60000,
								Components: []*topo_stream_v1.TopologyStreamComponent{
									{
										ExternalId:       "627cc493",
										Identifiers:      []string{"627cc493", "urn:process:12345", "checkout-service-8675309"},
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
						DataSource: "urn:otel-relation-mapping:synchronous",
						ShardId:    "2",
					},
					Message: &topo_stream_v1.TopologyStreamMessage{
						CollectionTimestamp: collectionTimestampMs,
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
							Tags: &[]settings.OtelTagMapping{
								{
									Source: anyExpr(`${resourceAttributes["host.name"]}`),
									Target: "host",
								},
							},
						},
						Optional: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `${resourceAttributes["service.instance.id"]}`},
							},
							Tags: &[]settings.OtelTagMapping{
								{
									Source: anyExpr(`${scopeAttributes["otel.scope.name"]}`),
									Target: "instrumentation-lib",
								},
								{
									Source: anyExpr(`${scopeAttributes["otel.scope.version"]}`),
									Target: "instrumentation-version",
								},
								{
									Source: anyExpr(`${scopeAttributes["otel.scope.provider"]}`),
									Target: "instrumentation-provider",
								},
							},
						},
					},
					Vars: &[]settings.OtelVariableMapping{
						{
							Name:  "instanceId",
							Value: anyExpr(`${resourceAttributes["service.instance.id"]}`),
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
					Id:         "mapping3a",
					Identifier: "urn:otel-component-mapping:service",
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
							Tags: &[]settings.OtelTagMapping{
								{
									Source: anyExpr(`${resourceAttributes["host.name"]}`),
									Target: "host",
								},
							},
						},
						Optional: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `${resourceAttributes["service.instance.id"]}`},
							},
							Tags: &[]settings.OtelTagMapping{
								{
									Source: anyExpr(`${scopeAttributes["otel.scope.name"]}`),
									Target: "instrumentation-lib",
								},
								{
									Source: anyExpr(`${scopeAttributes["otel.scope.version"]}`),
									Target: "instrumentation-version",
								},
								{
									// this attribute doesn't exist, but it is an Optional tag, so it is ignored by mapping
									Source: anyExpr(`${scopeAttributes["otel.scope.provider"]}`),
									Target: "instrumentation-provider",
								},
							},
						},
					},
					Vars: &[]settings.OtelVariableMapping{
						{
							Name:  "instanceId",
							Value: anyExpr(`${resourceAttributes["service.instance.id"]}`),
						},
					},
				},
			},
			relationMappings: []settings.OtelRelationMapping{
				{
					Id:         "mapping3b",
					Identifier: "urn:otel-relation-mapping:synchronous",
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
						DataSource: "urn:otel-component-mapping:service",
						ShardId:    "unknown",
					},
					Message: &topo_stream_v1.TopologyStreamMessage{
						CollectionTimestamp: collectionTimestampMs,
						SubmittedTimestamp:  submittedTime,
						Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
							TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
								ExpiryIntervalMs: 0,
								Errors: []*topo_stream_v1.TopoStreamError{
									{Message: "name: no such key: not-existing-attr"},
								},
							},
						},
					},
				},
				{
					Key: &topo_stream_v1.TopologyStreamMessageKey{
						Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
						DataSource: "urn:otel-relation-mapping:synchronous",
						ShardId:    "unknown",
					},
					Message: &topo_stream_v1.TopologyStreamMessage{
						CollectionTimestamp: collectionTimestampMs,
						SubmittedTimestamp:  submittedTime,
						Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
							TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
								ExpiryIntervalMs: 0,
								Errors: []*topo_stream_v1.TopoStreamError{
									{Message: "sourceId: no such key: not-existing-attr"},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Convert traces to Component with complex variables usage",
			componentMappings: []settings.OtelComponentMapping{
				{
					Id:            "mapping1a",
					ExpireAfterMs: 60000,
					Conditions: []settings.OtelConditionMapping{
						{Action: settings.CREATE, Expression: boolExpr(`vars.isInstanceId`)},
					},
					Identifier: "urn:otel-component-mapping:service",
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
							Tags: &[]settings.OtelTagMapping{
								{
									Source: anyExpr(`${resourceAttributes["host.name"]}`),
									Target: "host",
								},
								{
									Source: anyExpr(`${vars.arguments[0]}`),
									Target: "main_command",
								},
							},
						},
					},
					Vars: &[]settings.OtelVariableMapping{
						{
							Name:  "isInstanceId",
							Value: anyExpr(`${resourceAttributes["service.instance.id"] == "627cc493"}`),
						},
						{
							Name:  "instanceId",
							Value: anyExpr(`${resourceAttributes["service.instance.id"]}`),
						},
						{
							Name:  "arguments",
							Value: anyExpr(`${resourceAttributes["process.command_args"]}`),
						},
					},
				},
			},
			expected: []MessageWithKey{
				{
					Key: &topo_stream_v1.TopologyStreamMessageKey{
						Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
						DataSource: "urn:otel-component-mapping:service",
						ShardId:    "0",
					},
					Message: &topo_stream_v1.TopologyStreamMessage{
						CollectionTimestamp: collectionTimestampMs,
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
											"host:ip-10-1-2-3.ec2.internal",
											"main_command:ls",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	mapper := NewMapper(context.Background(), makeMeteredCacheSettings(100, 30*time.Second), makeMeteredCacheSettings(100, 30*time.Second))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			metrics := &noopMetrics{}
			eval, _ := NewCELEvaluator(ctx, makeMeteredCacheSettings(100, 30*time.Second))
			result := ConvertSpanToTopologyStreamMessage(
				ctx,
				zaptest.NewLogger(t),
				eval,
				mapper,
				traces,
				tt.componentMappings,
				tt.relationMappings,
				collectionTimestampMs,
				metrics,
			)
			unify(&result)
			//nolint:gosec
			unify(&tt.expected)
			assert.Equal(t, tt.expected, result)
		})
	}

	t.Run("Convert one span to multiple components and relations", func(t *testing.T) {
		ctx := context.Background()
		metrics := &noopMetrics{}
		componentMappings := []settings.OtelComponentMapping{
			createSimpleComponentMapping("cm1"),
			createSimpleComponentMapping("cm2"),
		}
		relationMappings := []settings.OtelRelationMapping{
			createSimpleRelationMapping("rm1"),
			createSimpleRelationMapping("rm2"),
		}
		eval, _ := NewCELEvaluator(ctx, makeMeteredCacheSettings(100, 30*time.Second))
		mapper := NewMapper(context.Background(), makeMeteredCacheSettings(100, 30*time.Second), makeMeteredCacheSettings(100, 30*time.Second))
		result := ConvertSpanToTopologyStreamMessage(
			ctx,
			zaptest.NewLogger(t),
			eval,
			mapper,
			traces,
			componentMappings,
			relationMappings,
			collectionTimestampMs,
			metrics,
		)
		actualKeys := make([]topo_stream_v1.TopologyStreamMessageKey, 0)
		for _, message := range result {
			//nolint:govet
			actualKeys = append(actualKeys, *message.Key)
		}
		expectedKeys := []topo_stream_v1.TopologyStreamMessageKey{
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-component-mapping:cm1",
				ShardId:    "0",
			},
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-component-mapping:cm2",
				ShardId:    "0",
			},
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-relation-mapping:rm1",
				ShardId:    "2",
			},
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-relation-mapping:rm2",
				ShardId:    "2",
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

		ctx := context.Background()
		metrics := &noopMetrics{}
		componentMappings := []settings.OtelComponentMapping{
			createSimpleComponentMapping("cm1"),
		}
		relationMappings := []settings.OtelRelationMapping{
			createSimpleRelationMapping("rm1"),
		}
		eval, _ := NewCELEvaluator(ctx, makeMeteredCacheSettings(100, 30*time.Second))
		mapper := NewMapper(context.Background(), makeMeteredCacheSettings(100, 30*time.Second), makeMeteredCacheSettings(100, 30*time.Second))
		result := ConvertSpanToTopologyStreamMessage(
			ctx,
			zaptest.NewLogger(t),
			eval,
			mapper,
			traceWithSpans,
			componentMappings,
			relationMappings,
			collectionTimestampMs,
			metrics,
		)
		actualKeys := make([]topo_stream_v1.TopologyStreamMessageKey, 0)
		for _, message := range result {
			//nolint:govet
			actualKeys = append(actualKeys, *message.Key)
		}
		expectedKeys := []topo_stream_v1.TopologyStreamMessageKey{
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-component-mapping:cm1",
				ShardId:    "0",
			},
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-relation-mapping:rm1",
				ShardId:    "2",
			},
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-component-mapping:cm1",
				ShardId:    "0",
			},
			{
				Owner:      topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-relation-mapping:rm1",
				ShardId:    "2",
			},
		}
		assert.Equal(t, expectedKeys, actualKeys)
	})
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
