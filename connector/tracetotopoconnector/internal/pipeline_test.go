//nolint:testpackage
package internal

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
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

func (n *noopMetrics) IncSpansProcessed(_ context.Context, _ int64)   {}
func (n *noopMetrics) IncMetricsProcessed(_ context.Context, _ int64) {}
func (n *noopMetrics) IncComponentsProduced(_ context.Context, _ int64, _ settings.SettingType, _ ...attribute.KeyValue) {
}
func (n *noopMetrics) IncComponentsRemoved(_ context.Context, _ int64, _ settings.SettingType) {}
func (n *noopMetrics) IncMappingErrors(_ context.Context, _ int64, _ settings.SettingType)   {}
func (n *noopMetrics) RecordMappingDuration(_ context.Context, _ time.Duration, _ ...attribute.KeyValue) {
}

func TestPipeline_ConvertSpanToTopologyStreamMessage(t *testing.T) {

	collectionTimestampMs := time.Now().UnixMilli()
	submittedTime := int64(1756851083000)
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
	_ = rs.Resource().Attributes().PutEmptySlice("process.command_args").FromRaw([]any{"ls", "-la", "/home"})
	ss := rs.ScopeSpans().AppendEmpty()
	_ = ss.Scope().Attributes().FromRaw(map[string]any{"otel.scope.name": "io.opentelemetry.instrumentation.http", "otel.scope.version": "1.17.0"})
	span := ss.Spans().AppendEmpty()
	//nolint:gosec
	span.SetEndTimestamp(pcommon.Timestamp(submittedTime))
	_ = span.Attributes().FromRaw(map[string]any{
		"http.method":      "GET",
		"http.status_code": "200",
		"db.system":        "postgresql",
		"db.statement":     "SELECT * FROM users WHERE id = 123",
		"net.peer.name":    "api.example.com",
		"user.id":          "123",
		"service.name":     "web-service",
	})

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
		_ = rsWithSpans.Resource().Attributes().FromRaw(map[string]any{
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
		ssWithSpans := rsWithSpans.ScopeSpans().AppendEmpty()
		_ = ssWithSpans.Scope().Attributes().FromRaw(map[string]any{"otel.scope.name": "io.opentelemetry.instrumentation.http", "otel.scope.version": "1.17.0"})
		span1 := ssWithSpans.Spans().AppendEmpty()
		//nolint:gosec
		span1.SetEndTimestamp(pcommon.Timestamp(submittedTime))
		_ = span1.Attributes().FromRaw(map[string]any{
			"http.method":      "GET",
			"http.status_code": "200",
			"service.name":     "web-service",
		})
		span2 := ssWithSpans.Spans().AppendEmpty()
		//nolint:gosec
		span2.SetEndTimestamp(pcommon.Timestamp(submittedTime))
		_ = span2.Attributes().FromRaw(map[string]any{"http.method": "GET", "http.status_code": "200", "service.name": "payment-service"})

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

func TestPipeline_ConvertMappingRemovalsToTopologyStreamMessage(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	componentMappings := []settings.OtelComponentMapping{
		createSimpleComponentMapping("mapping1"),
		createSimpleComponentMapping("mapping2"),
	}
	relationMappings := []settings.OtelRelationMapping{
		createSimpleRelationMapping("mapping3"),
	}

	messages := ConvertMappingRemovalsToTopologyStreamMessage(
		ctx, logger, componentMappings, relationMappings, &noopMetrics{},
	)

	// Expect one message per mapping per shard
	expectedMessages := (len(componentMappings) + len(relationMappings)) * ShardCount
	require.Len(t, messages, expectedMessages)

	// Validate message content
	for _, msg := range messages {
		assert.NotNil(t, msg.Key)
		assert.NotNil(t, msg.Message)

		assert.Equal(t, topo_stream_v1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL, msg.Key.Owner)
		assert.Contains(t, msg.Key.DataSource, "mapping")

		// Validate the payload type
		payload := msg.Message.GetTopologyStreamRemove()
		require.NotNil(t, payload)
		assert.Contains(t, payload.RemovalCause, "was removed")

		// Validate timestamps are reasonable
		assert.Greater(t, msg.Message.SubmittedTimestamp, int64(0))
		assert.GreaterOrEqual(t, msg.Message.CollectionTimestamp, msg.Message.SubmittedTimestamp)
	}

	// Validate shard IDs cover all expected shards
	seenShards := map[string]bool{}
	for _, msg := range messages {
		seenShards[msg.Key.ShardId] = true
	}
	for i := 0; i < ShardCount; i++ {
		assert.True(t, seenShards[fmt.Sprintf("%d", i)], "expected shard %d to be present", i)
	}
}

func TestPipeline_ConvertMetricsToTopologyStreamMessage(t *testing.T) {

	collectionTimestampMs := time.Now().UnixMilli()
	submittedTime := int64(1756851083000)

	newTestMetrics := func(addMetric func(m pmetric.Metric)) pmetric.Metrics {
		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		_ = rm.Resource().Attributes().FromRaw(map[string]any{
			"service.name":        "checkout-service",
			"service.instance.id": "627cc493",
			"service.namespace":   "shop",
			"k8s.pod.name":        "checkout-service-8675309",
		})
		sm := rm.ScopeMetrics().AppendEmpty()
		_ = sm.Scope().Attributes().FromRaw(map[string]any{"otel.scope.name": "io.opentelemetry.instrumentation.http"})
		m := sm.Metrics().AppendEmpty()
		addMetric(m)
		return metrics
	}

	tests := []struct {
		name    string
		metrics pmetric.Metrics
	}{
		{name: "sumMetrics",
			metrics: newTestMetrics(func(m pmetric.Metric) {
				m.SetName("http.server.duration")
				sum := m.SetEmptySum()
				dp := sum.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.Timestamp(submittedTime))
				_ = dp.Attributes().FromRaw(map[string]any{
					"http.method":      "GET",
					"http.status_code": "200",
					"net.peer.name":    "api.example.com",
					"service.name":     "web-service"}) // This is a destination service
			})}, {
			name: "gaugeMetrics",
			metrics: newTestMetrics(func(m pmetric.Metric) {
				m.SetName("http.server.active_requests")
				gauge := m.SetEmptyGauge()
				dp := gauge.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.Timestamp(submittedTime))
				dp.SetIntValue(10)
				_ = dp.Attributes().FromRaw(map[string]any{
					"http.method":      "GET",
					"http.status_code": "200",
					"net.peer.name":    "api.example.com",
					"service.name":     "web-service"})
			})},
		{
			name: "histogramMetrics",
			metrics: newTestMetrics(func(m pmetric.Metric) {
				m.SetName("http.server.duration.histogram")
				histogram := m.SetEmptyHistogram()
				dp := histogram.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.Timestamp(submittedTime))
				dp.SetCount(2)
				dp.SetSum(10)
				dp.BucketCounts().FromRaw([]uint64{1, 1})
				dp.ExplicitBounds().FromRaw([]float64{10})
				_ = dp.Attributes().FromRaw(map[string]any{
					"http.method":      "GET",
					"http.status_code": "200",
					"net.peer.name":    "api.example.com",
					"service.name":     "web-service"})
			})}, {
			name: "exponentialHistogramMetrics",

			metrics: newTestMetrics(func(m pmetric.Metric) {
				m.SetName("http.server.duration.exp_histogram")
				expHistogram := m.SetEmptyExponentialHistogram()
				dp := expHistogram.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.Timestamp(submittedTime))
				dp.SetCount(2)
				dp.SetSum(10)
				dp.SetScale(1)
				dp.Positive().SetOffset(0)
				dp.Positive().BucketCounts().FromRaw([]uint64{1, 1})
				_ = dp.Attributes().FromRaw(map[string]any{
					"http.method":      "GET",
					"http.status_code": "200",
					"net.peer.name":    "api.example.com",
					"service.name":     "web-service"})
			})}, {
			name: "summaryMetrics",
			metrics: newTestMetrics(func(m pmetric.Metric) {
				m.SetName("http.server.duration.summary")
				summary := m.SetEmptySummary()
				dp := summary.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.Timestamp(submittedTime))
				dp.SetCount(2)
				dp.SetSum(10)
				qv := dp.QuantileValues().AppendEmpty()
				qv.SetQuantile(0.9)
				qv.SetValue(5)
				qv = dp.QuantileValues().AppendEmpty()
				qv.SetQuantile(0.99)
				qv.SetValue(8)
				_ = dp.Attributes().FromRaw(map[string]any{
					"http.method":      "GET",
					"http.status_code": "200",
					"net.peer.name":    "api.example.com",
					"service.name":     "web-service"})
			}),
		},
	}

	componentMappings := []settings.OtelComponentMapping{
		{
			Id:            "mapping1a",
			ExpireAfterMs: 60000,
			Conditions: []settings.OtelConditionMapping{
				{Action: settings.CREATE, Expression: boolExpr(`resourceAttributes["service.instance.id"] == "627cc493"`)},
			},
			Identifier: "urn:otel-component-mapping:service",
			Output: settings.OtelComponentMappingOutput{
				Identifier:     strExpr(`${resourceAttributes["service.instance.id"]}`),
				Name:           strExpr(`${resourceAttributes["service.name"]}`),
				TypeName:       strExpr(`service-instance`),
				TypeIdentifier: ptr(strExpr(`service_instance_id`)),
				DomainName:     strExpr(`${resourceAttributes["service.namespace"]}`),
				LayerName:      strExpr(`${metricAttributes["net.peer.name"]}`),
				Required: &settings.OtelComponentMappingFieldMapping{
					AdditionalIdentifiers: &[]settings.OtelStringExpression{
						{Expression: `${resourceAttributes["k8s.pod.name"]}`},
					},
				},
			},
		},
	}
	relationMappings := []settings.OtelRelationMapping{
		{
			Id:            "mapping1b",
			Identifier:    "urn:otel-relation-mapping:synchronous",
			ExpireAfterMs: 300000,
			Conditions: []settings.OtelConditionMapping{
				{Action: settings.CREATE, Expression: boolExpr(`metricAttributes["http.method"] == "GET"`)},
			},
			Output: settings.OtelRelationMappingOutput{
				SourceId: strExpr(`${resourceAttributes["service.name"]}`),
				TargetId: strExpr(`${metricAttributes["service.name"]}`),
				TypeName: strExpr("http-request"),
			},
		},
	}
	expected := []MessageWithKey{
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
								ExternalId:     "627cc493",
								Identifiers:    []string{"627cc493", "checkout-service-8675309"},
								Name:           "checkout-service",
								TypeName:       "service-instance",
								TypeIdentifier: ptr("service_instance_id"),
								DomainName:     "shop",
								LayerName:      "api.example.com",
								Tags:           nil,
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
	}

	mapper := NewMapper(context.Background(), makeMeteredCacheSettings(100, 30*time.Second), makeMeteredCacheSettings(100, 30*time.Second))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			metricsReporter := &noopMetrics{}
			eval, _ := NewCELEvaluator(ctx, makeMeteredCacheSettings(100, 30*time.Second))
			result := ConvertMetricsToTopologyStreamMessage(
				ctx,
				zaptest.NewLogger(t),
				eval,
				mapper,
				tt.metrics,
				componentMappings,
				relationMappings,
				collectionTimestampMs,
				metricsReporter,
			)
			unify(&result)
			unify(&expected)
			assert.Equal(t, expected, result)
		})
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
