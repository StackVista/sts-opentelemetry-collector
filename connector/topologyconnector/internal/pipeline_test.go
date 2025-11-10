//nolint:testpackage
package internal

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/testing/protocmp"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
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

func (n *noopMetrics) IncInputsProcessed(_ context.Context, _ int64, _ settings.OtelInputSignal) {}
func (n *noopMetrics) IncTopologyProduced(_ context.Context, _ int64, _ settings.SettingType, _ settings.OtelInputSignal) {
}
func (n *noopMetrics) IncMappingsRemoved(_ context.Context, _ int64, _ settings.SettingType) {}
func (n *noopMetrics) IncMappingErrors(_ context.Context, _ int64, _ settings.SettingType, _ settings.OtelInputSignal) {
}
func (n *noopMetrics) RecordMappingDuration(
	_ context.Context,
	_ time.Duration,
	_ settings.OtelInputSignal,
	_ settings.SettingType,
	_ string,
) {
}
func (n *noopMetrics) RecordRequestDuration(_ context.Context, _ time.Duration, _ settings.OtelInputSignal) {
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
	span.SetEndTimestamp(pcommon.Timestamp(int64(1756851083000)))
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
					Input: settings.OtelInput{
						Signal: settings.OtelInputSignalList{
							settings.TRACES,
						},
						Resource: settings.OtelInputResource{
							Condition: ptr(boolExpr(`resourceAttributes["service.instance.id"] == "627cc493"`)),
							Scope: &settings.OtelInputScope{
								Action: ptr(settings.CREATE),
							},
						},
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
					Input: settings.OtelInput{
						Signal: settings.OtelInputSignalList{
							settings.TRACES,
						},
						Resource: settings.OtelInputResource{
							Scope: &settings.OtelInputScope{
								Span: &settings.OtelInputSpan{
									Condition: ptr(boolExpr(`spanAttributes["http.method"] == "GET"`)),
									Action:    ptr(settings.CREATE),
								},
							},
						},
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
					Key: &topostreamv1.TopologyStreamMessageKey{
						Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
						DataSource: "urn:otel-component-mapping:service",
						ShardId:    "0",
					},
					Message: &topostreamv1.TopologyStreamMessage{
						CollectionTimestamp: collectionTimestampMs,
						SubmittedTimestamp:  time.Now().UnixMilli(),
						Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
							TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
								ExpiryIntervalMs: 60000,
								Components: []*topostreamv1.TopologyStreamComponent{
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
					Key: &topostreamv1.TopologyStreamMessageKey{
						Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
						DataSource: "urn:otel-relation-mapping:synchronous",
						ShardId:    "2",
					},
					Message: &topostreamv1.TopologyStreamMessage{
						CollectionTimestamp: collectionTimestampMs,
						SubmittedTimestamp:  time.Now().UnixMilli(),
						Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
							TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
								ExpiryIntervalMs: 300000,
								Relations: []*topostreamv1.TopologyStreamRelation{
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
			name: "Reject (not create) components with unmatched conditions",
			componentMappings: []settings.OtelComponentMapping{
				{
					Id: "mapping2",
					Input: settings.OtelInput{
						Signal: settings.OtelInputSignalList{
							settings.TRACES,
						},
						Resource: settings.OtelInputResource{
							Condition: ptr(boolExpr(`resourceAttributes["service.instance.id"] != "627cc493"`)),
							Action:    ptr(settings.CREATE),
						},
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
					Input: settings.OtelInput{
						Signal: settings.OtelInputSignalList{
							settings.TRACES,
						},
						Resource: settings.OtelInputResource{
							Scope: &settings.OtelInputScope{
								Span: &settings.OtelInputSpan{
									Condition: ptr(boolExpr(`spanAttributes["http.method"] == "WRONG"`)),
									Action:    ptr(settings.CREATE),
								},
							},
						},
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
					Input: settings.OtelInput{
						Signal: settings.OtelInputSignalList{
							settings.TRACES,
						},
						Resource: settings.OtelInputResource{
							Condition: ptr(boolExpr(`resourceAttributes["service.instance.id"] == "627cc493"`)),
							Action:    ptr(settings.CREATE),
						},
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
					Input: settings.OtelInput{
						Signal: settings.OtelInputSignalList{
							settings.TRACES,
						},
						Resource: settings.OtelInputResource{
							Scope: &settings.OtelInputScope{
								Span: &settings.OtelInputSpan{
									Condition: ptr(boolExpr(`spanAttributes["http.method"] == "GET"`)),
									Action:    ptr(settings.CREATE),
								},
							},
						},
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
					Key: &topostreamv1.TopologyStreamMessageKey{
						Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
						DataSource: "urn:otel-component-mapping:service",
						ShardId:    "unknown",
					},
					Message: &topostreamv1.TopologyStreamMessage{
						CollectionTimestamp: collectionTimestampMs,
						SubmittedTimestamp:  time.Now().UnixMilli(),
						Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
							TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
								ExpiryIntervalMs: 0,
								Errors: []*topostreamv1.TopoStreamError{
									{Message: "name: no such key: not-existing-attr"},
								},
							},
						},
					},
				},
				{
					Key: &topostreamv1.TopologyStreamMessageKey{
						Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
						DataSource: "urn:otel-relation-mapping:synchronous",
						ShardId:    "unknown",
					},
					Message: &topostreamv1.TopologyStreamMessage{
						CollectionTimestamp: collectionTimestampMs,
						SubmittedTimestamp:  time.Now().UnixMilli(),
						Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
							TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
								ExpiryIntervalMs: 0,
								Errors: []*topostreamv1.TopoStreamError{
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
					Input: settings.OtelInput{
						Signal: settings.OtelInputSignalList{
							settings.TRACES,
						},
						Resource: settings.OtelInputResource{
							Condition: ptr(boolExpr(`resourceAttributes["service.instance.id"] == "627cc493"`)),
							Action:    ptr(settings.CREATE),
						},
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
					Key: &topostreamv1.TopologyStreamMessageKey{
						Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
						DataSource: "urn:otel-component-mapping:service",
						ShardId:    "0",
					},
					Message: &topostreamv1.TopologyStreamMessage{
						CollectionTimestamp: collectionTimestampMs,
						SubmittedTimestamp:  time.Now().UnixMilli(),
						Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
							TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
								ExpiryIntervalMs: 60000,
								Components: []*topostreamv1.TopologyStreamComponent{
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
			unifyMessages(t, &result)
			//nolint:gosec
			unifyMessages(t, &tt.expected)
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
		actualKeys := make([]topostreamv1.TopologyStreamMessageKey, 0)
		for _, message := range result {
			//nolint:govet
			actualKeys = append(actualKeys, *message.Key)
		}
		expectedKeys := []topostreamv1.TopologyStreamMessageKey{
			{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-component-mapping:cm1",
				ShardId:    "0",
			},
			{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-component-mapping:cm2",
				ShardId:    "0",
			},
			{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-relation-mapping:rm1",
				ShardId:    "2",
			},
			{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
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
		actualKeys := make([]topostreamv1.TopologyStreamMessageKey, 0)
		for _, message := range result {
			//nolint:govet
			actualKeys = append(actualKeys, *message.Key)
		}
		expectedKeys := []topostreamv1.TopologyStreamMessageKey{
			{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-component-mapping:cm1",
				ShardId:    "0",
			},
			{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-component-mapping:cm1",
				ShardId:    "0",
			},
			{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-relation-mapping:rm1",
				ShardId:    "2",
			},
			{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
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

		assert.Equal(t, topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL, msg.Key.Owner)
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

func TestPipeline_ConvertMetricsToTopologyStreamMessage_MultipleComponents(t *testing.T) {
	collectionTimestampMs := time.Now().UnixMilli()
	submittedTime := int64(1756851083000)

	metrics := func() pmetric.Metrics {
		metrics := pmetric.NewMetrics()
		rs := metrics.ResourceMetrics().AppendEmpty()
		_ = rs.Resource().Attributes().FromRaw(map[string]any{
			"service.name":        "api-gateway",
			"service.instance.id": "instance-1",
			"service.namespace":   "default",
		})
		ss := rs.ScopeMetrics().AppendEmpty()
		_ = ss.Scope().Attributes().FromRaw(map[string]any{"otel.scope.name": "http"})
		m := ss.Metrics().AppendEmpty()
		m.SetName("http.server.request_count")
		sum := m.SetEmptySum()

		// Datapoint 1
		dp1 := sum.DataPoints().AppendEmpty()
		//nolint:gosec
		dp1.SetTimestamp(pcommon.Timestamp(submittedTime))
		_ = dp1.Attributes().FromRaw(map[string]any{
			"http.method":      "GET",
			"http.route":       "/users",
			"http.status_code": "200",
		})

		// Datapoint 2
		dp2 := sum.DataPoints().AppendEmpty()
		//nolint:gosec
		dp2.SetTimestamp(pcommon.Timestamp(submittedTime))
		_ = dp2.Attributes().FromRaw(map[string]any{
			"http.method":      "POST",
			"http.route":       "/products",
			"http.status_code": "201",
		})
		return metrics
	}()

	componentMappings := []settings.OtelComponentMapping{
		{
			Id:            "mapping-multi-dp-comp",
			ExpireAfterMs: 60000,
			Input: settings.OtelInput{
				Signal: settings.OtelInputSignalList{
					settings.TRACES,
				},
				Resource: settings.OtelInputResource{
					Condition: ptr(boolExpr(`resourceAttributes["service.name"] == "api-gateway"`)),
					Scope: &settings.OtelInputScope{
						Metric: &settings.OtelInputMetric{
							Datapoint: &settings.OtelInputDatapoint{
								Action: ptr(settings.CREATE),
							},
						},
					},
				},
			},
			Identifier: "urn:otel-component-mapping:api-route",
			Output: settings.OtelComponentMappingOutput{
				Identifier: strExpr(`api-gateway-${datapointAttributes["http.route"]}`),
				Name:       strExpr(`API Route: ${datapointAttributes["http.route"]}`),
				TypeName:   strExpr(`api-route`),
				DomainName: strExpr(`${resourceAttributes["service.namespace"]}`),
				LayerName:  strExpr("frontend"),
				Required: &settings.OtelComponentMappingFieldMapping{
					Tags: &[]settings.OtelTagMapping{
						{
							Source: anyExpr(`${datapointAttributes["http.method"]}`),
							Target: "http_method",
						},
					},
				},
			},
		},
	}
	relationMappings := []settings.OtelRelationMapping{}
	expected := []MessageWithKey{
		{
			Key: &topostreamv1.TopologyStreamMessageKey{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-component-mapping:api-route",
				ShardId:    stableShardID("api-gateway-/products", ShardCount),
			},
			Message: &topostreamv1.TopologyStreamMessage{
				CollectionTimestamp: collectionTimestampMs,
				SubmittedTimestamp:  time.Now().UnixMilli(),
				Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
					TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
						ExpiryIntervalMs: 60000,
						Components: []*topostreamv1.TopologyStreamComponent{
							{
								ExternalId:  "api-gateway-/products",
								Identifiers: []string{"api-gateway-/products"},
								Name:        "API Route: /products",
								TypeName:    "api-route",
								DomainName:  "default",
								LayerName:   "frontend",
								Tags:        []string{"http_method:POST"},
							},
						},
					},
				},
			},
		},
		{
			Key: &topostreamv1.TopologyStreamMessageKey{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-component-mapping:api-route",
				ShardId:    stableShardID("api-gateway-/users", ShardCount),
			},
			Message: &topostreamv1.TopologyStreamMessage{
				CollectionTimestamp: collectionTimestampMs,
				SubmittedTimestamp:  time.Now().UnixMilli(),
				Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
					TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
						ExpiryIntervalMs: 60000,
						Components: []*topostreamv1.TopologyStreamComponent{
							{
								ExternalId:  "api-gateway-/users",
								Identifiers: []string{"api-gateway-/users"},
								Name:        "API Route: /users",
								TypeName:    "api-route",
								DomainName:  "default",
								LayerName:   "frontend",
								Tags:        []string{"http_method:GET"},
							},
						},
					},
				},
			},
		},
	}

	mapper := NewMapper(context.Background(), makeMeteredCacheSettings(100, 30*time.Second), makeMeteredCacheSettings(100, 30*time.Second))
	ctx := context.Background()
	metricsReporter := &noopMetrics{}
	eval, _ := NewCELEvaluator(ctx, makeMeteredCacheSettings(100, 30*time.Second))
	result := ConvertMetricsToTopologyStreamMessage(
		ctx,
		zaptest.NewLogger(t),
		eval,
		mapper,
		metrics,
		componentMappings,
		relationMappings,
		collectionTimestampMs,
		metricsReporter,
	)
	// Unify to handle unpredictable map and slice ordering
	unifyMessages(t, &result)
	unifyMessages(t, &expected)
	assert.Equal(t, expected, result)
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
				//nolint:gosec
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
				//nolint:gosec
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
				//nolint:gosec
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
				//nolint:gosec
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
				//nolint:gosec
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
			Input: settings.OtelInput{
				Signal: settings.OtelInputSignalList{
					settings.METRICS,
				},
				Resource: settings.OtelInputResource{
					Condition: ptr(boolExpr(`resourceAttributes["service.instance.id"] == "627cc493"`)),
					Scope: &settings.OtelInputScope{
						Metric: &settings.OtelInputMetric{
							Datapoint: &settings.OtelInputDatapoint{
								Action: ptr(settings.CREATE),
							},
						},
					},
				},
			},
			Identifier: "urn:otel-component-mapping:service",
			Output: settings.OtelComponentMappingOutput{
				Identifier:     strExpr(`${resourceAttributes["service.instance.id"]}`),
				Name:           strExpr(`${resourceAttributes["service.name"]}`),
				TypeName:       strExpr(`service-instance`),
				TypeIdentifier: ptr(strExpr(`service_instance_id`)),
				DomainName:     strExpr(`${resourceAttributes["service.namespace"]}`),
				LayerName:      strExpr(`${datapointAttributes["net.peer.name"]}`),
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
			Input: settings.OtelInput{
				Signal: settings.OtelInputSignalList{
					settings.METRICS,
				},
				Resource: settings.OtelInputResource{
					Scope: &settings.OtelInputScope{
						Metric: &settings.OtelInputMetric{
							Datapoint: &settings.OtelInputDatapoint{
								Condition: ptr(boolExpr(`datapointAttributes["http.method"] == "GET"`)),
								Action:    ptr(settings.CREATE),
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
		},
	}
	expected := []MessageWithKey{
		{
			Key: &topostreamv1.TopologyStreamMessageKey{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-component-mapping:service",
				ShardId:    "0",
			},
			Message: &topostreamv1.TopologyStreamMessage{
				CollectionTimestamp: collectionTimestampMs,
				SubmittedTimestamp:  time.Now().UnixMilli(),
				Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
					TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
						ExpiryIntervalMs: 60000,
						Components: []*topostreamv1.TopologyStreamComponent{
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
			Key: &topostreamv1.TopologyStreamMessageKey{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: "urn:otel-relation-mapping:synchronous",
				ShardId:    "2",
			},
			Message: &topostreamv1.TopologyStreamMessage{
				CollectionTimestamp: collectionTimestampMs,
				SubmittedTimestamp:  time.Now().UnixMilli(),
				Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
					TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
						ExpiryIntervalMs: 300000,
						Relations: []*topostreamv1.TopologyStreamRelation{
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

	unifyMessages(t, &expected) // only need to do this once
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
			unifyMessages(t, &result)
			diff := cmp.Diff(expected, result, protocmp.Transform())
			if diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func createSimpleComponentMapping(id string) settings.OtelComponentMapping {
	return settings.OtelComponentMapping{
		Id:            id,
		Identifier:    fmt.Sprintf("urn:otel-component-mapping:%s", id),
		ExpireAfterMs: 60000,
		Input: settings.OtelInput{
			Signal: settings.OtelInputSignalList{
				settings.TRACES,
			},
			Resource: settings.OtelInputResource{
				Scope: &settings.OtelInputScope{
					Span: &settings.OtelInputSpan{
						Condition: ptr(boolExpr(`spanAttributes["http.method"] == "GET"`)),
						Action:    ptr(settings.CREATE),
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
		},
	}
}

func createSimpleRelationMapping(id string) settings.OtelRelationMapping {
	return settings.OtelRelationMapping{
		Id:            id,
		Identifier:    fmt.Sprintf("urn:otel-relation-mapping:%s", id),
		ExpireAfterMs: 300000,
		Input: settings.OtelInput{
			Signal: settings.OtelInputSignalList{
				settings.TRACES,
			},
			Resource: settings.OtelInputResource{
				Scope: &settings.OtelInputScope{
					Span: &settings.OtelInputSpan{
						Condition: ptr(boolExpr(`spanAttributes["http.method"] == "GET"`)),
						Action:    ptr(settings.CREATE),
					},
				},
			},
		},
		Output: settings.OtelRelationMappingOutput{
			SourceId: strExpr(`${resourceAttributes["service.name"]}`),
			TargetId: strExpr(`${spanAttributes["service.name"]}`),
			TypeName: strExpr(`http-request`),
		},
	}
}

func unifyMessages(t *testing.T, data *[]MessageWithKey) {
	t.Helper()

	for _, message := range *data {
		if pl := message.Message.GetTopologyStreamRepeatElementsData(); pl != nil {
			if message.Message != nil && message.Message.SubmittedTimestamp == 0 {
				t.Errorf("expected SubmittedTimestamp to be set for message: %+v", message)
			}
			message.Message.SubmittedTimestamp = 0

			for _, component := range pl.Components {
				if component != nil {
					sort.Strings(component.Tags)
					sort.Strings(component.Identifiers)
				}
			}
			for _, relation := range pl.Relations {
				if relation != nil && relation.Name == "" {
					relation.Name = "" // Ensure empty is not nil for comparison
				}
			}
		}
	}
}
