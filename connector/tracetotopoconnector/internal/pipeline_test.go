package internal

import (
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"sort"
	"testing"
	"time"

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

	now := time.Now().UnixNano()
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
	span.Attributes().PutStr("http.method", "GET")
	span.Attributes().PutStr("http.status_code", "200")
	span.Attributes().PutStr("db.system", "postgresql")
	span.Attributes().PutStr("db.statement", "SELECT * FROM users WHERE id = 123")
	span.Attributes().PutStr("net.peer.name", "api.example.com")
	span.Attributes().PutStr("user.id", "123")
	span.Attributes().PutStr("service.name", "web-service")

	tests := []struct {
		name              string
		componentMappings []settings.OtelComponentMapping
		relationMappings  []settings.OtelRelationMapping
		expected          topo_stream_v1.TopologyStreamMessage
	}{
		{
			name: "Convert traces to Components and Relation",
			componentMappings: []settings.OtelComponentMapping{
				{
					Conditions: []settings.OtelConditionMapping{
						{Action: settings.CREATE, Expression: boolExpr(`vars.instanceId == "627cc493"`)},
					},
					Output: settings.OtelComponentMappingOutput{
						Identifier:     strExpr("vars.instanceId"),
						Name:           strExpr(`resourceAttributes["service.name"]`),
						TypeName:       strExpr(`"service-instance"`),
						TypeIdentifier: ptr(strExpr(`"service_instance_id"`)),
						DomainName:     strExpr(`resourceAttributes["service.namespace"]`),
						LayerName:      strExpr(`"backend"`),
						Required: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `resourceAttributes["k8s.pod.name"]`},
							},
							Tags: &map[string]settings.OtelStringExpression{
								"host": {`resourceAttributes["host.name"]`},
							},
						},
						Optional: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `"resourceAttributes["service.instance.id"]`},
							},
							Tags: &map[string]settings.OtelStringExpression{
								"instrumentation-lib":      {`scopeAttributes["otel.scope.name"]`},
								"instrumentation-version":  {`scopeAttributes["otel.scope.version"]`},
								"instrumentation-provider": {`scopeAttributes["otel.scope.provider"]`}, //this attribute doesn't exist in the span but it is Optional tag so it ignored by mapping
							},
						},
					},
					Vars: &[]settings.OtelVariableMapping{
						{
							Name:  "instanceId",
							Value: strExpr(`resourceAttributes["service.instance.id"]`),
						},
					},
				},
			},
			relationMappings: []settings.OtelRelationMapping{
				{
					Conditions: []settings.OtelConditionMapping{
						{Action: settings.CREATE, Expression: boolExpr(`spanAttributes["http.method"] == "GET"`)},
					},
					Output: settings.OtelRelationMappingOutput{
						SourceId: strExpr(`resourceAttributes["service.name"]`),
						TargetId: strExpr(`spanAttributes["service.name"]`),
						TypeName: strExpr(`"http-request"`),
					},
				},
			},
			expected: topo_stream_v1.TopologyStreamMessage{
				CollectionTimestamp: now,
				SubmittedTimestamp:  now,
				Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
					TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
						ExpiryIntervalMs: 0,
						Components: []*topo_stream_v1.TopologyStreamComponent{
							{
								ExternalId:       "627cc493",
								Identifiers:      []string{"checkout-service-8675309"},
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
						Errors: []string{},
					},
				},
			},
		},
		{
			name: "Reject components with unmatched conditions",
			componentMappings: []settings.OtelComponentMapping{
				{
					Conditions: []settings.OtelConditionMapping{
						{Action: settings.REJECT, Expression: boolExpr(`vars.instanceId == "627cc493"`)},
					},
					Output: settings.OtelComponentMappingOutput{
						Identifier:     strExpr("vars.instanceId"),
						Name:           strExpr(`resourceAttributes["service.name"]`),
						TypeName:       strExpr(`"service-instance"`),
						TypeIdentifier: ptr(strExpr(`"service_instance_id"`)),
						DomainName:     strExpr(`resourceAttributes["service.namespace"]`),
						LayerName:      strExpr(`"backend"`),
						Required: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `resourceAttributes["k8s.pod.name"]`},
							},
							Tags: &map[string]settings.OtelStringExpression{
								"host": {`resourceAttributes["host.name"]`},
							},
						},
						Optional: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `"resourceAttributes["service.instance.id"]`},
							},
							Tags: &map[string]settings.OtelStringExpression{
								"instrumentation-lib":      {`scopeAttributes["otel.scope.name"]`},
								"instrumentation-version":  {`scopeAttributes["otel.scope.version"]`},
								"instrumentation-provider": {`scopeAttributes["otel.scope.provider"]`},
							},
						},
					},
					Vars: &[]settings.OtelVariableMapping{
						{
							Name:  "instanceId",
							Value: strExpr(`resourceAttributes["service.instance.id"]`),
						},
					},
				},
			},
			relationMappings: []settings.OtelRelationMapping{
				{
					Conditions: []settings.OtelConditionMapping{
						{Action: settings.REJECT, Expression: boolExpr(`spanAttributes["http.method"] == "GET"`)},
					},
					Output: settings.OtelRelationMappingOutput{
						SourceId: strExpr(`resourceAttributes["service.name"]`),
						TargetId: strExpr(`spanAttributes["service.name"]`),
						TypeName: strExpr(`"http-request"`),
					},
				},
			},
			expected: topo_stream_v1.TopologyStreamMessage{
				CollectionTimestamp: now,
				SubmittedTimestamp:  now,
				Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
					TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
						ExpiryIntervalMs: 0,
						Components:       []*topo_stream_v1.TopologyStreamComponent{},
						Relations:        []*topo_stream_v1.TopologyStreamRelation{},
						Errors:           []string{},
					},
				},
			},
		},
		{
			name: "Returns error when an attribute is missing",
			componentMappings: []settings.OtelComponentMapping{
				{
					Conditions: []settings.OtelConditionMapping{
						{Action: settings.CREATE, Expression: boolExpr(`vars.instanceId == "627cc493"`)},
					},
					Output: settings.OtelComponentMappingOutput{
						Identifier:     strExpr("vars.instanceId"),
						Name:           strExpr(`resourceAttributes["not-existing-attr"]`),
						TypeName:       strExpr(`"service-instance"`),
						TypeIdentifier: ptr(strExpr(`"service_instance_id"`)),
						DomainName:     strExpr(`resourceAttributes["service.namespace"]`),
						LayerName:      strExpr(`"backend"`),
						Required: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `resourceAttributes["k8s.pod.name"]`},
							},
							Tags: &map[string]settings.OtelStringExpression{
								"host": {`resourceAttributes["host.name"]`},
							},
						},
						Optional: &settings.OtelComponentMappingFieldMapping{
							AdditionalIdentifiers: &[]settings.OtelStringExpression{
								{Expression: `"resourceAttributes["service.instance.id"]`},
							},
							Tags: &map[string]settings.OtelStringExpression{
								"instrumentation-lib":      {`scopeAttributes["otel.scope.name"]`},
								"instrumentation-version":  {`scopeAttributes["otel.scope.version"]`},
								"instrumentation-provider": {`scopeAttributes["otel.scope.provider"]`}, //this attribute doesn't exist in the span but it is Optional tag so it ignored by mapping
							},
						},
					},
					Vars: &[]settings.OtelVariableMapping{
						{
							Name:  "instanceId",
							Value: strExpr(`resourceAttributes["service.instance.id"]`),
						},
					},
				},
			},
			relationMappings: []settings.OtelRelationMapping{
				{
					Conditions: []settings.OtelConditionMapping{
						{Action: settings.CREATE, Expression: boolExpr(`spanAttributes["http.method"] == "GET"`)},
					},
					Output: settings.OtelRelationMappingOutput{
						SourceId: strExpr(`resourceAttributes["not-existing-attr"]`),
						TargetId: strExpr(`spanAttributes["service.name"]`),
						TypeName: strExpr(`"http-request"`),
					},
				},
			},
			expected: topo_stream_v1.TopologyStreamMessage{
				CollectionTimestamp: now,
				SubmittedTimestamp:  now,
				Payload: &topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
					TopologyStreamRepeatElementsData: &topo_stream_v1.TopologyStreamRepeatElementsData{
						ExpiryIntervalMs: 0,
						Components:       []*topo_stream_v1.TopologyStreamComponent{},
						Relations:        []*topo_stream_v1.TopologyStreamRelation{},
						Errors:           []string{"CEL evaluation error: no such key: not-existing-attr", "CEL evaluation error: no such key: not-existing-attr"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval, _ := NewCELEvaluator()
			result := ConvertSpanToTopologyStreamMessage(eval, traces, tt.componentMappings, tt.relationMappings, now)
			unify(&result)
			unify(&tt.expected)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func unify(data *topo_stream_v1.TopologyStreamMessage) {
	for _, component := range data.Payload.(*topo_stream_v1.TopologyStreamMessage_TopologyStreamRepeatElementsData).TopologyStreamRepeatElementsData.Components {
		sort.Strings(component.Tags)
	}
}
