//nolint:testpackage
package internal

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestMappingSpan_MapComponent(t *testing.T) {
	spanEvalContext := &ExpressionEvalContext{
		Span: NewSpan("name", "client", "ok", "", map[string]any{
			"kind":     "licence",
			"amount":   1000,
			"priority": "urgent",
			"env.name": "prod",
		}),
		Scope: NewScope("kamon", "version", map[string]any{
			"name": "kamon",
		}),
		Resource: NewResource(map[string]any{
			"name":              "microservice",
			"service.name":      "billing",
			"service.namespace": "billing-ns",
		}),
	}
	//nolint:govet
	tests := []struct {
		name        string
		mapping     *settingsproto.OtelComponentMapping
		evalContext *ExpressionEvalContext
		vars        map[string]any
		want        *topostreamv1.TopologyStreamComponent
		expectErr   []error
	}{
		{
			name: "valid mapping with all required fields and Configuration/Status fields",
			mapping: &settingsproto.OtelComponentMapping{
				Output: settingsproto.OtelComponentMappingOutput{
					Identifier:       strExpr(`resource.attributes["service.name"]`),
					Name:             strExpr("resource.attributes['service.name']"),
					TypeName:         strExpr("'service'"),
					TypeIdentifier:   ptr(strExpr("'service_id'")),
					DomainName:       strExpr(`vars.namespace`),
					DomainIdentifier: ptr(strExpr(`vars["namespace"]`)),
					LayerName:        strExpr(`'backend'`),
					LayerIdentifier:  ptr(strExpr("'backend_id'")),
					Optional: &settingsproto.OtelComponentMappingFieldMapping{
						Tags: &[]settingsproto.OtelTagMapping{
							{
								Source: anyExpr("span.attributes['priority']"),
								Target: "priority",
							},
							{
								Source: anyExpr("scope.attributes['name']"),
								Target: "scopeName",
							},
							{
								Source: anyExpr("resource.attributes['name']"),
								Target: "resourceName",
							},
						},
					},
					Required: &settingsproto.OtelComponentMappingFieldMapping{
						Configuration: ptr(anyExpr("omit(span.attributes, ['priority'])")),
						Status:        ptr(anyExpr("pick(span.attributes, ['priority', 'kind'])")),
						Tags: &[]settingsproto.OtelTagMapping{
							{
								Source: anyExpr("span.attributes['kind']"),
								Target: "kind",
							},
							{
								Source: anyExpr("span.attributes['amount']"),
								Target: "amount",
							},
						},
					},
				},
			},
			evalContext: spanEvalContext,
			vars: map[string]any{
				"namespace": "payments_ns",
			},
			want: &topostreamv1.TopologyStreamComponent{
				ExternalId:       "billing",
				Identifiers:      []string{"billing"},
				Name:             "billing",
				TypeName:         "service",
				TypeIdentifier:   ptr("service_id"),
				DomainName:       "payments_ns",
				DomainIdentifier: ptr("payments_ns"),
				LayerName:        "backend",
				LayerIdentifier:  ptr("backend_id"),
				Tags:             []string{"priority:urgent", "kind:licence", "amount:1000", "scopeName:kamon", "resourceName:microservice"},
				ResourceDefinition: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"kind":     structpb.NewStringValue("licence"),
						"amount":   structpb.NewNumberValue(1000),
						"env.name": structpb.NewStringValue("prod"),
					},
				},
				StatusData: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"priority": structpb.NewStringValue("urgent"),
						"kind":     structpb.NewStringValue("licence"),
					},
				},
			},
			expectErr: nil,
		},
		{
			name: "valid mapping with minimal set of properties",
			mapping: &settingsproto.OtelComponentMapping{
				Output: settingsproto.OtelComponentMappingOutput{
					Identifier: strExpr(`resource.attributes["service.name"]`),
					Name:       strExpr(`resource.attributes["service.name"]`),
					TypeName:   strExpr("'service'"),
					DomainName: strExpr(`'payment'`),
					LayerName:  strExpr("'backend'"),
				},
			},
			evalContext: spanEvalContext,
			vars:        map[string]any{},
			want: &topostreamv1.TopologyStreamComponent{
				ExternalId:  "billing",
				Identifiers: []string{"billing"},
				Name:        "billing",
				TypeName:    "service",
				DomainName:  "payment",
				LayerName:   "backend",
				Tags:        nil,
			},
			expectErr: nil,
		},
		{
			name: "missing required fields",
			mapping: &settingsproto.OtelComponentMapping{
				Output: settingsproto.OtelComponentMappingOutput{
					Identifier:       strExpr(`resource.attributes["service.name"]`),
					Name:             strExpr(`span.attributes["non-existing-attr"]`),
					TypeName:         strExpr(`'service'`),
					TypeIdentifier:   ptr(strExpr("'service_id'")),
					DomainName:       strExpr("vars['non-existing-var']"),
					DomainIdentifier: ptr(strExpr("vars.namespace")),
					LayerName:        strExpr(`'backend'`),
					LayerIdentifier:  ptr(strExpr("'backend_id'")),
					Optional: &settingsproto.OtelComponentMappingFieldMapping{
						Tags: &[]settingsproto.OtelTagMapping{
							{
								Source: anyExpr("span.attributes['priority']"),
								Target: "priority",
							},
						},
					},
					Required: &settingsproto.OtelComponentMappingFieldMapping{
						Tags: &[]settingsproto.OtelTagMapping{
							{
								Source: anyExpr("span.attributes['kind']"),
								Target: "kind",
							},
							{
								Source: anyExpr("span.attributes['amount']"),
								Target: "amount",
							},
						},
					},
				},
			},
			evalContext: spanEvalContext,
			vars: map[string]any{
				"namespace": "payments_ns",
			},
			want:      nil,
			expectErr: []error{errors.New("name: no such key: non-existing-attr"), errors.New("domainName: no such key: non-existing-var")},
		},
		{
			name: "missing optional fields are ok",
			mapping: &settingsproto.OtelComponentMapping{
				Output: settingsproto.OtelComponentMappingOutput{
					Identifier:       strExpr(`resource.attributes["service.name"]`),
					Name:             strExpr(`resource.attributes["service.name"]`),
					TypeName:         strExpr(`'service'`),
					TypeIdentifier:   ptr(strExpr(`span.attributes["non-existing-attr1"]`)),
					DomainName:       strExpr("resource.attributes['service.namespace']"),
					DomainIdentifier: ptr(strExpr(`span.attributes["non-existing-attr2"]`)),
					LayerName:        strExpr(`'backend'`),
					LayerIdentifier:  ptr(strExpr(`span.attributes["non-existing-attr3"]`)),
					Optional: &settingsproto.OtelComponentMappingFieldMapping{
						AdditionalIdentifiers: &[]settingsproto.OtelStringExpression{
							{Expression: `resource.attributes["missing"]`},
						},

						Tags: &[]settingsproto.OtelTagMapping{
							{
								Source: anyExpr(`span.attributes["non-existing-attr4"]`),
								Target: "priority",
							},
						},
					},
					Required: &settingsproto.OtelComponentMappingFieldMapping{
						Tags: &[]settingsproto.OtelTagMapping{
							{
								Source: anyExpr("span.attributes.kind"),
								Target: "kind",
							},
							{
								Source: anyExpr("span.attributes.amount"),
								Target: "amount",
							},
						},
					},
				},
			},
			evalContext: spanEvalContext,
			vars: map[string]any{
				"namespace": "payments_ns",
			},
			want: &topostreamv1.TopologyStreamComponent{
				ExternalId:  "billing",
				Identifiers: []string{"billing"},
				Name:        "billing",
				TypeName:    "service",
				DomainName:  "billing-ns",
				LayerName:   "backend",
				Tags:        []string{"kind:licence", "amount:1000"},
			},
			expectErr: nil,
		},
		{
			name: "optional fields with invalid expression still produce an error",
			mapping: &settingsproto.OtelComponentMapping{
				Output: settingsproto.OtelComponentMappingOutput{
					Identifier:       strExpr(`resource.attributes["service.name"]`),
					Name:             strExpr(`resource.attributes.name']`),
					TypeName:         strExpr(`'service'`),
					TypeIdentifier:   ptr(strExpr(`span.attributes-existing-attr1"]`)),
					DomainName:       strExpr("resource.attributes['service.namespace']"),
					DomainIdentifier: ptr(strExpr(`blabla`)),
					LayerName:        strExpr(`'backend'`),
					LayerIdentifier:  ptr(strExpr(`non-existing-attr3"]`)),
					Optional: &settingsproto.OtelComponentMappingFieldMapping{
						AdditionalIdentifiers: &[]settingsproto.OtelStringExpression{
							{Expression: `uhoh`},
						},

						Tags: &[]settingsproto.OtelTagMapping{
							{
								Source: anyExpr(`span.attributes["nope`),
								Target: "priority",
							},
						},
					},
					Required: &settingsproto.OtelComponentMappingFieldMapping{
						Tags: &[]settingsproto.OtelTagMapping{
							{
								Source: anyExpr("span.attributes['kind']"),
								Target: "kind",
							},
							{
								Source: anyExpr("span.attributes['amount']"),
								Target: "amount",
							},
						},
					},
				},
			},
			evalContext: spanEvalContext,
			vars: map[string]any{
				"namespace": "payments_ns",
			},
			want: nil,
			expectErr: []error{
				errors.New("optional.additionalIdentifiers: ERROR: <input>:1:1: undeclared reference to 'uhoh' (in container '')\n | uhoh\n | ^"),
				errors.New("name: ERROR: <input>:1:25: Syntax error: token recognition error at: '']'\n | resource.attributes.name']\n | ........................^"),
				errors.New("typeIdentifier: ERROR: <input>:1:31: Syntax error: token recognition error at: '\"]'\n | span.attributes-existing-attr1\"]\n | ..............................^"),
				errors.New("layerIdentifier: ERROR: <input>:1:19: Syntax error: token recognition error at: '\"]'\n | non-existing-attr3\"]\n | ..................^"),
				errors.New("domainIdentifier: ERROR: <input>:1:1: undeclared reference to 'blabla' (in container '')\n | blabla\n | ^")},
		},
	}

	mapper := NewMapper(context.Background(), makeMeteredCacheSettings(100, 30*time.Second), makeMeteredCacheSettings(100, 30*time.Second))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
			got, err := mapper.MapComponent(tt.mapping, eval, tt.evalContext.CloneWithVariables(tt.vars))
			assert.Equal(t, errorStrings(tt.expectErr), errorStrings(err))
			if got != nil {
				sort.Strings(got.Tags)
			}
			if tt.want != nil {
				sort.Strings(tt.want.Tags)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestResolveTagMappings(t *testing.T) {
	testSpan := ptrace.NewSpan()

	testScope := ptrace.NewScopeSpans()
	testScope.Scope().Attributes().PutStr("telemetry.sdk.lang", "java")
	testScope.Scope().Attributes().PutStr("telemetry.sdk.version", "1.0")
	testScope.Scope().Attributes().PutInt("telemetry.sdk.intAttr", 5)

	testResource := ptrace.NewResourceSpans()
	testResource.Resource().Attributes().PutStr("name", "microservice")
	testResource.Resource().Attributes().PutStr("service.name", "billing")

	// slice attribute type
	args := testResource.Resource().Attributes().PutEmptySlice("process.command_args")
	args.AppendEmpty().SetStr("java")
	args.AppendEmpty().SetStr("-jar")
	args.AppendEmpty().SetStr("app.jar")

	// map attribute type
	depMap := testResource.Resource().Attributes().PutEmptyMap("deployment")
	depMap.PutStr("region", "eu-west-1")
	depMap.PutStr("env", "prod")

	eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
	ctx := NewSpanEvalContext(
		NewSpan("name", "client", "ok", "", testSpan.Attributes().AsRaw()),
		NewScope("name", "version", testScope.Scope().Attributes().AsRaw()),
		NewResource(testResource.Resource().Attributes().AsRaw()),
	)

	tests := []struct {
		name          string
		mappings      []settingsproto.OtelTagMapping
		want          map[string]string
		errorContains string
	}{
		{
			name: "simple mapping without pattern",
			mappings: []settingsproto.OtelTagMapping{
				{
					Source: anyExpr(`resource.attributes["service.name"]`),
					Target: "service.name",
				},
				{
					Source: anyExpr("'static-value'"),
					Target: "static",
				},
			},
			want: map[string]string{
				"service.name": "billing",
				"static":       "static-value",
			},
		},
		{
			name: "mapping with regex pattern and capture group",
			mappings: []settingsproto.OtelTagMapping{
				{
					Source:  anyExpr("scope.attributes"),
					Pattern: ptr("telemetry.sdk.(.*)"),
					Target:  "otel.${1}",
				},
			},
			want: map[string]string{
				"otel.lang":    "java",
				"otel.version": "1.0",
				"otel.intAttr": "5", // make sure we can stringify other types
			},
		},
		{
			name: "mapping with multiple capture groups",
			mappings: []settingsproto.OtelTagMapping{
				{
					Source:  anyExpr("scope.attributes"),
					Pattern: ptr("telemetry.(sdk).(.*)"),
					Target:  "otel.${1}.${2}",
				},
			},
			want: map[string]string{
				"otel.sdk.lang":    "java",
				"otel.sdk.version": "1.0",
				"otel.sdk.intAttr": "5",
			},
		},
		{
			name: "mapping with regex that matches nothing",
			mappings: []settingsproto.OtelTagMapping{
				{
					Source:  anyExpr("scope.attributes"),
					Pattern: ptr("foo(.*)"),
					Target:  "ignored.${1}",
				},
			},
			want: map[string]string{}, // no matches
		},
		{
			name: "mixed simple and regex mappings",
			mappings: []settingsproto.OtelTagMapping{
				{
					Source: anyExpr(`resource.attributes["service.name"]`),
					Target: "service",
				},
				{
					Source:  anyExpr("scope.attributes"),
					Pattern: ptr("telemetry.sdk.(.*)"),
					Target:  "otel.${1}",
				},
			},
			want: map[string]string{
				"service":      "billing",
				"otel.lang":    "java",
				"otel.version": "1.0",
				"otel.intAttr": "5",
			},
		},
		{
			name: "invalid map source without pattern (string expression expected)",
			mappings: []settingsproto.OtelTagMapping{
				{
					Source: anyExpr("scope.attributes"),
					Target: "oops",
				},
			},
			want:          map[string]string{},
			errorContains: "failed to evaluate OtelTagMapping source \"scope.attributes\": expected string type, got: map(string, dyn), for expression 'scope.attributes'",
		},
		// cases for group merge mapping and explicit mapping trying to populate the same key
		{
			name: "explicit mapping overrides merged group key",
			mappings: []settingsproto.OtelTagMapping{
				{
					// Group mapping injects all scope Attributes with pattern
					Source:  anyExpr("scope.attributes"),
					Pattern: ptr("telemetry.sdk.(.*)"),
					Target:  "otel.${1}",
				},
				{
					// Explicit mapping for one key that would otherwise be produced by the group
					Source: anyExpr("'overridden-value'"),
					Target: "otel.lang",
				},
			},
			want: map[string]string{
				"otel.lang":    "overridden-value", // explicit wins
				"otel.version": "1.0",
				"otel.intAttr": "5",
			},
		},
		{
			name: "merged group mapping does not override explicit key",
			mappings: []settingsproto.OtelTagMapping{
				{
					Source: anyExpr("'explicit-value'"),
					Target: "otel.lang",
				},
				{
					Source:  anyExpr("scope.attributes"),
					Pattern: ptr("telemetry.sdk.(.*)"),
					Target:  "otel.${1}",
				},
			},
			want: map[string]string{
				"otel.lang":    "explicit-value", // explicit preserved
				"otel.version": "1.0",
				"otel.intAttr": "5",
			},
		},
		{
			name: "explicit mapping overrides merged resource attribute",
			mappings: []settingsproto.OtelTagMapping{
				{
					Source:  anyExpr("resource.attributes"),
					Pattern: ptr("(.*)"),
					Target:  "res.${1}",
				},
				{
					Source: anyExpr("'special'"),
					Target: "res.name",
				},
			},
			want: map[string]string{
				"res.service.name":         "billing",
				"res.name":                 "special", // explicit wins over merged
				"res.deployment":           `{"env":"prod","region":"eu-west-1"}`,
				"res.process.command_args": "java -jar app.jar",
			},
		},
		{
			name: "merge full attribute map without overrides",
			mappings: []settingsproto.OtelTagMapping{
				{
					Source:  anyExpr("scope.attributes"),
					Pattern: ptr("(.*)"),
					Target:  "all.${1}",
				},
			},
			want: map[string]string{
				"all.telemetry.sdk.lang":    "java",
				"all.telemetry.sdk.version": "1.0",
				"all.telemetry.sdk.intAttr": "5",
			},
		},
		{
			name: "merge filtered attribute map by prefix",
			mappings: []settingsproto.OtelTagMapping{
				{
					Source:  anyExpr("scope.attributes"),
					Pattern: ptr("telemetry.sdk.(.*)"),
					Target:  "filtered.${1}",
				},
			},
			want: map[string]string{
				"filtered.lang":    "java",
				"filtered.version": "1.0",
				"filtered.intAttr": "5",
			},
		},
		{
			name: "invalid source: not a map with pattern",
			mappings: []settingsproto.OtelTagMapping{
				{
					Source:  anyExpr("resource.attributes['service.name']"),
					Pattern: ptr("telemetry.sdk.(.*)"),
					Target:  "oops",
				},
			},
			want:          map[string]string{},
			errorContains: "expected 'map[string]any', got 'string'",
		},
	}

	mapper := NewMapper(context.Background(), makeMeteredCacheSettings(100, 30*time.Second), makeMeteredCacheSettings(100, 30*time.Second))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, errs := mapper.ResolveTagMappings(tt.mappings, eval, ctx)
			if tt.errorContains != "" {
				require.NotEmpty(t, errs, "expected an error but got none")
				require.Contains(t, errs[0].Error(), tt.errorContains)
			} else {
				require.Empty(t, errs, "expected no errors but got %v", errs)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMapping_MapRelation(t *testing.T) {
	spanEvalContext := &ExpressionEvalContext{
		Span: NewSpan("name", "client", "ok", "", map[string]any{
			"kind":     "licence",
			"amount":   1000,
			"priority": "urgent",
			"env.name": "prod",
		}),
		Scope: NewScope("kamon", "version", map[string]any{
			"name": "kamon",
		}),
		Resource: NewResource(map[string]any{
			"name":              "microservice",
			"service.name":      "billing",
			"service.namespace": "billing-ns",
		}),
	}

	//nolint:govet
	tests := []struct {
		name        string
		mapping     *settingsproto.OtelRelationMapping
		evalContext *ExpressionEvalContext
		vars        map[string]any
		want        *topostreamv1.TopologyStreamRelation
		expectErr   []error
	}{
		{
			name: "valid relation mapping",
			mapping: &settingsproto.OtelRelationMapping{
				Output: settingsproto.OtelRelationMappingOutput{
					SourceId:       strExpr(`resource.attributes["service.name"]`),
					TargetId:       strExpr(`'database'`),
					TypeName:       strExpr("'query'"),
					TypeIdentifier: ptr(strExpr("span.attributes.kind")),
				},
			},
			evalContext: spanEvalContext,
			vars:        map[string]any{},
			want: &topostreamv1.TopologyStreamRelation{
				ExternalId:       "billing-database",
				SourceIdentifier: "billing",
				TargetIdentifier: "database",
				Name:             "",
				TypeName:         "query",
				TypeIdentifier:   ptr("licence"),
				Tags:             nil,
			},
			expectErr: nil,
		},
		{
			name: "missing optional attribute is ok",
			mapping: &settingsproto.OtelRelationMapping{
				Output: settingsproto.OtelRelationMappingOutput{
					SourceId:       strExpr(`resource.attributes["service.name"]`),
					TargetId:       strExpr(`'database'`),
					TypeName:       strExpr("'query'"),
					TypeIdentifier: ptr(strExpr("span.attributes.blabla")),
				},
			},
			evalContext: spanEvalContext,
			vars:        map[string]any{},
			want: &topostreamv1.TopologyStreamRelation{
				ExternalId:       "billing-database",
				SourceIdentifier: "billing",
				TargetIdentifier: "database",
				Name:             "",
				TypeName:         "query",
				TypeIdentifier:   nil,
				Tags:             nil,
			},
			expectErr: nil,
		},
		{
			name: "invalid expression for optional field fails",
			mapping: &settingsproto.OtelRelationMapping{
				Output: settingsproto.OtelRelationMappingOutput{
					SourceId:       strExpr(`resource.attributes["service.name"]`),
					TargetId:       strExpr(`'database'`),
					TypeName:       strExpr("'query'"),
					TypeIdentifier: ptr(strExpr("not here")),
				},
			},
			evalContext: spanEvalContext,
			vars:        map[string]any{},
			want:        nil,
			expectErr:   []error{errors.New("typeIdentifier: ERROR: <input>:1:5: Syntax error: extraneous input 'here' expecting <EOF>\n | not here\n | ....^")},
		},
		{
			name: "missing mandatory attributes",
			mapping: &settingsproto.OtelRelationMapping{
				Output: settingsproto.OtelRelationMappingOutput{
					SourceId: strExpr(`span.attributes["non-existing"]`),
					TargetId: strExpr("'database'"),
					TypeName: strExpr(`'query'`),
				},
			},
			evalContext: spanEvalContext,
			vars:        map[string]any{},
			want:        nil,
			expectErr:   []error{errors.New("sourceId: no such key: non-existing")},
		},
	}

	mapper := NewMapper(context.Background(), makeMeteredCacheSettings(100, 30*time.Second), makeMeteredCacheSettings(100, 30*time.Second))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval, _ := NewCELEvaluator(context.Background(), makeMeteredCacheSettings(100, 30*time.Second))
			got, err := mapper.MapRelation(tt.mapping, eval, tt.evalContext.CloneWithVariables(tt.vars))
			assert.Equal(t, errorStrings(tt.expectErr), errorStrings(err))
			if got != nil {
				sort.Strings(got.Tags)
			}
			if tt.want != nil {
				sort.Strings(tt.want.Tags)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func errorStrings(errs []error) []string {
	out := make([]string, len(errs))
	for i, e := range errs {
		out[i] = e.Error()
	}
	return out
}
