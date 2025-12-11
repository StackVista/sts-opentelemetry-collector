//nolint:testpackage
package internal

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
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
		mapping     *settings.OtelComponentMapping
		evalContext *ExpressionEvalContext
		vars        map[string]any
		want        *topostreamv1.TopologyStreamComponent
		expectErr   []error
	}{
		{
			name: "valid mapping with all required fields",
			mapping: &settings.OtelComponentMapping{
				Output: settings.OtelComponentMappingOutput{
					Identifier:       strExpr(`${resource.attributes["service.name"]}`),
					Name:             strExpr("${resource.attributes['service.name']}"),
					TypeName:         strExpr("service"),
					TypeIdentifier:   ptr(strExpr("service_id")),
					DomainName:       strExpr(`${vars.namespace}`),
					DomainIdentifier: ptr(strExpr(`${vars["namespace"]}`)),
					LayerName:        strExpr(`backend`),
					LayerIdentifier:  ptr(strExpr("backend_id")),
					Optional: &settings.OtelComponentMappingFieldMapping{
						Tags: &[]settings.OtelTagMapping{
							{
								Source: anyExpr("${span.attributes['priority']}"),
								Target: "priority",
							},
							{
								Source: anyExpr("${scope.attributes['name']}"),
								Target: "scopeName",
							},
							{
								Source: anyExpr("${resource.attributes['name']}"),
								Target: "resourceName",
							},
						},
					},
					Required: &settings.OtelComponentMappingFieldMapping{
						Tags: &[]settings.OtelTagMapping{
							{
								Source: anyExpr("${span.attributes['kind']}"),
								Target: "kind",
							},
							{
								Source: anyExpr("${span.attributes['amount']}"),
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
			},
			expectErr: nil,
		},
		{
			name: "valid mapping with minimal set of properties",
			mapping: &settings.OtelComponentMapping{
				Output: settings.OtelComponentMappingOutput{
					Identifier: strExpr(`${resource.attributes["service.name"]}`),
					Name:       strExpr(`${resource.attributes["service.name"]}`),
					TypeName:   strExpr("service"),
					DomainName: strExpr(`payment`),
					LayerName:  strExpr("backend"),
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
			mapping: &settings.OtelComponentMapping{
				Output: settings.OtelComponentMappingOutput{
					Identifier:       strExpr(`${resource.attributes["service.name"]}`),
					Name:             strExpr(`${span.attributes["non-existing-attr"]}`),
					TypeName:         strExpr(`service`),
					TypeIdentifier:   ptr(strExpr("service_id")),
					DomainName:       strExpr("${vars['non-existing-var']}"),
					DomainIdentifier: ptr(strExpr("${vars.namespace}")),
					LayerName:        strExpr(`backend`),
					LayerIdentifier:  ptr(strExpr("backend_id")),
					Optional: &settings.OtelComponentMappingFieldMapping{
						Tags: &[]settings.OtelTagMapping{
							{
								Source: anyExpr("${span.attributes['priority']}"),
								Target: "priority",
							},
						},
					},
					Required: &settings.OtelComponentMappingFieldMapping{
						Tags: &[]settings.OtelTagMapping{
							{
								Source: anyExpr("${span.attributes['kind']}"),
								Target: "kind",
							},
							{
								Source: anyExpr("${span.attributes['amount']}"),
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
			mapping: &settings.OtelComponentMapping{
				Output: settings.OtelComponentMappingOutput{
					Identifier:       strExpr(`${resource.attributes["service.name"]}`),
					Name:             strExpr(`${resource.attributes["service.name"]}`),
					TypeName:         strExpr(`service`),
					TypeIdentifier:   ptr(strExpr(`${span.attributes["non-existing-attr1"]}`)),
					DomainName:       strExpr("${resource.attributes['service.namespace']}"),
					DomainIdentifier: ptr(strExpr(`${span.attributes["non-existing-attr2"]}`)),
					LayerName:        strExpr(`backend`),
					LayerIdentifier:  ptr(strExpr(`${span.attributes["non-existing-attr3"]}`)),
					Optional: &settings.OtelComponentMappingFieldMapping{
						AdditionalIdentifiers: &[]settings.OtelStringExpression{
							{Expression: `${resource.attributes["missing"]}`},
						},

						Tags: &[]settings.OtelTagMapping{
							{
								Source: anyExpr(`${span.attributes["non-existing-attr4"]}`),
								Target: "priority",
							},
						},
					},
					Required: &settings.OtelComponentMappingFieldMapping{
						Tags: &[]settings.OtelTagMapping{
							{
								Source: anyExpr("${span.attributes.kind}"),
								Target: "kind",
							},
							{
								Source: anyExpr("${span.attributes.amount}"),
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
			mapping: &settings.OtelComponentMapping{
				Output: settings.OtelComponentMappingOutput{
					Identifier:       strExpr(`${resource.attributes["service.name"]}`),
					Name:             strExpr(`${resource.attributes.name']}`),
					TypeName:         strExpr(`service`),
					TypeIdentifier:   ptr(strExpr(`${span.attributes-existing-attr1"]}`)),
					DomainName:       strExpr("${resource.attributes['service.namespace']}"),
					DomainIdentifier: ptr(strExpr(`${blabla}`)),
					LayerName:        strExpr(`backend`),
					LayerIdentifier:  ptr(strExpr(`${non-existing-attr3"]}`)),
					Optional: &settings.OtelComponentMappingFieldMapping{
						AdditionalIdentifiers: &[]settings.OtelStringExpression{
							{Expression: `${uhoh}`},
						},

						Tags: &[]settings.OtelTagMapping{
							{
								Source: anyExpr(`${span.attributes["nope}`),
								Target: "priority",
							},
						},
					},
					Required: &settings.OtelComponentMappingFieldMapping{
						Tags: &[]settings.OtelTagMapping{
							{
								Source: anyExpr("${span.attributes['kind']}"),
								Target: "kind",
							},
							{
								Source: anyExpr("${span.attributes['amount']}"),
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
				errors.New("name: unterminated interpolation starting at pos 0"),
				errors.New("typeIdentifier: unterminated interpolation starting at pos 0"),
				errors.New("layerIdentifier: unterminated interpolation starting at pos 0"),
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
		mappings      []settings.OtelTagMapping
		want          map[string]string
		errorContains string
	}{
		{
			name: "simple mapping without pattern",
			mappings: []settings.OtelTagMapping{
				{
					Source: anyExpr(`${resource.attributes["service.name"]}`),
					Target: "service.name",
				},
				{
					Source: anyExpr("static-value"),
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
			mappings: []settings.OtelTagMapping{
				{
					Source:  anyExpr("${scope.attributes}"),
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
			mappings: []settings.OtelTagMapping{
				{
					Source:  anyExpr("${scope.attributes}"),
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
			mappings: []settings.OtelTagMapping{
				{
					Source:  anyExpr("${scope.attributes}"),
					Pattern: ptr("foo(.*)"),
					Target:  "ignored.${1}",
				},
			},
			want: map[string]string{}, // no matches
		},
		{
			name: "mixed simple and regex mappings",
			mappings: []settings.OtelTagMapping{
				{
					Source: anyExpr(`${resource.attributes["service.name"]}`),
					Target: "service",
				},
				{
					Source:  anyExpr("${scope.attributes}"),
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
			mappings: []settings.OtelTagMapping{
				{
					Source: anyExpr("${scope.attributes}"),
					Target: "oops",
				},
			},
			want:          map[string]string{},
			errorContains: "failed to evaluate OtelTagMapping source \"${scope.attributes}\": expected string type, got: map(string, dyn), for expression '${scope.attributes}'",
		},
		// cases for group merge mapping and explicit mapping trying to populate the same Key
		{
			name: "explicit mapping overrides merged group Key",
			mappings: []settings.OtelTagMapping{
				{
					// Group mapping injects all scope Attributes with pattern
					Source:  anyExpr("${scope.attributes}"),
					Pattern: ptr("telemetry.sdk.(.*)"),
					Target:  "otel.${1}",
				},
				{
					// Explicit mapping for one Key that would otherwise be produced by the group
					Source: anyExpr("overridden-value"),
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
			name: "merged group mapping does not override explicit Key",
			mappings: []settings.OtelTagMapping{
				{
					Source: anyExpr("explicit-value"),
					Target: "otel.lang",
				},
				{
					Source:  anyExpr("${scope.attributes}"),
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
			mappings: []settings.OtelTagMapping{
				{
					Source:  anyExpr("${resource.attributes}"),
					Pattern: ptr("(.*)"),
					Target:  "res.${1}",
				},
				{
					Source: anyExpr("special"),
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
			mappings: []settings.OtelTagMapping{
				{
					Source:  anyExpr("${scope.attributes}"),
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
			mappings: []settings.OtelTagMapping{
				{
					Source:  anyExpr("${scope.attributes}"),
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
			mappings: []settings.OtelTagMapping{
				{
					Source:  anyExpr("${resource.attributes['service.name']}"),
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
		mapping     *settings.OtelRelationMapping
		evalContext *ExpressionEvalContext
		vars        map[string]any
		want        *topostreamv1.TopologyStreamRelation
		expectErr   []error
	}{
		{
			name: "valid relation mapping",
			mapping: &settings.OtelRelationMapping{
				Output: settings.OtelRelationMappingOutput{
					SourceId:       strExpr(`${resource.attributes["service.name"]}`),
					TargetId:       strExpr(`database`),
					TypeName:       strExpr("query"),
					TypeIdentifier: ptr(strExpr("${span.attributes.kind}")),
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
			mapping: &settings.OtelRelationMapping{
				Output: settings.OtelRelationMappingOutput{
					SourceId:       strExpr(`${resource.attributes["service.name"]}`),
					TargetId:       strExpr(`database`),
					TypeName:       strExpr("query"),
					TypeIdentifier: ptr(strExpr("${span.attributes.blabla}")),
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
			mapping: &settings.OtelRelationMapping{
				Output: settings.OtelRelationMappingOutput{
					SourceId:       strExpr(`${resource.attributes["service.name"]}`),
					TargetId:       strExpr(`database`),
					TypeName:       strExpr("query"),
					TypeIdentifier: ptr(strExpr("${not here}")),
				},
			},
			evalContext: spanEvalContext,
			vars:        map[string]any{},
			want:        nil,
			expectErr:   []error{errors.New("typeIdentifier: ERROR: <input>:1:5: Syntax error: extraneous input 'here' expecting <EOF>\n | not here\n | ....^")},
		},
		{
			name: "missing mandatory attributes",
			mapping: &settings.OtelRelationMapping{
				Output: settings.OtelRelationMappingOutput{
					SourceId: strExpr(`${span.attributes["non-existing"]}`),
					TargetId: strExpr("database"),
					TypeName: strExpr(`query`),
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
