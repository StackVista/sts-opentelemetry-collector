//nolint:testpackage
package internal

import (
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"

	topo_stream_v1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestMapping_MapComponent(t *testing.T) {

	testSpan := ptrace.NewSpan()
	testSpan.Attributes().PutStr("env.name", "prod")
	testSpan.Attributes().PutStr("service.name", "billing")
	testSpan.Attributes().PutInt("amount", 1000)
	testSpan.Attributes().PutStr("kind", "licence")
	testSpan.Attributes().PutStr("priority", "urgent")

	testScope := ptrace.NewScopeSpans()
	testScope.Scope().Attributes().PutStr("name", "kamon")

	testResource := ptrace.NewResourceSpans()
	testResource.Resource().Attributes().PutStr("name", "microservice")

	//nolint:govet
	tests := []struct {
		name      string
		mapping   *settings.OtelComponentMapping
		span      *ptrace.Span
		scope     *ptrace.ScopeSpans
		resource  *ptrace.ResourceSpans
		vars      map[string]any
		want      *topo_stream_v1.TopologyStreamComponent
		expectErr []error
	}{
		{
			name: "valid mapping with all required fields",
			mapping: &settings.OtelComponentMapping{
				Output: settings.OtelComponentMappingOutput{
					Identifier:       strExpr(`${spanAttributes["service.name"]}`),
					Name:             strExpr("${spanAttributes['service.name']}"),
					TypeName:         strExpr("service"),
					TypeIdentifier:   ptr(strExpr("service_id")),
					DomainName:       strExpr(`${vars.namespace}`),
					DomainIdentifier: ptr(strExpr(`${vars["namespace"]}`)),
					LayerName:        strExpr(`backend`),
					LayerIdentifier:  ptr(strExpr("backend_id")),
					Optional: &settings.OtelComponentMappingFieldMapping{
						Tags: &[]settings.OtelTagMapping{
							{
								Source: strExpr("${spanAttributes.priority}"),
								Target: "priority",
							},
							{
								Source: strExpr("${scopeAttributes.name}"),
								Target: "scopeName",
							},
							{
								Source: strExpr("${resourceAttributes.name}"),
								Target: "resourceName",
							},
						},
					},
					Required: &settings.OtelComponentMappingFieldMapping{
						Tags: &[]settings.OtelTagMapping{
							{
								Source: strExpr("${spanAttributes.kind}"),
								Target: "kind",
							},
							{
								Source: strExpr("${spanAttributes.amount}"),
								Target: "amount",
							},
						},
					},
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			vars: map[string]any{
				"namespace": "payments_ns",
			},
			want: &topo_stream_v1.TopologyStreamComponent{
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
					Identifier: strExpr(`${spanAttributes["service.name"]}`),
					Name:       strExpr(`${spanAttributes["service.name"]}`),
					TypeName:   strExpr("service"),
					DomainName: strExpr(`payment`),
					LayerName:  strExpr("backend"),
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			vars:     map[string]any{},
			want: &topo_stream_v1.TopologyStreamComponent{
				ExternalId:  "billing",
				Identifiers: []string{"billing"},
				Name:        "billing",
				TypeName:    "service",
				DomainName:  "payment",
				LayerName:   "backend",
				Tags:        []string{},
			},
			expectErr: nil,
		},
		{
			name: "missing required fields",
			mapping: &settings.OtelComponentMapping{
				Output: settings.OtelComponentMappingOutput{
					Identifier:       strExpr(`${spanAttributes["service.name"]}`),
					Name:             strExpr(`${spanAttributes["non-existing-attr"]}`),
					TypeName:         strExpr(`service`),
					TypeIdentifier:   ptr(strExpr("service_id")),
					DomainName:       strExpr("${vars['non-existing-var']}"),
					DomainIdentifier: ptr(strExpr("${vars.namespace}")),
					LayerName:        strExpr(`backend`),
					LayerIdentifier:  ptr(strExpr("backend_id")),
					Optional: &settings.OtelComponentMappingFieldMapping{
						Tags: &[]settings.OtelTagMapping{
							{
								Source: strExpr("${spanAttributes.priority}"),
								Target: "priority",
							},
						},
					},
					Required: &settings.OtelComponentMappingFieldMapping{
						Tags: &[]settings.OtelTagMapping{
							{
								Source: strExpr("${spanAttributes.kind}"),
								Target: "kind",
							},
							{
								Source: strExpr("${spanAttributes.amount}"),
								Target: "amount",
							},
						},
					},
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			vars: map[string]any{
				"namespace": "payments_ns",
			},
			want:      nil,
			expectErr: []error{errors.New("CEL evaluation error: no such key: non-existing-attr"), errors.New("CEL evaluation error: no such key: non-existing-var")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval, _ := NewCELEvaluator(CacheSettings{Size: 100, TTL: 30 * time.Second})
			evalCtx := NewEvalContext(tt.span.Attributes().AsRaw(), tt.scope.Scope().Attributes().AsRaw(), tt.resource.Resource().Attributes().AsRaw()).CloneWithVariables(tt.vars)
			got, err := MapComponent(tt.mapping, eval, evalCtx)
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
	testSpan.Attributes().PutStr("service.name", "billing")

	testScope := ptrace.NewScopeSpans()
	testScope.Scope().Attributes().PutStr("telemetry.sdk.lang", "java")
	testScope.Scope().Attributes().PutStr("telemetry.sdk.version", "1.0")
	testScope.Scope().Attributes().PutInt("telemetry.sdk.intAttr", 5)

	testResource := ptrace.NewResourceSpans()
	testResource.Resource().Attributes().PutStr("name", "microservice")

	// slice attribute type
	args := testResource.Resource().Attributes().PutEmptySlice("process.command_args")
	args.AppendEmpty().SetStr("java")
	args.AppendEmpty().SetStr("-jar")
	args.AppendEmpty().SetStr("app.jar")

	// map attribute type
	depMap := testResource.Resource().Attributes().PutEmptyMap("deployment")
	depMap.PutStr("region", "eu-west-1")
	depMap.PutStr("env", "prod")

	ctx := NewEvalContext(testSpan.Attributes().AsRaw(), testScope.Scope().Attributes().AsRaw(), testResource.Resource().Attributes().AsRaw())
	eval, _ := NewCELEvaluator(CacheSettings{Size: 100, TTL: 30 * time.Minute})

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
					Source: strExpr(`${spanAttributes["service.name"]}`),
					Target: "service.name",
				},
				{
					Source: strExpr("static-value"),
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
					Source:  strExpr("${scopeAttributes}"),
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
					Source:  strExpr("${scopeAttributes}"),
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
					Source:  strExpr("${scopeAttributes}"),
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
					Source: strExpr(`${spanAttributes["service.name"]}`),
					Target: "service",
				},
				{
					Source:  strExpr("${scopeAttributes}"),
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
					Source: strExpr("${scopeAttributes}"),
					Target: "oops",
				},
			},
			want:          map[string]string{},
			errorContains: "failed to evaluate OtelTagMapping source \"${scopeAttributes}\": expected string type, got: map(string, dyn), for expression '${scopeAttributes}'",
		},
		// cases for group merge mapping and explicit mapping trying to populate the same key
		{
			name: "explicit mapping overrides merged group key",
			mappings: []settings.OtelTagMapping{
				{
					// Group mapping injects all scope attributes with pattern
					Source:  strExpr("${scopeAttributes}"),
					Pattern: ptr("telemetry.sdk.(.*)"),
					Target:  "otel.${1}",
				},
				{
					// Explicit mapping for one key that would otherwise be produced by the group
					Source: strExpr("overridden-value"),
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
			mappings: []settings.OtelTagMapping{
				{
					Source: strExpr("explicit-value"),
					Target: "otel.lang",
				},
				{
					Source:  strExpr("${scopeAttributes}"),
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
					Source:  strExpr("${resourceAttributes}"),
					Pattern: ptr("(.*)"),
					Target:  "res.${1}",
				},
				{
					Source: strExpr("special"),
					Target: "res.name",
				},
			},
			want: map[string]string{
				"res.name":                 "special", // explicit wins over merged
				"res.deployment":           `{"env":"prod","region":"eu-west-1"}`,
				"res.process.command_args": "java -jar app.jar",
			},
		},
		{
			name: "merge full attribute map without overrides",
			mappings: []settings.OtelTagMapping{
				{
					Source:  strExpr("${scopeAttributes}"),
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
					Source:  strExpr("${scopeAttributes}"),
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, errs := ResolveTagMappings(tt.mappings, eval, ctx)
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

	testSpan := ptrace.NewSpan()
	testSpan.Attributes().PutStr("env.name", "prod")
	testSpan.Attributes().PutStr("service.name", "billing")
	testSpan.Attributes().PutInt("amount", 1000)
	testSpan.Attributes().PutStr("kind", "licence")
	testSpan.Attributes().PutStr("priority", "urgent")

	testScope := ptrace.NewScopeSpans()
	testScope.Scope().Attributes().PutStr("name", "kamon")

	testResource := ptrace.NewResourceSpans()
	testResource.Resource().Attributes().PutStr("name", "microservice")

	//nolint:govet
	tests := []struct {
		name      string
		mapping   *settings.OtelRelationMapping
		span      *ptrace.Span
		scope     *ptrace.ScopeSpans
		resource  *ptrace.ResourceSpans
		vars      map[string]any
		want      *topo_stream_v1.TopologyStreamRelation
		expectErr []error
	}{
		{
			name: "valid relation mapping",
			mapping: &settings.OtelRelationMapping{
				Output: settings.OtelRelationMappingOutput{
					SourceId:       strExpr(`${spanAttributes["service.name"]}`),
					TargetId:       strExpr(`database`),
					TypeName:       strExpr("query"),
					TypeIdentifier: ptr(strExpr("${spanAttributes.kind}")),
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			vars:     map[string]any{},
			want: &topo_stream_v1.TopologyStreamRelation{
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
			name: "missing mandatory attributes",
			mapping: &settings.OtelRelationMapping{
				Output: settings.OtelRelationMappingOutput{
					SourceId: strExpr(`${spanAttributes["non-existing"]}`),
					TargetId: strExpr("database"),
					TypeName: strExpr(`query`),
				},
			},
			span:      &testSpan,
			scope:     &testScope,
			resource:  &testResource,
			vars:      map[string]any{},
			want:      nil,
			expectErr: []error{errors.New("CEL evaluation error: no such key: non-existing")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval, _ := NewCELEvaluator(CacheSettings{Size: 100, TTL: 30 * time.Second})
			evalCtx := NewEvalContext(tt.span.Attributes().AsRaw(), tt.scope.Scope().Attributes().AsRaw(), tt.resource.Resource().Attributes().AsRaw()).CloneWithVariables(tt.vars)
			got, err := MapRelation(tt.mapping, eval, evalCtx)
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
