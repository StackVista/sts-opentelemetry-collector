package internal

import (
	"errors"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"sort"
	"testing"

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

	tests := []struct {
		name      string
		mapping   *settings.OtelComponentMapping
		span      *ptrace.Span
		scope     *ptrace.ScopeSpans
		resource  *ptrace.ResourceSpans
		vars      *map[string]string
		want      *topo_stream_v1.TopologyStreamComponent
		expectErr []error
	}{
		{
			name: "valid mapping with all required fields",
			mapping: &settings.OtelComponentMapping{
				Output: settings.OtelComponentMappingOutput{
					Identifier:       strExpr(`spanAttributes["service.name"]`),
					Name:             strExpr(`spanAttributes["service.name"]`),
					TypeName:         strExpr(`"service"`),
					TypeIdentifier:   ptr(strExpr(`"service_id"`)),
					DomainName:       strExpr(`vars.namespace`),
					DomainIdentifier: ptr(strExpr(`vars["namespace"]`)),
					LayerName:        strExpr(`"backend"`),
					LayerIdentifier:  ptr(strExpr(`"backend_id"`)),
					Optional: &settings.OtelComponentMappingFieldMapping{
						Tags: &map[string]settings.OtelStringExpression{
							"priority":     {"spanAttributes.priority"},
							"scopeName":    {"scopeAttributes.name"},
							"resourceName": {"resourceAttributes.name"},
						},
					},
					Required: &settings.OtelComponentMappingFieldMapping{
						Tags: &map[string]settings.OtelStringExpression{
							"kind":   {`spanAttributes["kind"]`},
							"amount": {`spanAttributes.amount`},
						},
					},
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			vars: &map[string]string{
				"namespace": "payments_ns",
			},
			want: &topo_stream_v1.TopologyStreamComponent{
				ExternalId:       "billing",
				Identifiers:      []string{},
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
					Identifier: strExpr(`spanAttributes["service.name"]`),
					Name:       strExpr(`spanAttributes["service.name"]`),
					TypeName:   strExpr(`"service"`),
					DomainName: strExpr(`"payment"`),
					LayerName:  strExpr(`"backend"`),
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			vars:     &map[string]string{},
			want: &topo_stream_v1.TopologyStreamComponent{
				ExternalId:  "billing",
				Identifiers: []string{},
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
					Identifier:       strExpr(`spanAttributes["service.name"]`),
					Name:             strExpr(`spanAttributes["non-existing-attr"]`),
					TypeName:         strExpr(`"service"`),
					TypeIdentifier:   ptr(strExpr(`"service_id"`)),
					DomainName:       strExpr(`vars["non-existing-var"]`),
					DomainIdentifier: ptr(strExpr("vars.namespace")),
					LayerName:        strExpr(`"backend"`),
					LayerIdentifier:  ptr(strExpr(`"backend_id"`)),
					Optional: &settings.OtelComponentMappingFieldMapping{
						Tags: &map[string]settings.OtelStringExpression{
							"priority": {"spanAttributes.priority"},
						},
					},
					Required: &settings.OtelComponentMappingFieldMapping{
						Tags: &map[string]settings.OtelStringExpression{
							"kind":   {"spanAttributes.kind"},
							"amount": {"spanAttributes.amount"},
						},
					},
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			vars: &map[string]string{
				"namespace": "payments_ns",
			},
			want:      nil,
			expectErr: []error{errors.New("CEL evaluation error: no such key: non-existing-attr"), errors.New("CEL evaluation error: no such key: non-existing-var")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval, _ := NewCELEvaluator()
			evalCtx := ExpressionEvalContext{*tt.span, *tt.scope, *tt.resource, *tt.vars}
			got, err := MapComponent(tt.mapping, eval, &evalCtx)
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

	tests := []struct {
		name      string
		mapping   *settings.OtelRelationMapping
		span      *ptrace.Span
		scope     *ptrace.ScopeSpans
		resource  *ptrace.ResourceSpans
		vars      *map[string]string
		want      *topo_stream_v1.TopologyStreamRelation
		expectErr []error
	}{
		{
			name: "valid relation mapping",
			mapping: &settings.OtelRelationMapping{
				Output: settings.OtelRelationMappingOutput{
					SourceId:       strExpr(`spanAttributes["service.name"]`),
					TargetId:       strExpr(`"database"`),
					TypeName:       strExpr(`"query"`),
					TypeIdentifier: ptr(strExpr("spanAttributes.kind")),
				},
			},
			span:     &testSpan,
			scope:    &testScope,
			resource: &testResource,
			vars:     &map[string]string{},
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
					SourceId: strExpr(`spanAttributes["non-existing"]`),
					TargetId: strExpr(`"database"`),
					TypeName: strExpr(`"query"`),
				},
			},
			span:      &testSpan,
			scope:     &testScope,
			resource:  &testResource,
			vars:      &map[string]string{},
			want:      nil,
			expectErr: []error{errors.New("CEL evaluation error: no such key: non-existing")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval, _ := NewCELEvaluator()
			evalCtx := ExpressionEvalContext{*tt.span, *tt.scope, *tt.resource, *tt.vars}
			got, err := MapRelation(tt.mapping, eval, &evalCtx)
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
