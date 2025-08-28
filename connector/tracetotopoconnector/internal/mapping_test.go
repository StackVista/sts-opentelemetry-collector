package internal_test

import (
	"errors"
	"sort"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"

	topo_stream_v1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/internal"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func Ptr[T any](v T) *T { return &v }

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
		vars      *map[string]string
		want      *topo_stream_v1.TopologyStreamComponent
		expectErr []error
	}{
		{
			name: "valid mapping with all required fields",
			mapping: &settings.OtelComponentMapping{
				Output: settings.OtelComponentMappingOutput{
					Identifier:       settings.OtelStringExpression{"spanAttributes.service.name"},
					Name:             settings.OtelStringExpression{"spanAttributes.service.name"},
					TypeName:         settings.OtelStringExpression{"service"},
					TypeIdentifier:   &settings.OtelStringExpression{"service_id"},
					DomainName:       settings.OtelStringExpression{"vars.namespace"},
					DomainIdentifier: &settings.OtelStringExpression{"vars.namespace"},
					LayerName:        settings.OtelStringExpression{"backend"},
					LayerIdentifier:  &settings.OtelStringExpression{"backend_id"},
					Optional: &settings.OtelComponentMappingFieldMapping{
						Tags: &map[string]settings.OtelStringExpression{
							"priority":     {"spanAttributes.priority"},
							"scopeName":    {"scopeAttributes.name"},
							"resourceName": {"resourceAttributes.name"},
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
			want: &topo_stream_v1.TopologyStreamComponent{
				ExternalId:       "billing",
				Identifiers:      []string{},
				Name:             "billing",
				TypeName:         "service",
				TypeIdentifier:   Ptr("service_id"),
				DomainName:       "payments_ns",
				DomainIdentifier: Ptr("payments_ns"),
				LayerName:        "backend",
				LayerIdentifier:  Ptr("backend_id"),
				Tags:             []string{"priority:urgent", "kind:licence", "amount:1000", "scopeName:kamon", "resourceName:microservice"},
			},
			expectErr: nil,
		},
		{
			name: "valid mapping with minimal set of properties",
			mapping: &settings.OtelComponentMapping{
				Output: settings.OtelComponentMappingOutput{
					Identifier: settings.OtelStringExpression{"spanAttributes.service.name"},
					Name:       settings.OtelStringExpression{"spanAttributes.service.name"},
					TypeName:   settings.OtelStringExpression{"service"},
					DomainName: settings.OtelStringExpression{"payment"},
					LayerName:  settings.OtelStringExpression{"backend"},
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
					Identifier:       settings.OtelStringExpression{"spanAttributes.service.name"},
					Name:             settings.OtelStringExpression{"spanAttributes.non-existing-attr"},
					TypeName:         settings.OtelStringExpression{"service"},
					TypeIdentifier:   &settings.OtelStringExpression{"service_id"},
					DomainName:       settings.OtelStringExpression{"vars.non-existing-var"},
					DomainIdentifier: &settings.OtelStringExpression{"vars.namespace"},
					LayerName:        settings.OtelStringExpression{"backend"},
					LayerIdentifier:  &settings.OtelStringExpression{"backend_id"},
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
			expectErr: []error{errors.New("Not found span attribute with name: non-existing-attr"), errors.New("Not found variable with name: non-existing-var")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := internal.MapComponent(tt.mapping, tt.span, tt.scope, tt.resource, tt.vars)
			assert.Equal(t, tt.expectErr, err)
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

	//nolint:govet
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
					SourceId:       settings.OtelStringExpression{"spanAttributes.service.name"},
					TargetId:       settings.OtelStringExpression{"database"},
					TypeName:       settings.OtelStringExpression{"query"},
					TypeIdentifier: &settings.OtelStringExpression{"spanAttributes.kind"},
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
				TypeIdentifier:   Ptr("licence"),
				Tags:             nil,
			},
			expectErr: nil,
		},
		{
			name: "missing mandatory attributes",
			mapping: &settings.OtelRelationMapping{
				Output: settings.OtelRelationMappingOutput{
					SourceId: settings.OtelStringExpression{"spanAttributes.non-existing"},
					TargetId: settings.OtelStringExpression{"database"},
					TypeName: settings.OtelStringExpression{"query"},
				},
			},
			span:      &testSpan,
			scope:     &testScope,
			resource:  &testResource,
			vars:      &map[string]string{},
			want:      nil,
			expectErr: []error{errors.New("Not found span attribute with name: non-existing")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := internal.MapRelation(tt.mapping, tt.span, tt.scope, tt.resource, tt.vars)
			assert.Equal(t, tt.expectErr, err)
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
