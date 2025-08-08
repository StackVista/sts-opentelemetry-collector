package internal

import (
	"errors"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	topo_stream_v1 "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/topostream/topo_stream.v1"
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

	tests := []struct {
		name      string
		mapping   *settings.OtelComponentMapping
		span      *ptrace.Span
		vars      *map[string]string
		want      *topo_stream_v1.TopologyStreamComponent
		expectErr error
	}{
		{
			name: "valid mapping with all required fields",
			mapping: &settings.OtelComponentMapping{
				Output: settings.OtelComponentMappingOutput{
					Identifier:       settings.OtelStringExpression{"attributes.service.name"},
					Name:             settings.OtelStringExpression{"attributes.service.name"},
					TypeName:         settings.OtelStringExpression{"service"},
					TypeIdentifier:   &settings.OtelStringExpression{"service_id"},
					DomainName:       settings.OtelStringExpression{"vars.namespace"},
					DomainIdentifier: &settings.OtelStringExpression{"vars.namespace"},
					LayerName:        settings.OtelStringExpression{"backend"},
					LayerIdentifier:  &settings.OtelStringExpression{"backend_id"},
					Optional: &settings.OtelComponentMappingFieldMapping{
						Tags: &map[string]settings.OtelStringExpression{
							"priority": {"attributes.priority"},
						},
					},
					Required: &settings.OtelComponentMappingFieldMapping{
						Tags: &map[string]settings.OtelStringExpression{
							"kind":   {"attributes.kind"},
							"amount": {"attributes.amount"},
						},
					},
				},
			},
			span: &testSpan,
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
				Tags:             []string{"priority:urgent", "kind:licence", "amount:1000"},
			},
			expectErr: nil,
		},
		{
			name: "valid mapping with minimal set of properties",
			mapping: &settings.OtelComponentMapping{
				Output: settings.OtelComponentMappingOutput{
					Identifier: settings.OtelStringExpression{"attributes.service.name"},
					Name:       settings.OtelStringExpression{"attributes.service.name"},
					TypeName:   settings.OtelStringExpression{"service"},
					DomainName: settings.OtelStringExpression{"payment"},
					LayerName:  settings.OtelStringExpression{"backend"},
				},
			},
			span: &testSpan,
			vars: &map[string]string{},
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
					Identifier:       settings.OtelStringExpression{"attributes.service.name"},
					Name:             settings.OtelStringExpression{"attributes.non-existing-attr"},
					TypeName:         settings.OtelStringExpression{"service"},
					TypeIdentifier:   &settings.OtelStringExpression{"service_id"},
					DomainName:       settings.OtelStringExpression{"vars.non-existing-var"},
					DomainIdentifier: &settings.OtelStringExpression{"vars.namespace"},
					LayerName:        settings.OtelStringExpression{"backend"},
					LayerIdentifier:  &settings.OtelStringExpression{"backend_id"},
					Optional: &settings.OtelComponentMappingFieldMapping{
						Tags: &map[string]settings.OtelStringExpression{
							"priority": {"attributes.priority"},
						},
					},
					Required: &settings.OtelComponentMappingFieldMapping{
						Tags: &map[string]settings.OtelStringExpression{
							"kind":   {"attributes.kind"},
							"amount": {"attributes.amount"},
						},
					},
				},
			},
			span: &testSpan,
			vars: &map[string]string{
				"namespace": "payments_ns",
			},
			want: &topo_stream_v1.TopologyStreamComponent{
				ExternalId:       "billing",
				Identifiers:      []string{},
				Name:             "",
				TypeName:         "service",
				TypeIdentifier:   Ptr("service_id"),
				DomainName:       "",
				DomainIdentifier: Ptr("payments_ns"),
				LayerName:        "backend",
				LayerIdentifier:  Ptr("backend_id"),
				Tags:             []string{"priority:urgent", "kind:licence", "amount:1000"},
			},
			expectErr: errors.New("Not found attribute with name: non-existing-attr\nNot found variable with name: non-existing-var"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MapComponent(tt.mapping, tt.span, tt.vars)
			if tt.expectErr != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectErr.Error())
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
