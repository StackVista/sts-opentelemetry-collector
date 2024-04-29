package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestTopology_addResource(t *testing.T) {
	collection := NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("service.name", "demo 1")
	attrs.PutStr("service.namespace", "demo")
	attrs.PutStr("telemetry.sdk.language", "go")
	attrs.PutStr("Resource Attributes 1", "value1")
	collection.AddResource(&attrs)

	components := collection.GetComponents()
	require.Equal(t, []*Component{
		{
			ExternalId: "urn:opentelemetry:namespace/demo:service/demo 1",
			Type: ComponentType{
				Name: "service",
			},
			Data: &ComponentData{
				Name:        "demo 1",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:services",
				Domain:      "",
				Environment: "",
				Tags: map[string]string{
					"service.namespace":      "demo",
					"telemetry.sdk.language": "go",
				},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/demo:service/demo 1:serviceInstance/demo 1",
			Type: ComponentType{
				Name: "service-instance",
			},
			Data: &ComponentData{
				Name:        "",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:containers",
				Domain:      "",
				Environment: "",
				Tags: map[string]string{
					"Resource Attributes 1":  "value1",
					"service.name":           "demo 1",
					"service.namespace":      "demo",
					"telemetry.sdk.language": "go",
				},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/demo",
			Type: ComponentType{
				Name: "namespace",
			},
			Data: &ComponentData{
				Name:        "demo",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:applications",
				Domain:      "",
				Environment: "",
				Tags:        map[string]string{},
			},
		},
	}, components)

	relations := collection.GetRelations()
	require.Equal(t, []*Relation{
		{
			ExternalId: "urn:opentelemetry:namespace/demo:service/demo 1-urn:opentelemetry:namespace/demo:service/demo 1:serviceInstance/demo 1",
			SourceId:   "urn:opentelemetry:namespace/demo:service/demo 1",
			TargetId:   "urn:opentelemetry:namespace/demo:service/demo 1:serviceInstance/demo 1",
			Type: RelationType{
				Name: "provided by",
			},
			Data: &RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}
