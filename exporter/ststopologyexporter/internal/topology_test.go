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
	collection.AddResource(&attrs, false)

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
				Name:        "demo 1 - instance",
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

func TestTopology_addSynchronousConnection(t *testing.T) {
	collection := NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("client", "frontend")
	attrs.PutStr("client_service.namespace", "ns")
	attrs.PutStr("server", "backend")
	attrs.PutStr("server_service.namespace", "ns")
	attrs.PutStr("connection_type", "")
	ok := collection.AddConnection(&attrs)
	require.True(t, ok)

	relations := collection.GetRelations()
	require.Equal(t, []*Relation{
		{
			ExternalId: "urn:opentelemetry:namespace/ns:service/frontend:serviceInstance/frontend-urn:opentelemetry:namespace/ns:service/backend:serviceInstance/backend",
			SourceId:   "urn:opentelemetry:namespace/ns:service/frontend:serviceInstance/frontend",
			TargetId:   "urn:opentelemetry:namespace/ns:service/backend:serviceInstance/backend",
			Type: RelationType{
				Name: "synchronous",
			},
			Data: &RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}

func TestTopology_failIncompleteConnection(t *testing.T) {
	collection := NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("client", "frontend")
	attrs.PutStr("client_service.namespace", "ns")
	attrs.PutStr("server", "backend")
	attrs.PutStr("connection_type", "")
	ok := collection.AddConnection(&attrs)
	require.False(t, ok)

	attrs.Remove("client_service.namespace")
	attrs.PutStr("server_service.namespace", "ns")
	ok = collection.AddConnection(&attrs)
	require.False(t, ok)

	attrs.Remove("client")
	attrs.PutStr("client_service.namespace", "ns")
	ok = collection.AddConnection(&attrs)
	require.False(t, ok)
}

func TestTopology_addAsynchronousConnection(t *testing.T) {
	collection := NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("client", "frontend")
	attrs.PutStr("client_service.namespace", "ns")
	attrs.PutStr("server", "backend")
	attrs.PutStr("server_service.namespace", "ns")
	attrs.PutStr("connection_type", "messaging_system")
	ok := collection.AddConnection(&attrs)
	require.True(t, ok)

	relations := collection.GetRelations()
	require.Equal(t, []*Relation{
		{
			ExternalId: "urn:opentelemetry:namespace/ns:service/frontend:serviceInstance/frontend-urn:opentelemetry:namespace/ns:service/backend:serviceInstance/backend",
			SourceId:   "urn:opentelemetry:namespace/ns:service/frontend:serviceInstance/frontend",
			TargetId:   "urn:opentelemetry:namespace/ns:service/backend:serviceInstance/backend",
			Type: RelationType{
				Name: "asynchronous",
			},
			Data: &RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}

func TestTopology_addDatabase(t *testing.T) {
	collection := NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("client", "frontend")
	attrs.PutStr("client_service.namespace", "ns")
	attrs.PutStr("server", "mydb")
	attrs.PutStr("connection_type", "database")
	ok := collection.AddConnection(&attrs)
	require.True(t, ok)

	relations := collection.GetRelations()
	require.Equal(t, []*Relation{
		{
			ExternalId: "urn:opentelemetry:namespace/ns:service/frontend:serviceInstance/frontend-urn:opentelemetry:namespace/ns:service/frontend:database/mydb",
			SourceId:   "urn:opentelemetry:namespace/ns:service/frontend:serviceInstance/frontend",
			TargetId:   "urn:opentelemetry:namespace/ns:service/frontend:database/mydb",
			Type: RelationType{
				Name: "database",
			},
			Data: &RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)

	components := collection.GetComponents()
	require.Equal(t, []*Component{
		{
			ExternalId: "urn:opentelemetry:namespace/ns:service/frontend:database/mydb",
			Type: ComponentType{
				Name: "database",
			},
			Data: &ComponentData{
				Name:        "mydb",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:databases",
				Domain:      "",
				Environment: "",
				Tags:        map[string]string{},
			},
		},
	}, components)
}
