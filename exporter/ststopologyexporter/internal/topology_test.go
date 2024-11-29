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
				Identifiers: []string{},
				Tags:        map[string]string{},
			},
		},
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
				Identifiers: []string{},
				Tags: map[string]string{
					"service.name":           "demo 1",
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
				Identifiers: []string{},
				Tags: map[string]string{
					"Resource Attributes 1":  "value1",
					"service.name":           "demo 1",
					"service.namespace":      "demo",
					"telemetry.sdk.language": "go",
				},
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
				Name: "provided-by",
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
				Identifiers: []string{},
				Tags: map[string]string{
					"service.name":      "frontend",
					"service.namespace": "ns",
				},
			},
		},
	}, components)
}

func TestTopology_addHost(t *testing.T) {
	collection := NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("service.name", "ye-service")
	attrs.PutStr("service.namespace", "ns")
	attrs.PutStr("host.id", "ye-host")
	ok := collection.AddResource(&attrs)
	require.True(t, ok)

	components := collection.GetComponents()
	require.Equal(t, []*Component{
		{
			ExternalId: "urn:opentelemetry:host/ye-host",
			Type: ComponentType{
				Name: "host",
			},
			Data: &ComponentData{
				Name:        "ye-host",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:machines",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"host.id": "ye-host",
				},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/ns",
			Type: ComponentType{
				Name: "namespace",
			},
			Data: &ComponentData{
				Name:        "ns",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:applications",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags:        map[string]string{},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/ns:service/ye-service",
			Type: ComponentType{
				Name: "service",
			},
			Data: &ComponentData{
				Name:        "ye-service",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:services",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"service.name":      "ye-service",
					"service.namespace": "ns",
				},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: ComponentType{
				Name: "service-instance",
			},
			Data: &ComponentData{
				Name:        "ye-service - instance",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:containers",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"host.id":           "ye-host",
					"service.name":      "ye-service",
					"service.namespace": "ns",
				},
			},
		},
	}, components)

	relations := collection.GetRelations()
	require.Equal(t, []*Relation{
		{
			ExternalId: "urn:opentelemetry:host/ye-host-urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			SourceId:   "urn:opentelemetry:host/ye-host",
			TargetId:   "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: RelationType{
				Name: "executes",
			},
			Data: &RelationData{
				Tags: map[string]string{},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/ns:service/ye-service-urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			SourceId:   "urn:opentelemetry:namespace/ns:service/ye-service",
			TargetId:   "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: RelationType{
				Name: "provided-by",
			},
			Data: &RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}

func TestTopology_addFaas(t *testing.T) {
	collection := NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("service.name", "ye-service")
	attrs.PutStr("service.namespace", "ns")
	attrs.PutStr("faas.id", "ye-faas")
	ok := collection.AddResource(&attrs)
	require.True(t, ok)

	components := collection.GetComponents()
	require.Equal(t, []*Component{
		{
			ExternalId: "urn:opentelemetry:function/ye-faas",
			Type: ComponentType{
				Name: "function",
			},
			Data: &ComponentData{
				Name:        "ye-faas",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:serverless",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"faas.id": "ye-faas",
				},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/ns",
			Type: ComponentType{
				Name: "namespace",
			},
			Data: &ComponentData{
				Name:        "ns",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:applications",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags:        map[string]string{},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/ns:service/ye-service",
			Type: ComponentType{
				Name: "service",
			},
			Data: &ComponentData{
				Name:        "ye-service",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:services",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"service.name":      "ye-service",
					"service.namespace": "ns",
				},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: ComponentType{
				Name: "service-instance",
			},
			Data: &ComponentData{
				Name:        "ye-service - instance",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:containers",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"faas.id":           "ye-faas",
					"service.name":      "ye-service",
					"service.namespace": "ns",
				},
			},
		},
	}, components)

	relations := collection.GetRelations()
	require.Equal(t, []*Relation{
		{
			ExternalId: "urn:opentelemetry:function/ye-faas-urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			SourceId:   "urn:opentelemetry:function/ye-faas",
			TargetId:   "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: RelationType{
				Name: "executes",
			},
			Data: &RelationData{
				Tags: map[string]string{},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/ns:service/ye-service-urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			SourceId:   "urn:opentelemetry:namespace/ns:service/ye-service",
			TargetId:   "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: RelationType{
				Name: "provided-by",
			},
			Data: &RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}

func TestTopology_addKubernetes(t *testing.T) {
	collection := NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("service.name", "ye-service")
	attrs.PutStr("service.namespace", "ns")
	attrs.PutStr("k8s.cluster.name", "ye-cluster")
	attrs.PutStr("k8s.namespace.name", "ye-k8s-namespace")
	attrs.PutStr("k8s.pod.name", "ye-pod-name")
	ok := collection.AddResource(&attrs)
	require.True(t, ok)

	components := collection.GetComponents()
	require.Equal(t, []*Component{
		{
			ExternalId: "urn:opentelemetry:kubernetes:/ye-cluster:ye-k8s-namespace:pod/ye-pod-name",
			Type: ComponentType{
				Name: "pod",
			},
			Data: &ComponentData{
				Name:        "ye-pod-name",
				Version:     "",
				Layer:       "",
				Domain:      "",
				Environment: "",
				Identifiers: []string{
					"urn:kubernetes:/ye-cluster:ye-k8s-namespace:pod/ye-pod-name",
				},
				Tags: map[string]string{
					"cluster-name": "ye-cluster",
					"namespace":    "ye-k8s-namespace",
				},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/ns",
			Type: ComponentType{
				Name: "namespace",
			},
			Data: &ComponentData{
				Name:        "ns",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:applications",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags:        map[string]string{},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/ns:service/ye-service",
			Type: ComponentType{
				Name: "service",
			},
			Data: &ComponentData{
				Name:        "ye-service",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:services",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"service.name":      "ye-service",
					"service.namespace": "ns",
				},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: ComponentType{
				Name: "service-instance",
			},
			Data: &ComponentData{
				Name:        "ye-service - instance",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:containers",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"k8s.cluster.name":   "ye-cluster",
					"k8s.namespace.name": "ye-k8s-namespace",
					"k8s.pod.name":       "ye-pod-name",
					"service.name":       "ye-service",
					"service.namespace":  "ns",
				},
			},
		},
	}, components)

	relations := collection.GetRelations()
	require.Equal(t, []*Relation{
		{
			ExternalId: "urn:opentelemetry:kubernetes:/ye-cluster:ye-k8s-namespace:pod/ye-pod-name-urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			SourceId:   "urn:opentelemetry:kubernetes:/ye-cluster:ye-k8s-namespace:pod/ye-pod-name",
			TargetId:   "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: RelationType{
				Name: "kubernetes-to-otel",
			},
			Data: &RelationData{
				Tags: map[string]string{},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/ns:service/ye-service-urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			SourceId:   "urn:opentelemetry:namespace/ns:service/ye-service",
			TargetId:   "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: RelationType{
				Name: "provided-by",
			},
			Data: &RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}

func TestTopology_addResourceWithoutNamespace(t *testing.T) {
	collection := NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("service.name", "demo 1")
	attrs.PutStr("telemetry.sdk.language", "go")
	attrs.PutStr("Resource Attributes 1", "value1")
	collection.AddResource(&attrs)

	components := collection.GetComponents()
	require.Equal(t, []*Component{
		{
			ExternalId: "urn:opentelemetry:namespace/default",
			Type: ComponentType{
				Name: "namespace",
			},
			Data: &ComponentData{
				Name:        "default",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:applications",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags:        map[string]string{},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/default:service/demo 1",
			Type: ComponentType{
				Name: "service",
			},
			Data: &ComponentData{
				Name:        "demo 1",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:services",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"service.name":           "demo 1",
					"service.namespace":      "default",
					"telemetry.sdk.language": "go",
				},
			},
		},
		{
			ExternalId: "urn:opentelemetry:namespace/default:service/demo 1:serviceInstance/demo 1",
			Type: ComponentType{
				Name: "service-instance",
			},
			Data: &ComponentData{
				Name:        "demo 1 - instance",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:containers",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"Resource Attributes 1":  "value1",
					"service.name":           "demo 1",
					"service.namespace":      "default",
					"telemetry.sdk.language": "go",
				},
			},
		},
	}, components)

	relations := collection.GetRelations()
	require.Equal(t, []*Relation{
		{
			ExternalId: "urn:opentelemetry:namespace/default:service/demo 1-urn:opentelemetry:namespace/default:service/demo 1:serviceInstance/demo 1",
			SourceId:   "urn:opentelemetry:namespace/default:service/demo 1",
			TargetId:   "urn:opentelemetry:namespace/default:service/demo 1:serviceInstance/demo 1",
			Type: RelationType{
				Name: "provided-by",
			},
			Data: &RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}
