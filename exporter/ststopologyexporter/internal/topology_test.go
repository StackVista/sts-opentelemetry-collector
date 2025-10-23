package internal_test

import (
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/ststopologyexporter/internal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestTopology_addResource(t *testing.T) {
	collection := internal.NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("service.name", "demo 1")
	attrs.PutStr("service.namespace", "demo")
	attrs.PutStr("telemetry.sdk.language", "go")
	attrs.PutStr("Resource Attributes 1", "value1")
	collection.AddResource(&attrs)

	components := collection.GetComponents()
	//nolint:dupl
	require.Equal(t, []*internal.Component{
		{
			ExternalID: "urn:opentelemetry:namespace/demo",
			Type: internal.ComponentType{
				Name: "namespace",
			},
			Data: &internal.ComponentData{
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
			ExternalID: "urn:opentelemetry:namespace/demo:service/demo 1",
			Type: internal.ComponentType{
				Name: "service",
			},
			Data: &internal.ComponentData{
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
			ExternalID: "urn:opentelemetry:namespace/demo:service/demo 1:serviceInstance/demo 1",
			Type: internal.ComponentType{
				Name: "service-instance",
			},
			Data: &internal.ComponentData{
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
	require.Equal(t, []*internal.Relation{
		{
			ExternalID: "urn:opentelemetry:namespace/demo:service/demo 1-urn:opentelemetry:namespace/demo:service/demo 1:serviceInstance/demo 1",
			SourceID:   "urn:opentelemetry:namespace/demo:service/demo 1",
			TargetID:   "urn:opentelemetry:namespace/demo:service/demo 1:serviceInstance/demo 1",
			Type: internal.RelationType{
				Name: "provided-by",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}

func TestTopology_addSynchronousConnection(t *testing.T) {
	collection := internal.NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("client", "frontend")
	attrs.PutStr("client_service.namespace", "ns")
	attrs.PutStr("server", "backend")
	attrs.PutStr("server_service.namespace", "ns")
	attrs.PutStr("connection_type", "")
	ok := collection.AddConnection(&attrs)
	require.True(t, ok)

	relations := collection.GetRelations()
	require.Equal(t, []*internal.Relation{
		{
			ExternalID: "urn:opentelemetry:namespace/ns:service/frontend:serviceInstance/frontend-urn:opentelemetry:namespace/ns:service/backend:serviceInstance/backend",
			SourceID:   "urn:opentelemetry:namespace/ns:service/frontend:serviceInstance/frontend",
			TargetID:   "urn:opentelemetry:namespace/ns:service/backend:serviceInstance/backend",
			Type: internal.RelationType{
				Name: "synchronous",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}

func TestTopology_failIncompleteConnection(t *testing.T) {
	collection := internal.NewCollection()
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
	collection := internal.NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("client", "frontend")
	attrs.PutStr("client_service.namespace", "ns")
	attrs.PutStr("server", "backend")
	attrs.PutStr("server_service.namespace", "ns")
	attrs.PutStr("connection_type", "messaging_system")
	ok := collection.AddConnection(&attrs)
	require.True(t, ok)

	relations := collection.GetRelations()
	require.Equal(t, []*internal.Relation{
		{
			ExternalID: "urn:opentelemetry:namespace/ns:service/frontend:serviceInstance/frontend-urn:opentelemetry:namespace/ns:service/backend:serviceInstance/backend",
			SourceID:   "urn:opentelemetry:namespace/ns:service/frontend:serviceInstance/frontend",
			TargetID:   "urn:opentelemetry:namespace/ns:service/backend:serviceInstance/backend",
			Type: internal.RelationType{
				Name: "asynchronous",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}

func TestTopology_addDatabase(t *testing.T) {
	collection := internal.NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("client", "frontend")
	attrs.PutStr("client_service.namespace", "ns")
	attrs.PutStr("server", "mydb")
	attrs.PutStr("connection_type", "database")
	ok := collection.AddConnection(&attrs)
	require.True(t, ok)

	relations := collection.GetRelations()
	require.Equal(t, []*internal.Relation{
		{
			ExternalID: "urn:opentelemetry:namespace/ns:service/frontend:serviceInstance/frontend-urn:opentelemetry:namespace/ns:service/frontend:database/mydb",
			SourceID:   "urn:opentelemetry:namespace/ns:service/frontend:serviceInstance/frontend",
			TargetID:   "urn:opentelemetry:namespace/ns:service/frontend:database/mydb",
			Type: internal.RelationType{
				Name: "database",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)

	components := collection.GetComponents()
	require.Equal(t, []*internal.Component{
		{
			ExternalID: "urn:opentelemetry:namespace/ns:service/frontend:database/mydb",
			Type: internal.ComponentType{
				Name: "database",
			},
			Data: &internal.ComponentData{
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

//nolint:dupl
func TestTopology_addHost(t *testing.T) {
	collection := internal.NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("service.name", "ye-service")
	attrs.PutStr("service.namespace", "ns")
	attrs.PutStr("host.id", "ye-host")
	ok := collection.AddResource(&attrs)
	require.True(t, ok)

	components := collection.GetComponents()
	require.Equal(t, []*internal.Component{
		{
			ExternalID: "urn:opentelemetry:host/ye-host",
			Type: internal.ComponentType{
				Name: "host",
			},
			Data: &internal.ComponentData{
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
			ExternalID: "urn:opentelemetry:namespace/ns",
			Type: internal.ComponentType{
				Name: "namespace",
			},
			Data: &internal.ComponentData{
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
			ExternalID: "urn:opentelemetry:namespace/ns:service/ye-service",
			Type: internal.ComponentType{
				Name: "service",
			},
			Data: &internal.ComponentData{
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
			ExternalID: "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: internal.ComponentType{
				Name: "service-instance",
			},
			Data: &internal.ComponentData{
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
	require.Equal(t, []*internal.Relation{
		{
			ExternalID: "urn:opentelemetry:host/ye-host-urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			SourceID:   "urn:opentelemetry:host/ye-host",
			TargetID:   "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: internal.RelationType{
				Name: "executes",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
		{
			ExternalID: "urn:opentelemetry:namespace/ns:service/ye-service-urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			SourceID:   "urn:opentelemetry:namespace/ns:service/ye-service",
			TargetID:   "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: internal.RelationType{
				Name: "provided-by",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}

//nolint:dupl
func TestTopology_addFaas(t *testing.T) {
	collection := internal.NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("service.name", "ye-service")
	attrs.PutStr("service.namespace", "ns")
	attrs.PutStr("faas.id", "ye-faas")
	ok := collection.AddResource(&attrs)
	require.True(t, ok)

	components := collection.GetComponents()
	require.Equal(t, []*internal.Component{
		{
			ExternalID: "urn:opentelemetry:function/ye-faas",
			Type: internal.ComponentType{
				Name: "function",
			},
			Data: &internal.ComponentData{
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
			ExternalID: "urn:opentelemetry:namespace/ns",
			Type: internal.ComponentType{
				Name: "namespace",
			},
			Data: &internal.ComponentData{
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
			ExternalID: "urn:opentelemetry:namespace/ns:service/ye-service",
			Type: internal.ComponentType{
				Name: "service",
			},
			Data: &internal.ComponentData{
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
			ExternalID: "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: internal.ComponentType{
				Name: "service-instance",
			},
			Data: &internal.ComponentData{
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
	require.Equal(t, []*internal.Relation{
		{
			ExternalID: "urn:opentelemetry:function/ye-faas-urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			SourceID:   "urn:opentelemetry:function/ye-faas",
			TargetID:   "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: internal.RelationType{
				Name: "executes",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
		{
			ExternalID: "urn:opentelemetry:namespace/ns:service/ye-service-urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			SourceID:   "urn:opentelemetry:namespace/ns:service/ye-service",
			TargetID:   "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: internal.RelationType{
				Name: "provided-by",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}

func TestTopology_addKubernetes(t *testing.T) {
	collection := internal.NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("service.name", "ye-service")
	attrs.PutStr("service.namespace", "ns")
	attrs.PutStr("k8s.cluster.name", "ye-cluster")
	attrs.PutStr("k8s.namespace.name", "ye-k8s-namespace")
	attrs.PutStr("k8s.pod.name", "ye-pod-name")
	ok := collection.AddResource(&attrs)
	require.True(t, ok)

	components := collection.GetComponents()
	require.Equal(t, []*internal.Component{
		{
			ExternalID: "urn:opentelemetry:kubernetes:/ye-cluster:ye-k8s-namespace:pod/ye-pod-name",
			Type: internal.ComponentType{
				Name: "pod",
			},
			Data: &internal.ComponentData{
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
					"k8s-scope":    "ye-cluster/ye-k8s-namespace",
					"namespace":    "ye-k8s-namespace",
				},
			},
		},
		{
			ExternalID: "urn:opentelemetry:namespace/ns",
			Type: internal.ComponentType{
				Name: "namespace",
			},
			Data: &internal.ComponentData{
				Name:        "ns",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:applications",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"cluster-name": "ye-cluster",
					"k8s-scope":    "ye-cluster/ye-k8s-namespace",
					"namespace":    "ye-k8s-namespace",
				},
			},
		},
		{
			ExternalID: "urn:opentelemetry:namespace/ns:service/ye-service",
			Type: internal.ComponentType{
				Name: "service",
			},
			Data: &internal.ComponentData{
				Name:        "ye-service",
				Version:     "",
				Layer:       "urn:stackpack:common:layer:services",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"cluster-name":      "ye-cluster",
					"k8s-scope":         "ye-cluster/ye-k8s-namespace",
					"namespace":         "ye-k8s-namespace",
					"service.name":      "ye-service",
					"service.namespace": "ns",
				},
			},
		},
		{
			ExternalID: "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: internal.ComponentType{
				Name: "service-instance",
			},
			Data: &internal.ComponentData{
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
					"cluster-name":       "ye-cluster",
					"k8s-scope":          "ye-cluster/ye-k8s-namespace",
					"namespace":          "ye-k8s-namespace",
					"service.name":       "ye-service",
					"service.namespace":  "ns",
				},
			},
		},
	}, components)

	relations := collection.GetRelations()
	require.Equal(t, []*internal.Relation{
		{
			ExternalID: "urn:opentelemetry:kubernetes:/ye-cluster:ye-k8s-namespace:pod/ye-pod-name-urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			SourceID:   "urn:opentelemetry:kubernetes:/ye-cluster:ye-k8s-namespace:pod/ye-pod-name",
			TargetID:   "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: internal.RelationType{
				Name: "kubernetes-to-otel",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
		{
			ExternalID: "urn:opentelemetry:namespace/ns:service/ye-service-urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			SourceID:   "urn:opentelemetry:namespace/ns:service/ye-service",
			TargetID:   "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: internal.RelationType{
				Name: "provided-by",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}

func TestTopology_addResourceWithoutNamespace(t *testing.T) {
	collection := internal.NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr("service.name", "demo 1")
	attrs.PutStr("telemetry.sdk.language", "go")
	attrs.PutStr("Resource Attributes 1", "value1")
	collection.AddResource(&attrs)

	components := collection.GetComponents()
	//nolint:dupl
	require.Equal(t, []*internal.Component{
		{
			ExternalID: "urn:opentelemetry:namespace/default",
			Type: internal.ComponentType{
				Name: "namespace",
			},
			Data: &internal.ComponentData{
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
			ExternalID: "urn:opentelemetry:namespace/default:service/demo 1",
			Type: internal.ComponentType{
				Name: "service",
			},
			Data: &internal.ComponentData{
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
			ExternalID: "urn:opentelemetry:namespace/default:service/demo 1:serviceInstance/demo 1",
			Type: internal.ComponentType{
				Name: "service-instance",
			},
			Data: &internal.ComponentData{
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
	require.Equal(t, []*internal.Relation{
		{
			ExternalID: "urn:opentelemetry:namespace/default:service/demo 1-urn:opentelemetry:namespace/default:service/demo 1:serviceInstance/demo 1",
			SourceID:   "urn:opentelemetry:namespace/default:service/demo 1",
			TargetID:   "urn:opentelemetry:namespace/default:service/demo 1:serviceInstance/demo 1",
			Type: internal.RelationType{
				Name: "provided-by",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}
