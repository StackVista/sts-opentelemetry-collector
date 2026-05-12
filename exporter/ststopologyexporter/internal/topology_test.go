package internal_test

import (
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/ststopologyexporter/internal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const (
	attrServiceName       = "service.name"
	attrServiceNamespace  = "service.namespace"
	attrTelemetryLanguage = "telemetry.sdk.language"
	attrNamespace         = "namespace"

	typeNamespace       = "namespace"
	typeService         = "service"
	typeServiceInstance = "service-instance"
	relationProvidedBy  = "provided-by"

	layerApplications = "urn:stackpack:common:layer:applications"
	layerServices     = "urn:stackpack:common:layer:services"
	layerContainers   = "urn:stackpack:common:layer:containers"

	demoNamespace        = "demo"
	demoService          = "demo 1"
	frontendSourceID     = "urn:opentelemetry:namespace/ns:service/frontend:serviceInstance/frontend"
	hostName             = "ye-host"
	nsExternalID         = "urn:opentelemetry:namespace/ns"
	yeServiceName        = "ye-service"
	yeServiceExternalID  = "urn:opentelemetry:namespace/ns:service/ye-service"
	yeServiceInstanceID  = "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service"
	yeServiceInstance    = "ye-service - instance"
	yeProvidedByID       = "urn:opentelemetry:namespace/ns:service/ye-service-urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service"
	yeFaasName           = "ye-faas"
	tagClusterName       = "cluster-name"
	tagK8sScope          = "k8s-scope"
	yeClusterName        = "ye-cluster"
	yeK8sNamespace       = "ye-k8s-namespace"
	defaultNamespaceName = "default"
)

func TestTopology_addResource(t *testing.T) {
	collection := internal.NewCollection()
	attrs := pcommon.NewMap()
	attrs.PutStr(attrServiceName, demoService)
	attrs.PutStr(attrServiceNamespace, demoNamespace)
	attrs.PutStr(attrTelemetryLanguage, "go")
	attrs.PutStr("Resource Attributes 1", "value1")
	collection.AddResource(&attrs)

	components := collection.GetComponents()
	//nolint:dupl
	require.Equal(t, []*internal.Component{
		{
			ExternalID: "urn:opentelemetry:namespace/" + demoNamespace,
			Type: internal.ComponentType{
				Name: typeNamespace,
			},
			Data: &internal.ComponentData{
				Name:        demoNamespace,
				Version:     "",
				Layer:       layerApplications,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags:        map[string]string{},
			},
		},
		{
			ExternalID: "urn:opentelemetry:namespace/" + demoNamespace + ":service/" + demoService,
			Type: internal.ComponentType{
				Name: typeService,
			},
			Data: &internal.ComponentData{
				Name:        demoService,
				Version:     "",
				Layer:       layerServices,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					attrServiceName:       demoService,
					attrServiceNamespace:  demoNamespace,
					attrTelemetryLanguage: "go",
				},
			},
		},
		{
			ExternalID: "urn:opentelemetry:namespace/" + demoNamespace + ":service/" + demoService + ":serviceInstance/" + demoService,
			Type: internal.ComponentType{
				Name: typeServiceInstance,
			},
			Data: &internal.ComponentData{
				Name:        demoService + " - instance",
				Version:     "",
				Layer:       layerContainers,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"Resource Attributes 1": "value1",
					attrServiceName:         demoService,
					attrServiceNamespace:    demoNamespace,
					attrTelemetryLanguage:   "go",
				},
			},
		},
	}, components)

	relations := collection.GetRelations()
	require.Equal(t, []*internal.Relation{
		{
			ExternalID: "urn:opentelemetry:namespace/" + demoNamespace + ":service/" + demoService + "-urn:opentelemetry:namespace/" + demoNamespace + ":service/" + demoService + ":serviceInstance/" + demoService,
			SourceID:   "urn:opentelemetry:namespace/" + demoNamespace + ":service/" + demoService,
			TargetID:   "urn:opentelemetry:namespace/" + demoNamespace + ":service/" + demoService + ":serviceInstance/" + demoService,
			Type: internal.RelationType{
				Name: relationProvidedBy,
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
			SourceID:   frontendSourceID,
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
	attrs.PutStr(attrServiceName, yeServiceName)
	attrs.PutStr(attrServiceNamespace, "ns")
	attrs.PutStr("host.id", hostName)
	ok := collection.AddResource(&attrs)
	require.True(t, ok)

	components := collection.GetComponents()
	require.Equal(t, []*internal.Component{
		{
			ExternalID: "urn:opentelemetry:host/" + hostName,
			Type: internal.ComponentType{
				Name: "host",
			},
			Data: &internal.ComponentData{
				Name:        hostName,
				Version:     "",
				Layer:       "urn:stackpack:common:layer:machines",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"host.id": hostName,
				},
			},
		},
		{
			ExternalID: nsExternalID,
			Type: internal.ComponentType{
				Name: typeNamespace,
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
			ExternalID: yeServiceExternalID,
			Type: internal.ComponentType{
				Name: typeService,
			},
			Data: &internal.ComponentData{
				Name:        yeServiceName,
				Version:     "",
				Layer:       layerServices,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					attrServiceName:      yeServiceName,
					attrServiceNamespace: "ns",
				},
			},
		},
		{
			ExternalID: yeServiceInstanceID,
			Type: internal.ComponentType{
				Name: typeServiceInstance,
			},
			Data: &internal.ComponentData{
				Name:        yeServiceInstance,
				Version:     "",
				Layer:       layerContainers,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"host.id":            hostName,
					attrServiceName:      yeServiceName,
					attrServiceNamespace: "ns",
				},
			},
		},
	}, components)

	relations := collection.GetRelations()
	require.Equal(t, []*internal.Relation{
		{
			ExternalID: "urn:opentelemetry:host/" + hostName + "-" + yeServiceInstanceID,
			SourceID:   "urn:opentelemetry:host/" + hostName,
			TargetID:   yeServiceInstanceID,
			Type: internal.RelationType{
				Name: "executes",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
		{
			ExternalID: yeProvidedByID,
			SourceID:   yeServiceExternalID,
			TargetID:   yeServiceInstanceID,
			Type: internal.RelationType{
				Name: relationProvidedBy,
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
	attrs.PutStr(attrServiceName, yeServiceName)
	attrs.PutStr(attrServiceNamespace, "ns")
	attrs.PutStr("faas.id", yeFaasName)
	ok := collection.AddResource(&attrs)
	require.True(t, ok)

	components := collection.GetComponents()
	require.Equal(t, []*internal.Component{
		{
			ExternalID: "urn:opentelemetry:function/" + yeFaasName,
			Type: internal.ComponentType{
				Name: "function",
			},
			Data: &internal.ComponentData{
				Name:        yeFaasName,
				Version:     "",
				Layer:       "urn:stackpack:common:layer:serverless",
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"faas.id": yeFaasName,
				},
			},
		},
		{
			ExternalID: nsExternalID,
			Type: internal.ComponentType{
				Name: typeNamespace,
			},
			Data: &internal.ComponentData{
				Name:        "ns",
				Version:     "",
				Layer:       layerApplications,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags:        map[string]string{},
			},
		},
		{
			ExternalID: yeServiceExternalID,
			Type: internal.ComponentType{
				Name: typeService,
			},
			Data: &internal.ComponentData{
				Name:        yeServiceName,
				Version:     "",
				Layer:       layerServices,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					attrServiceName:      yeServiceName,
					attrServiceNamespace: "ns",
				},
			},
		},
		{
			ExternalID: yeServiceInstanceID,
			Type: internal.ComponentType{
				Name: typeServiceInstance,
			},
			Data: &internal.ComponentData{
				Name:        yeServiceInstance,
				Version:     "",
				Layer:       layerContainers,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"faas.id":            yeFaasName,
					attrServiceName:      yeServiceName,
					attrServiceNamespace: "ns",
				},
			},
		},
	}, components)

	relations := collection.GetRelations()
	require.Equal(t, []*internal.Relation{
		{
			ExternalID: "urn:opentelemetry:function/" + yeFaasName + "-" + yeServiceInstanceID,
			SourceID:   "urn:opentelemetry:function/" + yeFaasName,
			TargetID:   yeServiceInstanceID,
			Type: internal.RelationType{
				Name: "executes",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
		{
			ExternalID: yeProvidedByID,
			SourceID:   yeServiceExternalID,
			TargetID:   yeServiceInstanceID,
			Type: internal.RelationType{
				Name: relationProvidedBy,
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
	attrs.PutStr(attrServiceName, yeServiceName)
	attrs.PutStr(attrServiceNamespace, "ns")
	attrs.PutStr("k8s.cluster.name", yeClusterName)
	attrs.PutStr("k8s.namespace.name", yeK8sNamespace)
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
					tagClusterName: yeClusterName,
					tagK8sScope:    yeClusterName + "/" + yeK8sNamespace,
					attrNamespace:  yeK8sNamespace,
				},
			},
		},
		{
			ExternalID: nsExternalID,
			Type: internal.ComponentType{
				Name: typeNamespace,
			},
			Data: &internal.ComponentData{
				Name:        "ns",
				Version:     "",
				Layer:       layerApplications,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					tagClusterName: yeClusterName,
					tagK8sScope:    yeClusterName + "/" + yeK8sNamespace,
					attrNamespace:  yeK8sNamespace,
				},
			},
		},
		{
			ExternalID: yeServiceExternalID,
			Type: internal.ComponentType{
				Name: typeService,
			},
			Data: &internal.ComponentData{
				Name:        yeServiceName,
				Version:     "",
				Layer:       layerServices,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					tagClusterName:       yeClusterName,
					tagK8sScope:          yeClusterName + "/" + yeK8sNamespace,
					attrNamespace:        yeK8sNamespace,
					attrServiceName:      yeServiceName,
					attrServiceNamespace: "ns",
				},
			},
		},
		{
			ExternalID: yeServiceInstanceID,
			Type: internal.ComponentType{
				Name: typeServiceInstance,
			},
			Data: &internal.ComponentData{
				Name:        yeServiceInstance,
				Version:     "",
				Layer:       layerContainers,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"k8s.cluster.name":   yeClusterName,
					"k8s.namespace.name": yeK8sNamespace,
					"k8s.pod.name":       "ye-pod-name",
					tagClusterName:       yeClusterName,
					tagK8sScope:          yeClusterName + "/" + yeK8sNamespace,
					attrNamespace:        yeK8sNamespace,
					attrServiceName:      yeServiceName,
					attrServiceNamespace: "ns",
				},
			},
		},
	}, components)

	relations := collection.GetRelations()
	require.Equal(t, []*internal.Relation{
		{
			ExternalID: "urn:opentelemetry:kubernetes:/" + yeClusterName + ":" + yeK8sNamespace + ":pod/ye-pod-name-" + yeServiceInstanceID,
			SourceID:   "urn:opentelemetry:kubernetes:/" + yeClusterName + ":" + yeK8sNamespace + ":pod/ye-pod-name",
			TargetID:   "urn:opentelemetry:namespace/ns:service/ye-service:serviceInstance/ye-service",
			Type: internal.RelationType{
				Name: "kubernetes-to-otel",
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
		{
			ExternalID: yeProvidedByID,
			SourceID:   yeServiceExternalID,
			TargetID:   yeServiceInstanceID,
			Type: internal.RelationType{
				Name: relationProvidedBy,
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
	attrs.PutStr(attrServiceName, demoService)
	attrs.PutStr(attrTelemetryLanguage, "go")
	attrs.PutStr("Resource Attributes 1", "value1")
	collection.AddResource(&attrs)

	components := collection.GetComponents()
	//nolint:dupl
	require.Equal(t, []*internal.Component{
		{
			ExternalID: "urn:opentelemetry:namespace/" + defaultNamespaceName,
			Type: internal.ComponentType{
				Name: typeNamespace,
			},
			Data: &internal.ComponentData{
				Name:        defaultNamespaceName,
				Version:     "",
				Layer:       layerApplications,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags:        map[string]string{},
			},
		},
		{
			ExternalID: "urn:opentelemetry:namespace/" + defaultNamespaceName + ":service/" + demoService,
			Type: internal.ComponentType{
				Name: typeService,
			},
			Data: &internal.ComponentData{
				Name:        demoService,
				Version:     "",
				Layer:       layerServices,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					attrServiceName:       demoService,
					attrServiceNamespace:  defaultNamespaceName,
					attrTelemetryLanguage: "go",
				},
			},
		},
		{
			ExternalID: "urn:opentelemetry:namespace/" + defaultNamespaceName + ":service/" + demoService + ":serviceInstance/" + demoService,
			Type: internal.ComponentType{
				Name: typeServiceInstance,
			},
			Data: &internal.ComponentData{
				Name:        demoService + " - instance",
				Version:     "",
				Layer:       layerContainers,
				Domain:      "",
				Environment: "",
				Identifiers: []string{},
				Tags: map[string]string{
					"Resource Attributes 1": "value1",
					attrServiceName:         demoService,
					attrServiceNamespace:    defaultNamespaceName,
					attrTelemetryLanguage:   "go",
				},
			},
		},
	}, components)

	relations := collection.GetRelations()
	require.Equal(t, []*internal.Relation{
		{
			ExternalID: "urn:opentelemetry:namespace/" + defaultNamespaceName + ":service/" + demoService + "-urn:opentelemetry:namespace/" + defaultNamespaceName + ":service/" + demoService + ":serviceInstance/" + demoService,
			SourceID:   "urn:opentelemetry:namespace/" + defaultNamespaceName + ":service/" + demoService,
			TargetID:   "urn:opentelemetry:namespace/" + defaultNamespaceName + ":service/" + demoService + ":serviceInstance/" + demoService,
			Type: internal.RelationType{
				Name: relationProvidedBy,
			},
			Data: &internal.RelationData{
				Tags: map[string]string{},
			},
		},
	}, relations)
}
