package internal

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

type ComponentsCollection struct {
	namespaces       map[string]*Component
	services         map[string]*Component
	serviceInstances map[string]*Component
	relations        map[string]*Relation
}

func NewCollection() *ComponentsCollection {
	return &ComponentsCollection{
		make(map[string]*Component, 0),
		make(map[string]*Component, 0),
		make(map[string]*Component, 0),
		make(map[string]*Relation, 0),
	}
}

func (c *ComponentsCollection) AddResource(attrs *pcommon.Map) bool {
	serviceName, ok := attrs.Get("service.name")
	if !ok {
		return false
	}
	serviceNamespace, ok := attrs.Get("service.namespace")
	if !ok {
		return false
	}
	instanceId, ok := attrs.Get("service.instance.id")
	var serviceInstanceId pcommon.Value
	if !ok {
		serviceInstanceId = serviceName
	} else {
		serviceInstanceId = instanceId
	}

	if _, ok := c.namespaces[serviceNamespace.AsString()]; !ok {
		c.namespaces[serviceNamespace.AsString()] = &Component{
			fmt.Sprintf("urn:opentelemetry:namespace/%s", serviceNamespace.AsString()),
			ComponentType{
				"namespace",
			},
			newComponentData().
				withLayer("urn:stackpack:common:layer:applications").
				withEnvironment(attrs).
				withName(attrs, "service.namespace"),
		}
	}

	serviceIdentifier := fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s", serviceNamespace.AsString(), serviceName.AsString())
	c.services[serviceIdentifier] = &Component{
		serviceIdentifier,
		ComponentType{
			"service",
		},
		newComponentData().
			withLayer("urn:stackpack:common:layer:services").
			withEnvironment(attrs).
			withName(attrs, "service.name").
			withVersion(attrs, "service.version").
			withTag(attrs, "service.namespace").
			withTagPrefix(attrs, "telemetry.sdk"),
	}
	serviceInstanceIdentifier := fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s:serviceInstance/%s", serviceNamespace.AsString(), serviceName.AsString(), serviceInstanceId.AsString())
	c.serviceInstances[serviceInstanceIdentifier] = &Component{
		serviceInstanceIdentifier,
		ComponentType{
			"service-instance",
		},
		newComponentData().
			withLayer("urn:stackpack:common:layer:containers").
			withEnvironment(attrs).
			withName(attrs, "service.instance.id").
			withVersion(attrs, "service.version").
			withTag(attrs, "service.namespace").
			withTags(attrs),
	}
	c.addRelation(serviceIdentifier, serviceInstanceIdentifier, "provided by")
	return true
}

func (c *ComponentsCollection) AddConnection(attrs *pcommon.Map) bool {
	reqAttrs := make(map[string]string, 3)
	for _, key := range []string{
		"client",
		"client_service.namespace",
		"server",
		"connection_type",
	} {
		value, ok := attrs.Get(key)
		if !ok {
			return false
		}
		reqAttrs[key] = value.AsString()
	}

	var connectionType string
	if reqAttrs["connection_type"] == "" {
		connectionType = "synchronous"
	} else if reqAttrs["connection_type"] == "messaging_system" {
		connectionType = "asynchronous"
	} else if reqAttrs["connection_type"] == "database" {
		connectionType = "database"
	} else {
		return false
	}

	instanceId, ok := attrs.Get("client_service.instance.id")
	var clientInstanceId string
	if !ok {
		clientInstanceId = reqAttrs["client"]
	} else {
		clientInstanceId = instanceId.AsString()
	}
	sourceId := fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s:serviceInstance/%s", reqAttrs["client_service.namespace"], reqAttrs["client"], clientInstanceId)

	var targetId string
	if connectionType == "database" {
		targetId = fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s:database/%s", reqAttrs["client_service.namespace"], reqAttrs["client"], reqAttrs["server"])
		c.serviceInstances[targetId] = &Component{
			targetId,
			ComponentType{
				"database",
			},
			newComponentData().
				withLayer("urn:stackpack:common:layer:databases").
				withName(attrs, "server"),
		}
	} else {
		serverNamespace, ok := attrs.Get("server_service.namespace")
		if !ok {
			return false
		}
		instanceId, ok := attrs.Get("server_service.instance.id")
		var serverInstanceId string
		if !ok {
			serverInstanceId = reqAttrs["server"]
		} else {
			serverInstanceId = instanceId.AsString()
		}
		targetId = fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s:serviceInstance/%s", serverNamespace.AsString(), reqAttrs["server"], serverInstanceId)
	}

	c.addRelation(sourceId, targetId, connectionType)

	return true
}

func (c *ComponentsCollection) addRelation(sourceId string, targetId string, typeName string) {
	relationId := fmt.Sprintf("%s-%s", sourceId, targetId)
	c.relations[relationId] = &Relation{
		ExternalId: fmt.Sprintf("%s-%s", sourceId, targetId),
		SourceId:   sourceId,
		TargetId:   targetId,
		Type: RelationType{
			Name: typeName,
		},
		Data: newRelationData(),
	}
}

func (c *ComponentsCollection) GetComponents() []*Component {
	namespaces := make([]*Component, 0, len(c.namespaces))
	for _, namespace := range c.namespaces {
		namespaces = append(namespaces, namespace)
	}
	services := make([]*Component, 0, len(c.services))
	for _, service := range c.services {
		services = append(services, service)
	}
	instances := make([]*Component, 0, len(c.serviceInstances))
	for _, instance := range c.serviceInstances {
		instances = append(instances, instance)
	}
	return append(
		append(
			services,
			instances...,
		),
		namespaces...,
	)
}

func (c *ComponentsCollection) GetRelations() []*Relation {
	relations := make([]*Relation, 0, len(c.relations))
	for _, relation := range c.relations {
		relations = append(relations, relation)
	}
	return relations
}

func newComponentData() *ComponentData {
	return &ComponentData{
		Name:        "",
		Version:     "",
		Layer:       "",
		Domain:      "",
		Environment: "",
		Tags:        map[string]string{},
	}
}

func (c *ComponentData) withLayer(layer string) *ComponentData {
	c.Layer = layer
	return c
}

func (c *ComponentData) withName(attrs *pcommon.Map, key string) *ComponentData {
	value, ok := attrs.Get(key)
	if ok {
		c.Name = value.AsString()
	}
	return c
}

func (c *ComponentData) withTag(attrs *pcommon.Map, key string) *ComponentData {
	value, ok := attrs.Get(key)
	if ok {
		c.Tags[key] = value.AsString()
	}
	return c
}

func (c *ComponentData) withTagPrefix(attrs *pcommon.Map, prefix string) *ComponentData {
	attrs.Range(func(k string, v pcommon.Value) bool {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			c.Tags[k] = v.AsString()
		}
		return true
	})
	return c
}

func (c *ComponentData) withVersion(attrs *pcommon.Map, key string) *ComponentData {
	value, ok := attrs.Get(key)
	if ok {
		c.Version = value.AsString()
	}
	return c
}

func (c *ComponentData) withEnvironment(attrs *pcommon.Map) *ComponentData {
	value, ok := attrs.Get("deployment.environment")
	if ok {
		c.Environment = value.AsString()
		c.Tags["deployment.environment"] = value.AsString()
	}
	return c
}

func (c *ComponentData) withTags(attrs *pcommon.Map) *ComponentData {
	attrs.Range(func(k string, v pcommon.Value) bool {
		if _, ok := c.Tags[k]; !ok {
			c.Tags[k] = v.AsString()
		}
		return true
	})
	return c
}

func newRelationData() *RelationData {
	return &RelationData{
		Tags: map[string]string{},
	}
}
