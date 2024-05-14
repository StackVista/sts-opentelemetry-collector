package internal

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

const (
	CONNECTION_TYPE_DATABASE     = "database"
	CONNECTION_TYPE_SYNCHRONOUS  = "synchronous"
	CONNECTION_TYPE_ASYNCHRONOUS = "asynchronous"
)

type ComponentsCollection struct {
	hosts            map[string]*Component
	functions        map[string]*Component
	tasks            map[string]*Component
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
	var serviceInstanceName string
	if !ok {
		serviceInstanceId = serviceName
		serviceInstanceName = fmt.Sprintf("%s - instance", serviceName.AsString())
	} else {
		serviceInstanceId = instanceId
		serviceInstanceName = fmt.Sprintf("%s - %s", serviceName.AsString(), instanceId.AsString())
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
				withNameFromAttr(attrs, "service.namespace"),
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
			withNameFromAttr(attrs, "service.name").
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
			withName(serviceInstanceName).
			withVersion(attrs, "service.version").
			withTag(attrs, "service.namespace").
			withTags(attrs),
	}
	c.addRelation(serviceIdentifier, serviceInstanceIdentifier, "provided by")
	c.addHostResource(attrs, serviceInstanceIdentifier)
	return true
}

func (c *ComponentsCollection) addHostResource(attrs *pcommon.Map, instance string) {
	if hostId, ok := attrs.Get("host.id"); ok {
		hostIdentifier := fmt.Sprintf("urn:opentelemetry:host/%s", hostId.AsString())
		c.hosts[hostId.AsString()] = &Component{
			hostIdentifier,
			ComponentType{
				"host",
			},
			newComponentData().
				withEnvironment(attrs).
				withLayer("urn:stackpack:common:layer:machines").
				withTagPrefix(attrs, "os").
				withTagPrefix(attrs, "host").
				withTagPrefix(attrs, "cloud").
				withTagPrefix(attrs, "azure").
				withTagPrefix(attrs, "gcp"),
		}
		c.addRelation(hostIdentifier, instance, "executes")
	} else if faasId, ok := attrs.Get("faas.id"); ok {
		c.functions[faasId.AsString()] = &Component{
			fmt.Sprintf("urn:opentelemetry:function/%s", faasId.AsString()),
			ComponentType{
				"function",
			},
			newComponentData().
				withEnvironment(attrs).
				withLayer("urn:stackpack:common:layer:serverless").
				withVersion(attrs, "faas.version").
				withTagPrefix(attrs, "faas").
				withTagPrefix(attrs, "cloud"),
		}
	} else if taskId, ok := attrs.Get("aws.ecs.task.id"); ok {
		c.tasks[taskId.AsString()] = &Component{
			fmt.Sprintf("urn:opentelemetry:task/%s", taskId.AsString()),
			ComponentType{
				"task",
			},
			newComponentData().
				withEnvironment(attrs).
				withLayer("urn:stackpack:common:layer:serverless").
				withTagPrefix(attrs, "aws.ecs").
				withTagPrefix(attrs, "cloud"),
		}
	}
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
		connectionType = CONNECTION_TYPE_SYNCHRONOUS
	} else if reqAttrs["connection_type"] == "messaging_system" {
		connectionType = CONNECTION_TYPE_ASYNCHRONOUS
	} else if reqAttrs["connection_type"] == "database" {
		connectionType = CONNECTION_TYPE_DATABASE
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

	peerService, hasPeer := attrs.Get("client_peer.service")
	var targetId string
	if connectionType == CONNECTION_TYPE_DATABASE {
		if hasPeer {
			// create separate relations producer -> peer and consumer -> peer
			namespace := reqAttrs["client_service.namespace"]
			targetId = fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s", namespace, peerService.AsString())
		} else {
			targetId = fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s:database/%s", reqAttrs["client_service.namespace"], reqAttrs["client"], reqAttrs["server"])
			c.serviceInstances[targetId] = &Component{
				targetId,
				ComponentType{
					"database",
				},
				newComponentData().
					withLayer("urn:stackpack:common:layer:databases").
					withTagValue("service.namespace", reqAttrs["client_service.namespace"]).
					withTagValue("service.name", reqAttrs["client"]).
					withNameFromAttr(attrs, "server"),
			}
		}
	} else if connectionType == CONNECTION_TYPE_ASYNCHRONOUS {
		consumerNamespace, ok := attrs.Get("server_service.namespace")
		if !ok {
			return false
		}
		instanceId, ok := attrs.Get("server_service.instance.id")
		var consumerInstanceId string
		if !ok {
			consumerInstanceId = reqAttrs["server"]
		} else {
			consumerInstanceId = instanceId.AsString()
		}
		consumerId := fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s:serviceInstance/%s", consumerNamespace.AsString(), reqAttrs["server"], consumerInstanceId)
		if hasPeer {
			// create separate relations producer -> peer and consumer -> peer
			namespace := reqAttrs["client_service.namespace"]
			targetId = fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s", namespace, peerService.AsString())
			c.addRelation(consumerId, targetId, connectionType)
		} else {
			targetId = consumerId
		}
	} else { // connectionType == CONNECTION_TYPE_SYNCHRONOUS
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

func (c *ComponentData) withNameFromAttr(attrs *pcommon.Map, key string) *ComponentData {
	value, ok := attrs.Get(key)
	if ok {
		c.Name = value.AsString()
	}
	return c
}

func (c *ComponentData) withName(name string) *ComponentData {
	c.Name = name
	return c
}

func (c *ComponentData) withTag(attrs *pcommon.Map, key string) *ComponentData {
	value, ok := attrs.Get(key)
	if ok {
		c.Tags[key] = value.AsString()
	}
	return c
}

func (c *ComponentData) withTagValue(key string, value string) *ComponentData {
	c.Tags[key] = value
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
