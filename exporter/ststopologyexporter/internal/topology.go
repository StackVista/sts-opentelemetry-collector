package internal

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

type ComponentsCollection struct {
	namespaces       map[string]*Component
	services         []*Component
	serviceInstances []*Component
}

func NewCollection() *ComponentsCollection {
	return &ComponentsCollection{
		make(map[string]*Component, 0),
		make([]*Component, 0),
		make([]*Component, 0),
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
			newData().
				withLayer("urn:stackpack:common:layer:applications").
				withEnvironment(attrs).
				withName(attrs, "service.namespace"),
		}
	}
	c.services = append(c.services, &Component{
		fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s", serviceNamespace.AsString(), serviceName.AsString()),
		ComponentType{
			"service",
		},
		newData().
			withLayer("urn:stackpack:common:layer:services").
			withEnvironment(attrs).
			withName(attrs, "service.name").
			withVersion(attrs, "service.version").
			withTag(attrs, "service.namespace"),
	})
	c.serviceInstances = append(c.serviceInstances, &Component{
		fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s:serviceInstance/%s", serviceNamespace.AsString(), serviceName.AsString(), serviceInstanceId.AsString()),
		ComponentType{
			"service_instance",
		},
		newData().
			withLayer("urn:stackpack:common:layer:containers").
			withEnvironment(attrs).
			withName(attrs, "service.instance.id").
			withVersion(attrs, "service.version").
			withTag(attrs, "service.namespace").
			withTags(attrs),
	})
	return true
}

func (c *ComponentsCollection) GetComponents() []*Component {
	namespaces := make([]*Component, 0, len(c.namespaces))
	for _, namespace := range c.namespaces {
		namespaces = append(namespaces, namespace)
	}
	return append(
		append(
			c.services,
			c.serviceInstances...,
		),
		namespaces...,
	)
}

func (c *ComponentsCollection) GetRelations() []*Relation {
	return make([]*Relation, 0)
}

func newData() *ComponentData {
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
