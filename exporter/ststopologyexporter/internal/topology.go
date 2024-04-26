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
			withTags(attrs, "telemetry.sdk"),
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
			withTags(attrs, "telemetry.sdk").
			withTags(attrs, "telemetry.distro").
			withProperties(attrs),
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
		Labels:      []string{},
		Tags:        map[string]string{},
		Properties:  map[string]string{},
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
	}
	return c
}
func (c *ComponentData) withTags(attrs *pcommon.Map, prefix string) *ComponentData {
	attrs.Range(func(k string, v pcommon.Value) bool {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			c.Tags[k] = v.AsString()
		}
		return true
	})
	return c
}

func (c *ComponentData) withProperties(attrs *pcommon.Map) *ComponentData {
	m := make(map[string]string, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})
	c.Properties = m
	return c
}
