package internal

import (
	"cmp"
	"fmt"
	"slices"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

const (
	ConnectionTypeDatabase     = "database"
	ConnectionTypeSynchronous  = "synchronous"
	ConnectionTypeASynchronous = "asynchronous"
)

type ComponentsCollection struct {
	components map[string]*Component
	relations  map[string]*Relation
}

func NewCollection() *ComponentsCollection {
	return &ComponentsCollection{
		make(map[string]*Component, 0),
		make(map[string]*Relation, 0),
	}
}

func (c *ComponentsCollection) AddResource(attrs *pcommon.Map) bool {
	serviceName, ok := attrs.Get("service.name")
	if !ok {
		return false
	}
	serviceNamespaceValue, ok := attrs.Get("service.namespace")
	var serviceNamespace string
	if !ok {
		serviceNamespace = "default"
	} else {
		serviceNamespace = serviceNamespaceValue.AsString()
	}
	instanceID, ok := attrs.Get("service.instance.id")
	var serviceInstanceID pcommon.Value
	var serviceInstanceName string
	if !ok {
		serviceInstanceID = serviceName
		serviceInstanceName = fmt.Sprintf("%s - instance", serviceName.AsString())
	} else {
		serviceInstanceID = instanceID
		serviceInstanceName = fmt.Sprintf("%s - %s", serviceName.AsString(), instanceID.AsString())
	}

	namespaceIdentifier := fmt.Sprintf("urn:opentelemetry:namespace/%s", serviceNamespace)
	if _, ok := c.components[namespaceIdentifier]; !ok {
		c.components[namespaceIdentifier] = &Component{
			namespaceIdentifier,
			ComponentType{
				"namespace",
			},
			newComponentData().
				withLayer("urn:stackpack:common:layer:applications").
				withScope(attrs).
				withEnvironment(attrs).
				withName(serviceNamespace),
		}
	}

	serviceIdentifier := fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s", serviceNamespace, serviceName.AsString())
	c.components[serviceIdentifier] = &Component{
		serviceIdentifier,
		ComponentType{
			"service",
		},
		newComponentData().
			withLayer("urn:stackpack:common:layer:services").
			withScope(attrs).
			withEnvironment(attrs).
			withNameFromAttr(attrs, "service.name").
			withVersion(attrs, "service.version").
			withTag(attrs, "service.name").
			withTagValue("service.namespace", serviceNamespace).
			withTag(attrs, "service.version").
			withTagPrefix(attrs, "telemetry.sdk"),
	}
	serviceInstanceIdentifier := fmt.Sprintf(
		"urn:opentelemetry:namespace/%s:service/%s:serviceInstance/%s",
		serviceNamespace,
		serviceName.AsString(),
		serviceInstanceID.AsString(),
	)
	c.components[serviceInstanceIdentifier] = &Component{
		serviceInstanceIdentifier,
		ComponentType{
			"service-instance",
		},
		newComponentData().
			withLayer("urn:stackpack:common:layer:containers").
			withScope(attrs).
			withEnvironment(attrs).
			withName(serviceInstanceName).
			withVersion(attrs, "service.version").
			withTagValue("service.namespace", serviceNamespace).
			withTags(attrs),
	}
	c.addRelation(serviceIdentifier, serviceInstanceIdentifier, "provided-by")
	c.addHostResource(attrs, serviceInstanceIdentifier)
	c.addKubernetesRelation(attrs, serviceInstanceIdentifier)
	return true
}

func (c *ComponentsCollection) addHostResource(attrs *pcommon.Map, instance string) {
	if hostID, ok := attrs.Get("host.id"); ok {
		hostIdentifier := fmt.Sprintf("urn:opentelemetry:host/%s", hostID.AsString())
		c.components[hostID.AsString()] = &Component{
			hostIdentifier,
			ComponentType{
				"host",
			},
			newComponentData().
				withName(hostID.AsString()).
				withEnvironment(attrs).
				withLayer("urn:stackpack:common:layer:machines").
				withTagPrefix(attrs, "os").
				withTagPrefix(attrs, "host").
				withTagPrefix(attrs, "cloud").
				withTagPrefix(attrs, "azure").
				withTagPrefix(attrs, "gcp"),
		}
		c.addRelation(hostIdentifier, instance, "executes")
	} else if faasID, ok := attrs.Get("faas.id"); ok {
		faasIdentifier := fmt.Sprintf("urn:opentelemetry:function/%s", faasID.AsString())
		c.components[faasIdentifier] = &Component{
			faasIdentifier,
			ComponentType{
				"function",
			},
			newComponentData().
				withName(faasID.AsString()).
				withEnvironment(attrs).
				withLayer("urn:stackpack:common:layer:serverless").
				withVersion(attrs, "faas.version").
				withTagPrefix(attrs, "faas").
				withTagPrefix(attrs, "cloud"),
		}
		c.addRelation(faasIdentifier, instance, "executes")
	} else if taskID, ok := attrs.Get("aws.ecs.task.id"); ok {
		taskIdentifier := fmt.Sprintf("urn:opentelemetry:task/%s", taskID.AsString())
		c.components[taskIdentifier] = &Component{
			taskIdentifier,
			ComponentType{
				"task",
			},
			newComponentData().
				withName(taskID.AsString()).
				withEnvironment(attrs).
				withLayer("urn:stackpack:common:layer:serverless").
				withTagPrefix(attrs, "aws.ecs").
				withTagPrefix(attrs, "cloud"),
		}
		c.addRelation(taskIdentifier, instance, "executes")
	}
}

func (c *ComponentsCollection) addKubernetesRelation(attrs *pcommon.Map, instance string) {
	reqAttrs := make(map[string]string, 3)
	for _, key := range []string{
		"k8s.cluster.name",
		"k8s.namespace.name",
		"k8s.pod.name",
	} {
		value, ok := attrs.Get(key)
		if !ok {
			return
		}
		reqAttrs[key] = value.AsString()
	}
	podIdentifier := fmt.Sprintf(
		"urn:opentelemetry:kubernetes:/%s:%s:pod/%s",
		reqAttrs["k8s.cluster.name"],
		reqAttrs["k8s.namespace.name"],
		reqAttrs["k8s.pod.name"],
	)
	c.components[podIdentifier] = &Component{
		podIdentifier,
		ComponentType{
			"pod",
		},
		newComponentData().
			withNameFromAttr(attrs, "k8s.pod.name").
			withScope(attrs).
			withIdentifier(fmt.Sprintf(
				"urn:kubernetes:/%s:%s:pod/%s",
				reqAttrs["k8s.cluster.name"],
				reqAttrs["k8s.namespace.name"],
				reqAttrs["k8s.pod.name"]),
			),
	}
	c.addRelation(podIdentifier, instance, "kubernetes-to-otel")
}

func (c *ComponentsCollection) AddConnection(attrs *pcommon.Map) bool {
	reqAttrs := make(map[string]string, 4)
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
	switch reqAttrs["connection_type"] {
	case "":
		connectionType = ConnectionTypeSynchronous
	case "messaging_system":
		connectionType = ConnectionTypeASynchronous
	case "database":
		connectionType = ConnectionTypeDatabase
	default:
		return false
	}

	instanceID, ok := attrs.Get("client_service.instance.id")
	var clientInstanceID string
	if !ok {
		clientInstanceID = reqAttrs["client"]
	} else {
		clientInstanceID = instanceID.AsString()
	}
	sourceID := fmt.Sprintf(
		"urn:opentelemetry:namespace/%s:service/%s:serviceInstance/%s",
		reqAttrs["client_service.namespace"],
		reqAttrs["client"],
		clientInstanceID,
	)

	peerService, hasPeer := attrs.Get("client_peer.service")
	var targetID string

	switch connectionType {
	case ConnectionTypeDatabase:
		if hasPeer {
			// create separate relations producer -> peer and consumer -> peer
			namespace := reqAttrs["client_service.namespace"]
			targetID = fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s", namespace, peerService.AsString())
		} else {
			targetID = fmt.Sprintf(
				"urn:opentelemetry:namespace/%s:service/%s:database/%s",
				reqAttrs["client_service.namespace"],
				reqAttrs["client"],
				reqAttrs["server"],
			)
			c.components[targetID] = &Component{
				targetID,
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

	case ConnectionTypeASynchronous:
		consumerNamespace, ok := attrs.Get("server_service.namespace")
		if !ok {
			return false
		}
		instanceID, ok := attrs.Get("server_service.instance.id")
		var consumerInstanceID string
		if !ok {
			consumerInstanceID = reqAttrs["server"]
		} else {
			consumerInstanceID = instanceID.AsString()
		}
		consumerID := fmt.Sprintf(
			"urn:opentelemetry:namespace/%s:service/%s:serviceInstance/%s",
			consumerNamespace.AsString(),
			reqAttrs["server"],
			consumerInstanceID,
		)
		if hasPeer {
			// create separate relations producer -> peer and consumer -> peer
			namespace := reqAttrs["client_service.namespace"]
			targetID = fmt.Sprintf("urn:opentelemetry:namespace/%s:service/%s", namespace, peerService.AsString())
			c.addRelation(consumerID, targetID, connectionType)
		} else {
			targetID = consumerID
		}
	default:

		serverNamespace, ok := attrs.Get("server_service.namespace")
		if !ok {
			return false
		}
		instanceID, ok := attrs.Get("server_service.instance.id")
		var serverInstanceID string
		if !ok {
			serverInstanceID = reqAttrs["server"]
		} else {
			serverInstanceID = instanceID.AsString()
		}
		targetID = fmt.Sprintf(
			"urn:opentelemetry:namespace/%s:service/%s:serviceInstance/%s",
			serverNamespace.AsString(),
			reqAttrs["server"],
			serverInstanceID,
		)
	}

	c.addRelation(sourceID, targetID, connectionType)

	return true
}

func (c *ComponentsCollection) addRelation(sourceID string, targetID string, typeName string) {
	relationID := fmt.Sprintf("%s-%s", sourceID, targetID)
	c.relations[relationID] = &Relation{
		ExternalID: fmt.Sprintf("%s-%s", sourceID, targetID),
		SourceID:   sourceID,
		TargetID:   targetID,
		Type: RelationType{
			Name: typeName,
		},
		Data: newRelationData(),
	}
}

func (c *ComponentsCollection) GetComponents() []*Component {
	components := make([]*Component, 0, len(c.components))
	for _, component := range c.components {
		components = append(components, component)
	}
	slices.SortFunc(components, func(a, b *Component) int {
		return cmp.Compare(a.ExternalID, b.ExternalID)
	})
	return components
}

func (c *ComponentsCollection) GetRelations() []*Relation {
	relations := make([]*Relation, 0, len(c.relations))
	for _, relation := range c.relations {
		relations = append(relations, relation)
	}
	slices.SortFunc(relations, func(a, b *Relation) int {
		return cmp.Compare(a.ExternalID, b.ExternalID)
	})
	return relations
}

func newComponentData() *ComponentData {
	return &ComponentData{
		Name:        "",
		Version:     "",
		Layer:       "",
		Domain:      "",
		Environment: "",
		Identifiers: []string{},
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

func (c *ComponentData) withScope(attrs *pcommon.Map) *ComponentData {
	clusterName, ok := attrs.Get("k8s.cluster.name")

	if ok {
		c.Tags["cluster-name"] = clusterName.AsString()
		namespaceName, nok := attrs.Get("k8s.namespace.name")
		if nok {
			c.Tags["namespace"] = namespaceName.AsString()
			c.Tags["k8s-scope"] = fmt.Sprintf("%s/%s", clusterName.AsString(), namespaceName.AsString())
		}
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

func (c *ComponentData) withIdentifier(identifier string) *ComponentData {
	c.Identifiers = append(c.Identifiers, identifier)
	return c
}

func newRelationData() *RelationData {
	return &RelationData{
		Tags: map[string]string{},
	}
}
