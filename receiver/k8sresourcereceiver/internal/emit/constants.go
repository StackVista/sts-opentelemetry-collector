package emit

const (
	// ScopeName is the instrumentation scope name for logs emitted by this receiver.
	ScopeName = "github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver"

	// Attribute keys for log records
	AttrK8sResourceKind    = "k8s.resource.kind"
	AttrK8sResourceGroup   = "k8s.resource.group"
	AttrK8sResourceVersion = "k8s.resource.version"
	AttrK8sObjectName      = "k8s.object.name"
	AttrEventDomain        = "event.domain"
	AttrK8sNamespaceName   = "k8s.namespace.name"
	AttrK8sClusterName     = "k8s.cluster.name"

	// EventDomainK8s is the value for the event.domain attribute.
	EventDomainK8s = "k8s"

	// EventNameCR EventNameObject and EventNameCRD are Static event names for log-based OTel mappings.
	// Object emission picks between CR and Object based on the watch's source: CRD-discovered (or
	// CRD-backed static) watches use EventNameCR so downstream keeps the CR
	// log shape; plain static watches use EventNameObject.
	EventNameCR     = "KubernetesCustomResourceEvent"
	EventNameObject = "KubernetesObjectEvent"
	EventNameCRD    = "KubernetesCustomResourceDefinitionEvent"
)
