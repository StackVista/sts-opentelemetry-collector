package emit

const (
	// ScopeName is the instrumentation scope name for logs emitted by this receiver.
	ScopeName = "github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver"

	// Attribute keys for log records
	AttrK8sResourceName    = "k8s.resource.name"
	AttrK8sResourceGroup   = "k8s.resource.group"
	AttrK8sResourceVersion = "k8s.resource.version"
	AttrK8sObjectName      = "k8s.object.name"
	AttrEventDomain        = "event.domain"
	AttrK8sNamespaceName   = "k8s.namespace.name"
	AttrK8sClusterName     = "k8s.cluster.name"

	// EventDomainK8s is the value for the event.domain attribute.
	EventDomainK8s = "k8s"

	// Static event names for log-based OTel mappings
	EventNameCR  = "KubernetesCustomResourceEvent"
	EventNameCRD = "KubernetesCustomResourceDefinitionEvent"
)
