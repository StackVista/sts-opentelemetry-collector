package k8scrdreceiver

const (
	// scopeName is the instrumentation scope name for logs emitted by this receiver
	scopeName = "github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver"

	// Attribute keys for log records
	attrK8sResourceName    = "k8s.resource.name"
	attrK8sResourceGroup   = "k8s.resource.group"
	attrK8sResourceVersion = "k8s.resource.version"
	attrK8sObjectName      = "k8s.object.name"
	attrEventDomain        = "event.domain"
	attrK8sNamespaceName   = "k8s.namespace.name"
	attrK8sClusterName     = "k8s.cluster.name"

	// eventDomainK8s is the value for the event.domain attribute
	eventDomainK8s = "k8s"

	// Static event names for log-based OTel mappings
	eventNameCR  = "KubernetesCustomResourceEvent"
	eventNameCRD = "KubernetesCustomResourceDefinitionEvent"
)
