package emit

const (
	// ScopeName is the instrumentation scope name for logs emitted by this receiver.
	ScopeName = "github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver"

	// Attribute keys for log records
	AttrK8sResourceKind           = "k8s.resource.kind"
	AttrK8sResourceGroup          = "k8s.resource.group"
	AttrK8sResourceVersion        = "k8s.resource.version"
	AttrK8sCRDCRsWatched          = "k8s.crd.custom_resources_watched"
	AttrK8sObjectName             = "k8s.object.name"
	AttrEventDomain               = "event.domain"
	AttrK8sNamespaceName          = "k8s.namespace.name"
	AttrK8sClusterName            = "k8s.cluster.name"
	AttrK8sPodRestartCount        = "k8s.pod.restart_count"
	AttrK8sPodContainerCount      = "k8s.pod.container_count"
	AttrK8sPodReadyContainerCount = "k8s.pod.ready_container_count"

	// EventDomainK8s is the value for the event.domain attribute.
	EventDomainK8s = "k8s"

	// EventNameCR EventNameObject and EventNameCRD are Static event names for log-based OTel mappings.
	// Object emission picks between CR and Object based on the watch's source: CRD-discovered (or
	// CRD-backed static) watches use EventNameCR so downstream keeps the CR
	// log shape; plain static watches use EventNameObject.
	EventNameCR     = "KubernetesCustomResourceEvent"
	EventNameObject = "KubernetesObjectEvent"
	EventNameCRD    = "KubernetesCustomResourceDefinitionEvent"

	// EventNameSnapshotBoundary marks snapshot start/end and collection reset for
	// consumers that must distinguish a complete snapshot from partial state.
	EventNameSnapshotBoundary = "KubernetesSnapshotBoundary"

	AttrK8sSnapshotBoundary = "k8s.snapshot.boundary"
	AttrK8sSnapshotID       = "k8s.snapshot.id"
	// AttrK8sSnapshotComplete is set on end boundaries: true only when every
	// configured static watch had a synced informer when the snapshot was read.
	AttrK8sSnapshotComplete = "k8s.snapshot.complete"

	SnapshotBoundaryStart = "start"
	SnapshotBoundaryEnd   = "end"
	// SnapshotBoundaryReset is emitted when this replica stops collecting, e.g.
	// on leadership loss, so consumers discard state they can no longer refresh.
	SnapshotBoundaryReset = "reset"
)
