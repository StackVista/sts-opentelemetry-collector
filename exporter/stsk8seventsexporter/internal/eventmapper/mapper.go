// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2017 Datadog, Inc.

// Package eventmapper is vendored from stackstate-agent
// pkg/collector/corechecks/cluster/kubeapi/kubernetes_event_mapper.go. The
// output must stay identical to the cluster agent's kubernetes_api_events check.
// Changes: telemetry types from the receiver client replace metrics.Event, the
// configured cluster type replaces OpenShift API detection, and zap replaces the
// agent logger.
package eventmapper

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/model/telemetry"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8seventsexporter/internal/urn"
)

type EventCategory string

const (
	Alerts     EventCategory = "Alerts"
	Changes                  = "Changes"
	Activities               = "Activities"
	Others                   = "Others"
)

// ValidCategories contains categories that are only expected in StackState
var ValidCategories = []EventCategory{Alerts, Changes, Activities, Others}

// DefaultEventCategoriesMap contains a mapping of Kubernetes EventReason strings to specific Event Categories.
// If an event is 'event.Type = warning' then we map it automatically to an 'Alert'
var DefaultEventCategoriesMap = map[string]EventCategory{
	// Container events
	"BackOff":             Alerts,
	"Created":             Activities,
	"ExceededGracePeriod": Activities,
	"Killing":             Activities,
	"Preempting":          Activities,
	"Started":             Activities,

	// Image events
	"Pulling": Activities,
	"Pulled":  Activities,

	// Kubelet events
	"NodeReady":                  Activities,
	"NodeNotReady":               Activities,
	"NodeSchedulable":            Activities,
	"Starting":                   Activities,
	"VolumeResizeSuccessful":     Activities,
	"FileSystemResizeSuccessful": Activities,
	"SuccessfulAttachVolume":     Activities,
	"SuccessfulMountVolume":      Activities,
	"Rebooted":                   Activities,
	"ContainerGCFailed":          Activities,
	"ImageGCFailed":              Activities,
	"NodeAllocatableEnforced":    Activities,
	"SandboxChanged":             Changes,

	// Seen in the wild, not keys of our current lib
	"Completed":         Activities,
	"NoPods":            Activities,
	"NotTriggerScaleUp": Alerts,
	"SawCompletedJob":   Activities,
	"ScalingReplicaSet": Activities,
	"Scheduled":         Activities,
	"SuccessfulCreate":  Activities,
	"SuccessfulDelete":  Activities,

	// HPA events
	"SuccessfulRescale":            Activities,
	"DesiredReplicasComputed":      Others,
	"FailedComputeMetricsReplicas": Alerts,
	"FailedRescale":                Alerts,

	"EnsuringLoadBalancer":        Activities,
	"AddedInterface":              Changes,
	"BuildStarted":                Activities,
	"ScaleDown":                   Activities,
	"BuildCompleted":              Activities,
	"DeploymentCreated":           Changes,
	"OperationStarted":            Activities,
	"ResourceUpdated":             Changes,
	"OperationCompleted":          Activities,
	"WaitForFirstConsumer":        Others,
	"Running":                     Activities,
	"Pending":                     Others,
	"ProvisioningSucceeded":       Activities,
	"Succeeded":                   Activities,
	"NodeNotSchedulable":          Alerts,
	"Updated":                     Changes,
	"Deleted":                     Changes,
	"Inject":                      Others,
	"DeletingNode":                Activities,
	"RemovingNode":                Activities,
	"ReplicationControllerScaled": Activities,
	"DetectedUnhealthy":           Alerts,
	"ConnectivityRestored":        Alerts,
	"DeletingLoadBalancer":        Changes,
	"JobAlreadyActive":            Others,
	"BuildFailed":                 Alerts,
	"NeedsReinstall":              Alerts,
	"AllRequirementsMet":          Activities,
	"InstallSucceeded":            Activities,
	"InstallWaiting":              Others,
	"CreatedSCCRanges":            Changes,

	// Service events
	"UpdatedLoadBalancer": Changes,

	// Pod events
	"TriggeredScaleUp": Activities,
	"RELOAD":           Changes,
	"EvictedByVPA":     Activities,

	// Statefulset events
	"ScaleUp": Activities,

	// Configmap events
	"ScaledUpGroup":  Activities,
	"ScaleDownEmpty": Activities,
	"CREATE":         Activities,

	// PVC events
	"Provisioning":         Activities,
	"ExternalProvisioning": Activities,
	"Resizing":             Activities,

	// Node events
	"NodeHasInsufficientMemory": Alerts,
}

// Mapper maps Kubernetes events to StackState events.
type Mapper struct {
	urn                     urn.Builder
	clusterName             string
	sourceType              string
	eventCategoriesOverride map[string]EventCategory
	logger                  *zap.Logger
	thrownCategoryWarnings  sync.Map
}

// New creates a mapper for one cluster. clusterType sets both the URN prefix
// and the event source type, as the cluster agent's OpenShift detection does.
func New(clusterType urn.ClusterType, clusterName string, eventCategoriesOverride map[string]EventCategory, logger *zap.Logger) *Mapper {
	return &Mapper{
		urn:                     urn.NewURNBuilder(clusterType, clusterName),
		clusterName:             clusterName,
		sourceType:              string(clusterType),
		eventCategoriesOverride: eventCategoriesOverride,
		logger:                  logger,
	}
}

// SourceType is the key the events are grouped under in the intake payload.
func (k *Mapper) SourceType() string {
	return k.sourceType
}

// Map maps one Kubernetes event. It fails for events without a kind, reason,
// message or involved object name, which the cluster agent skips.
func (k *Mapper) Map(event *v1.Event) (telemetry.Event, error) {
	if err := checkEvent(event); err != nil {
		return telemetry.Event{}, err
	}

	mEvent := telemetry.Event{
		Title:          fmt.Sprintf("%s - %s %s (%dx)", event.Reason, event.InvolvedObject.Name, event.InvolvedObject.Kind, event.Count),
		Host:           getHostName(event, k.clusterName),
		SourceTypeName: k.sourceType,
		Priority:       telemetry.EventPriorityNormal,
		AlertType:      k.getAlertType(event),
		EventType:      event.Reason,
		Ts:             getTimeStamp(event),
		Tags:           k.getTags(event),
		EventContext: &telemetry.EventContext{
			Source:             k.sourceType,
			Category:           string(k.getCategory(event)),
			SourceIdentifier:   string(event.GetUID()),
			ElementIdentifiers: k.externalIdentifierForInvolvedObject(event),
			SourceLinks:        []telemetry.SourceLink{},
			Data:               map[string]interface{}{},
		},
		Text: event.Message,
	}

	return mEvent, nil
}

func checkEvent(event *v1.Event) error {
	// As some fields are optional, we want to avoid evaluating empty values.
	if event == nil || event.InvolvedObject.Kind == "" {
		return errors.New("could not retrieve some parent attributes of the event")
	}

	if event.Reason == "" || event.Message == "" || event.InvolvedObject.Name == "" {
		return errors.New("could not retrieve some attributes of the event")
	}

	return nil
}

func getHostName(event *v1.Event, clusterName string) string {
	if event.InvolvedObject.Kind == "Node" || event.InvolvedObject.Kind == "Pod" {
		if clusterName != "" {
			return fmt.Sprintf("%s-%s", event.Source.Host, clusterName)
		}

		return event.Source.Host
	}

	// If hostname was not defined, the aggregator will then set the local hostname
	return ""
}

func (k *Mapper) getCategory(event *v1.Event) EventCategory {
	if category, ok := k.eventCategoriesOverride[event.Reason]; ok {
		return category
	}

	alertType := k.getAlertType(event)
	if alertType == telemetry.EventAlertTypeWarning || alertType == telemetry.EventAlertTypeError {
		return Alerts
	}

	if category, ok := DefaultEventCategoriesMap[event.Reason]; ok {
		return category
	}
	if _, exists := k.thrownCategoryWarnings.LoadOrStore(event.Reason, true); !exists {
		k.logger.Warn("Kubernetes event has unknown reason, categorising as 'Others'",
			zap.String("reason", event.Reason),
			zap.String("involved_object", event.InvolvedObject.Kind+"/"+event.InvolvedObject.Name))
	}
	return Others
}

func (k *Mapper) getAlertType(event *v1.Event) telemetry.EventAlertType {
	switch strings.ToLower(event.Type) {
	case "normal":
		return telemetry.EventAlertTypeInfo
	case "warning":
		return telemetry.EventAlertTypeWarning
	default:
		k.logger.Warn("Unhandled kubernetes event type, falling back to info", zap.String("type", event.Type))
		return telemetry.EventAlertTypeInfo
	}
}

func getTimeStamp(event *v1.Event) int64 {
	return event.LastTimestamp.Unix()
}

func (k *Mapper) getTags(event *v1.Event) []string {
	tags := []string{}

	if event.InvolvedObject.Namespace != "" {
		tags = append(tags, fmt.Sprintf("kube_namespace:%s", event.InvolvedObject.Namespace))
	}

	if event.InvolvedObject.FieldPath != "" && getContainerNameFromEvent(event) != "" {
		tags = append(tags, fmt.Sprintf("kube_container_name:%s", getContainerNameFromEvent(event)))
	}

	tags = append(tags,
		fmt.Sprintf("source_component:%s", event.Source.Component),
		fmt.Sprintf("kube_object_name:%s", event.InvolvedObject.Name),
		fmt.Sprintf("kube_object_kind:%s", event.InvolvedObject.Kind),
		fmt.Sprintf("kube_cluster_name:%s", k.clusterName),
		fmt.Sprintf("kube_reason:%s", event.Reason),
		fmt.Sprintf("alert_type:%s", k.getAlertType(event)),
	)

	return tags
}

func (k *Mapper) externalIdentifierForInvolvedObject(event *v1.Event) []string {
	identifiers := []string{}
	namespace := event.InvolvedObject.Namespace
	obj := event.InvolvedObject

	if event.InvolvedObject.FieldPath != "" && getContainerNameFromEvent(event) != "" {
		identifiers = append(identifiers, k.urn.BuildContainerExternalID(namespace, obj.Name, getContainerNameFromEvent(event)))
	}

	// An unknown kind yields an empty URN plus another empty identifier; the
	// cluster agent sends both.
	externalID, err := k.urn.BuildExternalID(obj.Kind, namespace, obj.Name)
	identifiers = append(identifiers, externalID)
	if err != nil {
		k.logger.Debug("Unknown involved object kind",
			zap.String("kind", obj.Kind), zap.String("object", namespace+"/"+obj.Name), zap.String("event", event.Name))
		identifiers = append(identifiers, "")
	}

	return identifiers
}

var containerFieldPath = regexp.MustCompile("spec.containers{(.*?)}")

func getContainerNameFromEvent(event *v1.Event) string {
	containerName := ""

	if event.InvolvedObject.FieldPath != "" {
		containerNameSubmatch := containerFieldPath.FindStringSubmatch(event.InvolvedObject.FieldPath)

		if len(containerNameSubmatch) == 2 {
			containerName = containerNameSubmatch[1]
		}
	}

	return containerName
}
