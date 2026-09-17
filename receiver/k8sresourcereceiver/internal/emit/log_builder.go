package emit

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// BuildObjectLogRecord creates an OTLP log record from a k8s object and event
// type. eventName lets the caller pick the downstream log shape: EventNameCR
// for CR-sourced objects (CRD-discovered or CRD-backed statics), EventNameObject
// for plain static objects.
func BuildObjectLogRecord(
	obj *unstructured.Unstructured, eventType watch.EventType, timestamp time.Time,
	clusterName, eventName string, resourceAttributes map[string]string,
) (plog.Logs, error) {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()

	for key, value := range resourceAttributes {
		resourceLogs.Resource().Attributes().PutStr(key, value)
	}
	if clusterName != "" {
		resourceLogs.Resource().Attributes().PutStr(AttrK8sClusterName, clusterName)
	}
	if obj.GetNamespace() != "" {
		resourceLogs.Resource().Attributes().PutStr(AttrK8sNamespaceName, obj.GetNamespace())
	}

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().SetName(ScopeName)

	logRecord := scopeLogs.LogRecords().AppendEmpty()
	logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(timestamp))

	bodyMap := logRecord.Body().SetEmptyMap()
	if err := bodyMap.PutEmptyMap("object").FromRaw(obj.Object); err != nil {
		return logs, fmt.Errorf("failed to set object in body: %w", err)
	}
	bodyMap.PutStr("type", string(eventType))

	logRecord.SetEventName(eventName)
	logRecord.Attributes().PutStr(AttrK8sResourceKind, obj.GetKind())
	logRecord.Attributes().PutStr(AttrK8sResourceGroup, obj.GroupVersionKind().Group)
	logRecord.Attributes().PutStr(AttrK8sResourceVersion, obj.GroupVersionKind().Version)
	logRecord.Attributes().PutStr(AttrEventDomain, EventDomainK8s)
	logRecord.Attributes().PutStr(AttrK8sObjectName, obj.GetName())
	if obj.GetNamespace() != "" {
		logRecord.Attributes().PutStr(AttrK8sNamespaceName, obj.GetNamespace())
	}
	if summary, ok := podContainerSummary(obj); ok {
		logRecord.Attributes().PutInt(AttrK8sPodRestartCount, summary.restarts)
		logRecord.Attributes().PutInt(AttrK8sPodContainerCount, summary.total)
		logRecord.Attributes().PutInt(AttrK8sPodReadyContainerCount, summary.ready)
	}

	return logs, nil
}

type containerSummary struct {
	total, ready, restarts int64
}

func podContainerSummary(obj *unstructured.Unstructured) (containerSummary, bool) {
	summary := containerSummary{}
	if obj.GetKind() != "Pod" || obj.GroupVersionKind().Group != "" {
		return summary, false
	}
	raw, found, err := unstructured.NestedFieldNoCopy(obj.Object, "status", "containerStatuses")
	if err != nil || !found {
		return summary, false
	}
	statuses, ok := raw.([]interface{})
	if !ok {
		return summary, false
	}
	summary.total = int64(len(statuses))
	for _, value := range statuses {
		status, ok := value.(map[string]interface{})
		if !ok {
			return summary, false
		}
		count, _, err := unstructured.NestedInt64(status, "restartCount")
		if err != nil || count < 0 {
			return summary, false
		}
		ready, _, err := unstructured.NestedBool(status, "ready")
		if err != nil {
			return summary, false
		}
		if ready {
			summary.ready++
		}
		summary.restarts += count
	}
	return summary, true
}

// BuildCRDLogRecord creates an OTLP log record from a CRD and event type.
// CRDs are cluster-scoped resources that define custom resource types.
func BuildCRDLogRecord(
	crd *unstructured.Unstructured, eventType watch.EventType, timestamp time.Time, clusterName string,
	customResourcesWatched bool,
) (plog.Logs, error) {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()

	// CRDs are cluster-scoped — only cluster name in resource attributes
	if clusterName != "" {
		resourceLogs.Resource().Attributes().PutStr(AttrK8sClusterName, clusterName)
	}

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().SetName(ScopeName)

	logRecord := scopeLogs.LogRecords().AppendEmpty()
	logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(timestamp))

	bodyMap := logRecord.Body().SetEmptyMap()
	if err := bodyMap.PutEmptyMap("object").FromRaw(crd.Object); err != nil {
		return logs, fmt.Errorf("failed to set object in body: %w", err)
	}
	bodyMap.PutStr("type", string(eventType))

	// Extract the CRD's defined group and kind from spec — these identify what
	// custom resource type this CRD defines, not the CRD API type itself.
	crdGroup, _, _ := unstructured.NestedString(crd.Object, "spec", "group")
	crdKind, _, _ := unstructured.NestedString(crd.Object, "spec", "names", "kind")

	logRecord.SetEventName(EventNameCRD)
	logRecord.Attributes().PutStr(AttrK8sResourceKind, crdKind)
	logRecord.Attributes().PutStr(AttrK8sResourceGroup, crdGroup)
	logRecord.Attributes().PutStr(AttrK8sResourceVersion, "v1")
	logRecord.Attributes().PutBool(AttrK8sCRDCRsWatched, customResourcesWatched)
	logRecord.Attributes().PutStr(AttrEventDomain, EventDomainK8s)
	logRecord.Attributes().PutStr(AttrK8sObjectName, crd.GetName())

	return logs, nil
}

// FormatGVRKey returns a unique key for a GroupVersionResource.
func FormatGVRKey(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("%s/%s/%s", gvr.Group, gvr.Version, gvr.Resource)
}

// LogCRD builds and sends a CRD log record to the consumer.
func LogCRD(
	ctx context.Context, cons consumer.Logs, crd *unstructured.Unstructured,
	eventType watch.EventType, clusterName string, customResourcesWatched bool,
) error {
	logs, err := BuildCRDLogRecord(crd, eventType, time.Now(), clusterName, customResourcesWatched)
	if err != nil {
		return err
	}
	return cons.ConsumeLogs(ctx, logs)
}

// LogObject builds and sends an object log record to the consumer. eventName
// selects the downstream log shape (CR vs Object); see BuildObjectLogRecord.
func LogObject(
	ctx context.Context, cons consumer.Logs, obj *unstructured.Unstructured,
	eventType watch.EventType, clusterName, eventName string, resourceAttributes map[string]string,
) error {
	logs, err := BuildObjectLogRecord(obj, eventType, time.Now(), clusterName, eventName, resourceAttributes)
	if err != nil {
		return err
	}
	return cons.ConsumeLogs(ctx, logs)
}
