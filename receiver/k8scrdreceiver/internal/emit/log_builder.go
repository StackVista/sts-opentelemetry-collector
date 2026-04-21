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

// BuildCRLogRecord creates an OTLP log record from a CR and event type.
func BuildCRLogRecord(
	cr *unstructured.Unstructured, eventType watch.EventType, timestamp time.Time, clusterName string,
) (plog.Logs, error) {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()

	// Set resource attributes
	if clusterName != "" {
		resourceLogs.Resource().Attributes().PutStr(AttrK8sClusterName, clusterName)
	}
	if cr.GetNamespace() != "" {
		resourceLogs.Resource().Attributes().PutStr(AttrK8sNamespaceName, cr.GetNamespace())
	}

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().SetName(ScopeName)

	logRecord := scopeLogs.LogRecords().AppendEmpty()
	logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(timestamp))

	// Build log body with the CR object + event type
	bodyMap := logRecord.Body().SetEmptyMap()

	// Add the CR object (use .Object directly - already a map[string]interface{})
	if err := bodyMap.PutEmptyMap("object").FromRaw(cr.Object); err != nil {
		return logs, fmt.Errorf("failed to set object in body: %w", err)
	}

	// Add event type
	bodyMap.PutStr("type", string(eventType))

	// Add attributes for easier filtering
	logRecord.SetEventName(EventNameCR)
	logRecord.Attributes().PutStr(AttrK8sResourceName, cr.GetKind())
	logRecord.Attributes().PutStr(AttrK8sResourceGroup, cr.GroupVersionKind().Group)
	logRecord.Attributes().PutStr(AttrK8sResourceVersion, cr.GroupVersionKind().Version)
	logRecord.Attributes().PutStr(AttrEventDomain, EventDomainK8s)
	logRecord.Attributes().PutStr(AttrK8sObjectName, cr.GetName())
	if cr.GetNamespace() != "" {
		logRecord.Attributes().PutStr(AttrK8sNamespaceName, cr.GetNamespace())
	}

	return logs, nil
}

// BuildCRDLogRecord creates an OTLP log record from a CRD and event type.
// CRDs are cluster-scoped resources that define custom resource types.
func BuildCRDLogRecord(
	crd *unstructured.Unstructured, eventType watch.EventType, timestamp time.Time, clusterName string,
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

	// Build log body with the CRD object + event type
	bodyMap := logRecord.Body().SetEmptyMap()

	// Add the CRD object (use .Object directly - already a map[string]interface{})
	if err := bodyMap.PutEmptyMap("object").FromRaw(crd.Object); err != nil {
		return logs, fmt.Errorf("failed to set object in body: %w", err)
	}

	// Add event type
	bodyMap.PutStr("type", string(eventType))

	// Extract the CRD's defined group and kind from spec — these identify what
	// custom resource type this CRD defines, not the CRD API type itself.
	crdGroup, _, _ := unstructured.NestedString(crd.Object, "spec", "group")
	crdKind, _, _ := unstructured.NestedString(crd.Object, "spec", "names", "kind")

	logRecord.SetEventName(EventNameCRD)
	logRecord.Attributes().PutStr(AttrK8sResourceName, crdKind)
	logRecord.Attributes().PutStr(AttrK8sResourceGroup, crdGroup)
	logRecord.Attributes().PutStr(AttrK8sResourceVersion, "v1")
	logRecord.Attributes().PutStr(AttrEventDomain, EventDomainK8s)
	logRecord.Attributes().PutStr(AttrK8sObjectName, crd.GetName())

	return logs, nil
}

// FormatGVRKey returns a unique key for a GroupVersionResource.
func FormatGVRKey(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("%s/%s/%s", gvr.Group, gvr.Version, gvr.Resource)
}

// Log builds and sends a log record to the consumer.
func Log(
	ctx context.Context,
	cons consumer.Logs,
	obj *unstructured.Unstructured,
	eventType watch.EventType,
	clusterName string,
	buildLogFn func(*unstructured.Unstructured, watch.EventType, time.Time, string) (plog.Logs, error),
) error {
	logs, err := buildLogFn(obj, eventType, time.Now(), clusterName)
	if err != nil {
		return err
	}
	return cons.ConsumeLogs(ctx, logs)
}
