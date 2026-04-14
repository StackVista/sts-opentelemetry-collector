package k8scrdreceiver

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// getStorageVersion returns the storage version of a CRD.
// If no storage version is marked, it returns the first version.
// Returns empty string if the CRD has no versions.
func getStorageVersion(crd *apiextensionsv1.CustomResourceDefinition) string {
	for _, version := range crd.Spec.Versions {
		if version.Storage {
			return version.Name
		}
	}

	// Fallback to first version if no storage version marked
	if len(crd.Spec.Versions) > 0 {
		return crd.Spec.Versions[0].Name
	}

	return ""
}

// convertUnstructuredToCRD converts an Unstructured object to a CRD.
func convertUnstructuredToCRD(u *unstructured.Unstructured, crd *apiextensionsv1.CustomResourceDefinition) error {
	bytes, err := u.MarshalJSON()
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, crd)
}

// buildCRLogRecord creates an OTLP log record from a CR and event type.
// This function contains the pure log-building logic and can be tested independently.
func buildCRLogRecord(
	cr *unstructured.Unstructured, eventType watch.EventType, timestamp time.Time,
) (plog.Logs, error) {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()

	// Set resource attributes - namespace is a property of the resource itself
	if cr.GetNamespace() != "" {
		resourceLogs.Resource().Attributes().PutStr(attrK8sNamespaceName, cr.GetNamespace())
	}

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().SetName(scopeName)

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
	logRecord.SetEventName(eventNameCR)
	logRecord.Attributes().PutStr(attrK8sResourceName, cr.GetKind())
	logRecord.Attributes().PutStr(attrK8sResourceGroup, cr.GroupVersionKind().Group)
	logRecord.Attributes().PutStr(attrK8sResourceVersion, cr.GroupVersionKind().Version)
	logRecord.Attributes().PutStr(attrEventDomain, eventDomainK8s)
	logRecord.Attributes().PutStr(attrK8sObjectName, cr.GetName())
	if cr.GetNamespace() != "" {
		logRecord.Attributes().PutStr(attrK8sNamespaceName, cr.GetNamespace())
	}

	return logs, nil
}

// buildCRDLogRecord creates an OTLP log record from a CRD and event type.
// CRDs are cluster-scoped resources that define custom resource types.
func buildCRDLogRecord(
	crd *unstructured.Unstructured, eventType watch.EventType, timestamp time.Time,
) (plog.Logs, error) {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()

	// CRDs are cluster-scoped - no namespace in resource attributes

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().SetName(scopeName)

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

	// Add attributes for easier filtering
	// CRDs are always kind "CustomResourceDefinition" in group "apiextensions.k8s.io"
	logRecord.SetEventName(eventNameCRD)
	logRecord.Attributes().PutStr(attrK8sResourceName, "CustomResourceDefinition")
	logRecord.Attributes().PutStr(attrK8sResourceGroup, "apiextensions.k8s.io")
	logRecord.Attributes().PutStr(attrK8sResourceVersion, "v1")
	logRecord.Attributes().PutStr(attrEventDomain, eventDomainK8s)
	logRecord.Attributes().PutStr(attrK8sObjectName, crd.GetName())
	// No namespace attribute for cluster-scoped CRDs

	return logs, nil
}

// formatGVRKey returns a unique key for a GroupVersionResource.
func formatGVRKey(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("%s/%s/%s", gvr.Group, gvr.Version, gvr.Resource)
}

// isPermissionDenied checks if an error is a Kubernetes RBAC permission denied error
func isPermissionDenied(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err)
}

// emitLog builds and sends a log record to the consumer.
// This is the unified emit helper used by both pull and watch modes.
func emitLog(
	ctx context.Context,
	cons consumer.Logs,
	obj *unstructured.Unstructured,
	eventType watch.EventType,
	buildLogFn func(*unstructured.Unstructured, watch.EventType, time.Time) (plog.Logs, error),
) error {
	logs, err := buildLogFn(obj, eventType, time.Now())
	if err != nil {
		return err
	}
	return cons.ConsumeLogs(ctx, logs)
}
