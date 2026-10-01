package stsk8stopologyexporter //nolint:testpackage // Shared fixtures for the internal tests.

import (
	"encoding/json"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const goldenClusterName = "golden-cluster"

func loadFixture(t *testing.T) []*unstructured.Unstructured {
	t.Helper()
	data, err := os.ReadFile("testdata/cluster.json")
	require.NoError(t, err)
	var raws []json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raws))
	objects := make([]*unstructured.Unstructured, 0, len(raws))
	for _, raw := range raws {
		obj := &unstructured.Unstructured{}
		require.NoError(t, obj.UnmarshalJSON(raw))
		objects = append(objects, obj)
	}
	return objects
}

// objectRecord mirrors k8sresourcereceiver emit.BuildObjectLogRecord.
func objectRecord(t *testing.T, obj *unstructured.Unstructured, eventType string) plog.Logs {
	t.Helper()
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	resourceLogs.Resource().Attributes().PutStr("k8s.cluster.name", goldenClusterName)
	record := resourceLogs.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	body := record.Body().SetEmptyMap()
	require.NoError(t, body.PutEmptyMap("object").FromRaw(obj.Object))
	body.PutStr("type", eventType)
	record.SetEventName(eventNameObject)
	gvk := obj.GroupVersionKind()
	record.Attributes().PutStr(attrKind, gvk.Kind)
	record.Attributes().PutStr(attrGroup, gvk.Group)
	record.Attributes().PutStr(attrVersion, gvk.Version)
	record.Attributes().PutStr("k8s.object.name", obj.GetName())
	return logs
}

func boundaryRecord(boundary, id string, complete bool) plog.Logs {
	logs := plog.NewLogs()
	record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.SetEventName(eventNameSnapshotBoundary)
	record.Attributes().PutStr(attrBoundary, boundary)
	if id != "" {
		record.Attributes().PutStr(attrSnapshotID, id)
	}
	if boundary == "end" {
		record.Attributes().PutBool(attrSnapshotComplete, complete)
	}
	return logs
}

// snapshotRecords is one complete Cluster Observer snapshot of objects.
func snapshotRecords(t *testing.T, id string, objects []*unstructured.Unstructured) []plog.Logs {
	t.Helper()
	out := []plog.Logs{boundaryRecord("start", id, false)}
	for _, obj := range objects {
		out = append(out, objectRecord(t, obj, "ADDED"))
	}
	return append(out, boundaryRecord("end", id, true))
}

type topologyDocument struct {
	Components []any `json:"components"`
	Relations  []any `json:"relations"`
}

func loadGolden(t *testing.T) topologyDocument {
	t.Helper()
	data, err := os.ReadFile("testdata/golden.json")
	require.NoError(t, err)
	var doc topologyDocument
	require.NoError(t, json.Unmarshal(data, &doc))
	return doc
}

// normalize sorts elements by their JSON encoding, as the golden generator does.
func normalize[T any](t *testing.T, items []T) []any {
	t.Helper()
	encoded := make([]string, 0, len(items))
	for _, item := range items {
		raw, err := json.Marshal(item)
		require.NoError(t, err)
		encoded = append(encoded, string(raw))
	}
	sort.Strings(encoded)
	out := make([]any, 0, len(encoded))
	for _, raw := range encoded {
		var value any
		require.NoError(t, json.Unmarshal([]byte(raw), &value))
		out = append(out, value)
	}
	return out
}

func testLogger(t *testing.T) *zap.Logger {
	t.Helper()
	return zaptest.NewLogger(t)
}

func testConfig() *Config {
	cfg, _ := createDefaultConfig().(*Config)
	cfg.Endpoint = "http://127.0.0.1:1/stsAgent/intake"
	cfg.APIKey = "test-key"
	cfg.ClusterName = goldenClusterName
	cfg.HandoverDelay = 0
	return cfg
}
