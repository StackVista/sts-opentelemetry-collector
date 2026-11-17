//go:build integration

package integration_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type capturedRecord struct {
	Body       map[string]any
	Attributes map[string]any
}

func records(sink *consumertest.LogsSink) []capturedRecord {
	var result []capturedRecord
	for _, batch := range sink.AllLogs() {
		for _, resource := range batch.ResourceLogs().All() {
			for _, scope := range resource.ScopeLogs().All() {
				for _, record := range scope.LogRecords().All() {
					var body map[string]any
					if record.Body().Type() == pcommon.ValueTypeMap {
						body = record.Body().Map().AsRaw()
					}
					result = append(result, capturedRecord{Body: body, Attributes: record.Attributes().AsRaw()})
				}
			}
		}
	}
	return result
}

func object(record capturedRecord) *unstructured.Unstructured {
	raw, _ := record.Body["object"].(map[string]any)
	return &unstructured.Unstructured{Object: raw}
}

func identity(record capturedRecord) string {
	obj := object(record)
	if obj.GetAPIVersion() != testGroup+"/v1" {
		return ""
	}
	kind := obj.GetKind()
	if kind != "Widget" && kind != "Gadget" && kind != "Fleet" {
		return ""
	}
	return strings.ToLower(kind) + "s/" + obj.GetNamespace() + "/" + obj.GetName()
}

func objectState(all []capturedRecord) []string {
	state := make(map[string]bool)
	for _, record := range all {
		key := identity(record)
		if key == "" {
			continue
		}
		if record.Body["type"] == "DELETED" {
			delete(state, key)
		} else {
			state[key] = true
		}
	}
	result := make([]string, 0, len(state))
	for key := range state {
		result = append(result, key)
	}
	slices.Sort(result)
	return result
}

func sameObjects(actual, expected []string) bool {
	sorted := slices.Clone(expected)
	slices.Sort(sorted)
	return slices.Equal(actual, sorted)
}

func uniqueObjects(all []capturedRecord) bool {
	seen := make(map[string]bool)
	for _, record := range all {
		key := identity(record)
		if key == "" || record.Body["type"] == "DELETED" {
			continue
		}
		if seen[key] {
			return false
		}
		seen[key] = true
	}
	return true
}

func completeSnapshots(all []capturedRecord) [][]capturedRecord {
	var result [][]capturedRecord
	var current []capturedRecord
	var id any
	for _, record := range all {
		switch record.Attributes["k8s.snapshot.boundary"] {
		case "start":
			current, id = []capturedRecord{}, record.Attributes["k8s.snapshot.id"]
		case "reset":
			current, id = nil, nil
		case "end":
			if current != nil && id != nil && record.Attributes["k8s.snapshot.id"] == id &&
				record.Attributes["k8s.snapshot.complete"] == true {
				result = append(result, current)
			}
			current, id = nil, nil
		default:
			if current != nil {
				current = append(current, record)
			}
		}
	}
	return result
}

func waitObjects(t *testing.T, sink *consumertest.LogsSink, expected []string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return sameObjects(objectState(records(sink)), expected)
	}, time.Minute, 200*time.Millisecond, "export did not reach %v", expected)
}

func TestSnapshotAssertions(t *testing.T) {
	boundary := func(kind, id string, complete bool) capturedRecord {
		return capturedRecord{Attributes: map[string]any{
			"k8s.snapshot.boundary": kind, "k8s.snapshot.id": id, "k8s.snapshot.complete": complete,
		}}
	}
	obj := capturedRecord{Body: map[string]any{"type": "ADDED", "object": map[string]any{
		"apiVersion": testGroup + "/v1", "kind": "Widget",
		"metadata": map[string]any{"namespace": teamA, "name": "both"},
	}}}
	require.False(t, uniqueObjects([]capturedRecord{obj, obj}))
	require.True(t, uniqueObjects([]capturedRecord{obj}))
	for _, end := range []capturedRecord{boundary("reset", "a", false),
		boundary("end", "b", true), boundary("end", "a", false)} {
		require.Empty(t, completeSnapshots([]capturedRecord{boundary("start", "a", false), obj, end}))
	}
	require.Len(t, completeSnapshots([]capturedRecord{boundary("start", "a", false),
		obj, boundary("end", "a", true)}), 1)
	sink := &consumertest.LogsSink{}
	batch := plog.NewLogs()
	entries := batch.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	start := entries.AppendEmpty()
	require.NoError(t, start.Attributes().FromRaw(boundary("start", "a", false).Attributes))
	item := entries.AppendEmpty()
	require.NoError(t, item.Body().SetEmptyMap().FromRaw(obj.Body))
	end := entries.AppendEmpty()
	require.NoError(t, end.Attributes().FromRaw(boundary("end", "a", true).Attributes))
	require.NoError(t, sink.ConsumeLogs(context.Background(), batch))
	snapshots := completeSnapshots(records(sink))
	require.Len(t, snapshots, 1)
	require.Equal(t, []string{"widgets/" + teamA + "/both"}, objectState(snapshots[0]))
}
