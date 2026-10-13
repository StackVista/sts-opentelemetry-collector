package stsk8seventsexporter //nolint:testpackage // Drives the exporter's flush directly.

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/transactional"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap/zaptest"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8seventsexporter/internal/eventmapper"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8seventsexporter/internal/urn"
)

const (
	goldenClusterName = "golden-cluster"
	goldenDefaultHost = "agent-host"
)

func loadFixture(t *testing.T) []map[string]any {
	t.Helper()
	data, err := os.ReadFile("testdata/objects.json")
	require.NoError(t, err)
	var objects []map[string]any
	require.NoError(t, json.Unmarshal(data, &objects))
	return objects
}

// watchRecords mirrors k8sobjectsreceiver's watch-mode log records.
func watchRecords(t *testing.T, watchType string, objects []map[string]any) plog.Logs {
	t.Helper()
	logs := plog.NewLogs()
	records := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for _, object := range objects {
		record := records.AppendEmpty()
		resource := "events"
		if object["kind"] == "Pod" {
			resource = "pods"
		}
		record.Attributes().PutStr(attrResourceName, resource)
		require.NoError(t, record.Body().SetEmptyMap().FromRaw(map[string]any{"type": watchType, "object": object}))
	}
	return logs
}

type intakeCapture struct {
	mu       sync.Mutex
	status   []int
	payloads []transactional.IntakePayload
	raw      []map[string]any
	apiKeys  []string
	requests int
}

func (c *intakeCapture) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reader, err := gzip.NewReader(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(reader)
		require.NoError(t, err)
		c.mu.Lock()
		defer c.mu.Unlock()
		c.requests++
		c.apiKeys = append(c.apiKeys, r.Header.Get("sts-api-key"))
		status := http.StatusOK
		if len(c.status) > 0 {
			status, c.status = c.status[0], c.status[1:]
		}
		if status == http.StatusOK {
			var payload transactional.IntakePayload
			require.NoError(t, json.Unmarshal(body, &payload))
			var raw map[string]any
			require.NoError(t, json.Unmarshal(body, &raw))
			c.payloads = append(c.payloads, payload)
			c.raw = append(c.raw, raw)
		}
		w.WriteHeader(status)
	}
}

func (c *intakeCapture) snapshot() ([]transactional.IntakePayload, []map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]transactional.IntakePayload(nil), c.payloads...), append([]map[string]any(nil), c.raw...)
}

func testExporter(t *testing.T, clusterType string, configure func(*Config)) (*eventsExporter, *intakeCapture) {
	t.Helper()
	capture := &intakeCapture{}
	server := httptest.NewServer(capture.handler(t))
	t.Cleanup(server.Close)
	cfg, _ := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL + intakeSuffix
	cfg.APIKey = "test-key"
	cfg.ClusterName = goldenClusterName
	cfg.ClusterType = clusterType
	cfg.DefaultHost = goldenDefaultHost
	cfg.EventCategories = map[string]string{"MysteryChange": "Changes"}
	if configure != nil {
		configure(cfg)
	}
	require.NoError(t, cfg.Validate())
	logger := zaptest.NewLogger(t)
	sender := &intakeSender{
		client: server.Client(), endpoint: cfg.Endpoint, apiKey: string(cfg.APIKey),
		maxAttempts: cfg.MaxAttempts, backoff: func(int) time.Duration { return 0 },
	}
	mapper := eventmapper.New(urn.ClusterTypeFromString(cfg.ClusterType), cfg.ClusterName, cfg.categoryOverrides(), logger)
	return newEventsExporter(cfg, logger, sender, mapper, "collector-pod"), capture
}

func consumeFixture(t *testing.T, exp *eventsExporter) {
	t.Helper()
	require.NoError(t, exp.consumeLogs(context.Background(), watchRecords(t, "ADDED", loadFixture(t))))
}
