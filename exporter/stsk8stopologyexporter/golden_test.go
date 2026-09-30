package stsk8stopologyexporter //nolint:testpackage // Feeds the internal store and collectors with observer records.

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/model/topology"
	"github.com/StackVista/stackstate-receiver-go-client/pkg/transactional"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8stopologyexporter/internal/hostname"
	collectors "github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8stopologyexporter/internal/topologycollectors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.uber.org/zap/zaptest"
)

// testdata/golden.json is the cluster-agent output for testdata/cluster.json,
// produced by testdata/agentgolden/generate.sh.
func TestTopologyMatchesClusterAgent(t *testing.T) {
	hostname.SetClusterName(goldenClusterName)
	cfg := testConfig()
	exp := newTopologyExporter(cfg, zaptest.NewLogger(t), nil)
	for _, logs := range snapshotRecords(t, "1", loadFixture(t)) {
		require.NoError(t, exp.consumeLogs(context.Background(), logs))
	}

	objects, ok := exp.store.view(time.Now(), cfg.SnapshotMaxAge)
	require.True(t, ok)
	result, err := collectTopology(&cacheClient{objects: objects, logger: exp.logger},
		exp.instance, collectors.Kubernetes, cfg, exp.logger)
	require.NoError(t, err)
	require.Empty(t, result.errors)

	golden := loadGolden(t)
	assert.Equal(t, golden.Components, normalize(t, result.components))
	assert.Equal(t, golden.Relations, normalize(t, result.relations))
}

type intakeCapture struct {
	mu       sync.Mutex
	payloads []transactional.IntakePayload
	headers  []http.Header
}

func (c *intakeCapture) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reader, err := gzip.NewReader(r.Body)
		if !assert.NoError(t, err) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(reader)
		require.NoError(t, err)
		var payload transactional.IntakePayload
		require.NoError(t, json.Unmarshal(body, &payload))
		c.mu.Lock()
		c.payloads = append(c.payloads, payload)
		c.headers = append(c.headers, r.Header.Clone())
		c.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}
}

func (c *intakeCapture) snapshot() ([]transactional.IntakePayload, []http.Header) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]transactional.IntakePayload(nil), c.payloads...), append([]http.Header(nil), c.headers...)
}

func TestExporterSendsChunkedSnapshotThroughIntake(t *testing.T) {
	capture := &intakeCapture{}
	server := httptest.NewServer(capture.handler(t))
	defer server.Close()

	cfg := testConfig()
	cfg.Endpoint = server.URL + "/stsAgent/intake"
	cfg.MaxElementsPerRequest = 25
	cfg.Interval = time.Hour
	cfg.SnapshotMaxAge = 2 * time.Hour

	exp, err := NewFactory().CreateLogs(context.Background(), exportertest.NewNopSettings(NewFactory().Type()), cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, exp.Shutdown(context.Background())) }()

	for _, logs := range snapshotRecords(t, "1", loadFixture(t)) {
		require.NoError(t, exp.ConsumeLogs(context.Background(), logs))
	}

	golden := loadGolden(t)
	total := len(golden.Components) + len(golden.Relations)
	want := (total + cfg.MaxElementsPerRequest - 1) / cfg.MaxElementsPerRequest
	require.Eventually(t, func() bool {
		payloads, _ := capture.snapshot()
		return len(payloads) >= want
	}, 10*time.Second, 20*time.Millisecond)

	payloads, headers := capture.snapshot()
	require.Len(t, payloads, want)
	var components []topology.Component
	var relations []topology.Relation
	for i, payload := range payloads {
		assert.Equal(t, "test-key", headers[i].Get("sts-api-key"))
		assert.Equal(t, "gzip", headers[i].Get("Content-Encoding"))
		assert.Equal(t, goldenClusterName+"-cluster-topology", payload.InternalHostname)
		assert.Empty(t, payload.Health)
		require.Len(t, payload.Topologies, 1)
		chunk := payload.Topologies[0]
		assert.Equal(t, topology.Instance{Type: clusterTypeKubernetes, URL: goldenClusterName}, chunk.Instance)
		assert.Equal(t, i == 0, chunk.StartSnapshot, "only the first request starts the snapshot")
		assert.Equal(t, i == len(payloads)-1, chunk.StopSnapshot, "only the last request stops the snapshot")
		assert.LessOrEqual(t, len(chunk.Components)+len(chunk.Relations), cfg.MaxElementsPerRequest)
		components = append(components, chunk.Components...)
		relations = append(relations, chunk.Relations...)
	}
	assert.Equal(t, golden.Components, normalize(t, components))
	assert.Equal(t, golden.Relations, normalize(t, relations))
}

func TestExporterDoesNotSendWithoutCompleteSnapshot(t *testing.T) {
	capture := &intakeCapture{}
	server := httptest.NewServer(capture.handler(t))
	defer server.Close()

	cfg := testConfig()
	cfg.Endpoint = server.URL + "/stsAgent/intake"
	cfg.Interval = 20 * time.Millisecond
	cfg.SnapshotMaxAge = time.Hour

	exp, err := NewFactory().CreateLogs(context.Background(), exportertest.NewNopSettings(NewFactory().Type()), cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, exp.Shutdown(context.Background())) }()

	objects := loadFixture(t)
	for _, obj := range objects {
		require.NoError(t, exp.ConsumeLogs(context.Background(), objectRecord(t, obj, "ADDED")))
	}
	require.NoError(t, exp.ConsumeLogs(context.Background(), boundaryRecord("start", "1", false)))
	for _, obj := range objects {
		require.NoError(t, exp.ConsumeLogs(context.Background(), objectRecord(t, obj, "ADDED")))
	}
	require.NoError(t, exp.ConsumeLogs(context.Background(), boundaryRecord("end", "1", false)))

	time.Sleep(200 * time.Millisecond)
	payloads, _ := capture.snapshot()
	assert.Empty(t, payloads, "unbracketed or incomplete observer state must never reach the intake")
}
