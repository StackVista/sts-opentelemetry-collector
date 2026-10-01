package stsk8stopologyexporter //nolint:testpackage // Exercises delivery cancellation inside the exporter.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8stopologyexporter/internal/apiserver"
	collectors "github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8stopologyexporter/internal/topologycollectors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	coreV1 "k8s.io/api/core/v1"
)

// A former leader must not complete its snapshot after the observer reports
// that it stopped collecting.
func TestResetStopsSnapshotDeliveryInFlight(t *testing.T) {
	var requests atomic.Int32
	firstArrived := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			close(firstArrived)
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
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
	select {
	case <-firstArrived:
	case <-time.After(10 * time.Second):
		t.Fatal("the first intake request never arrived")
	}

	require.NoError(t, exp.ConsumeLogs(context.Background(), boundaryRecord("reset", "", false)))
	close(release)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(1), requests.Load(), "no chunk, and so no stop_snapshot, may follow a reset")
}

type panickingClient struct{ *cacheClient }

func (panickingClient) GetPods() ([]coreV1.Pod, error) { panic("collector bug") }

var _ apiserver.APICollectorClient = panickingClient{}

func TestCollectorPanicAbortsSnapshot(t *testing.T) {
	cfg := testConfig()
	exp := newTopologyExporter(cfg, testLogger(t), nil)
	client := panickingClient{&cacheClient{objects: map[objectKey]storedObject{}, logger: exp.logger}}
	result, err := collectTopology(client, exp.instance, collectors.Kubernetes, cfg, exp.logger)
	require.ErrorIs(t, err, errCollectPanic)
	assert.Empty(t, result.components, "a partial snapshot must never be sent")
}
