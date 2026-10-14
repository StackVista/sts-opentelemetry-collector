package stsk8seventsexporter //nolint:testpackage // Drives the exporter's flush directly.

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/model/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func fixtureEvents(t *testing.T) []map[string]any {
	t.Helper()
	var events []map[string]any
	for _, object := range loadFixture(t) {
		if object["kind"] == "Event" {
			events = append(events, object)
		}
	}
	return events
}

func TestIntakePayloadShape(t *testing.T) {
	exp, capture := testExporter(t, clusterTypeKubernetes, nil)
	consumeFixture(t, exp)
	exp.flush(context.Background())

	payloads, raw := capture.snapshot()
	require.Len(t, payloads, 1)
	assert.Equal(t, "collector-pod", payloads[0].InternalHostname)
	assert.Equal(t, []any{}, raw[0]["topologies"])
	assert.Equal(t, []any{}, raw[0]["health"])
	assert.Equal(t, []any{}, raw[0]["metrics"])
	assert.Len(t, payloads[0].Events["kubernetes"], 10)
	assert.Equal(t, []string{"test-key"}, capture.apiKeys)
}

func TestBatchesByMaxEventsPerRequest(t *testing.T) {
	exp, capture := testExporter(t, clusterTypeKubernetes, func(cfg *Config) { cfg.MaxEventsPerRequest = 4 })
	consumeFixture(t, exp)
	exp.flush(context.Background())

	payloads, _ := capture.snapshot()
	require.Len(t, payloads, 3)
	sizes := make([]int, 0, len(payloads))
	for _, payload := range payloads {
		sizes = append(sizes, len(payload.Events["kubernetes"]))
	}
	assert.Equal(t, []int{4, 4, 2}, sizes)
}

func TestDeletedWatchEventsAreIgnored(t *testing.T) {
	exp, capture := testExporter(t, clusterTypeKubernetes, nil)
	require.NoError(t, exp.consumeLogs(context.Background(), watchRecords(t, "DELETED", loadFixture(t))))
	exp.flush(context.Background())

	assert.Zero(t, capture.requests)
}

func TestModifiedEventIsSentAgain(t *testing.T) {
	exp, capture := testExporter(t, clusterTypeKubernetes, nil)
	events := fixtureEvents(t)[:1]
	require.NoError(t, exp.consumeLogs(context.Background(), watchRecords(t, "ADDED", events)))
	require.NoError(t, exp.consumeLogs(context.Background(), watchRecords(t, "MODIFIED", events)))
	exp.flush(context.Background())

	payloads, _ := capture.snapshot()
	require.Len(t, payloads, 1)
	assert.Len(t, payloads[0].Events["kubernetes"], 2)
}

func TestIgnoresRecordsThatAreNotWatchEvents(t *testing.T) {
	exp, capture := testExporter(t, clusterTypeKubernetes, nil)
	logs := plog.NewLogs()
	records := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	records.AppendEmpty().Body().SetStr("not a map")
	unknown := records.AppendEmpty()
	unknown.Attributes().PutStr(attrResourceName, "configmaps")
	require.NoError(t, unknown.Body().SetEmptyMap().FromRaw(map[string]any{"type": "ADDED", "object": fixtureEvents(t)[0]}))
	require.NoError(t, exp.consumeLogs(context.Background(), logs))
	exp.flush(context.Background())

	assert.Zero(t, capture.requests)
}

func TestDefaultHostFallsBackToProducer(t *testing.T) {
	exp, capture := testExporter(t, clusterTypeKubernetes, func(cfg *Config) { cfg.DefaultHost = "" })
	deployment := fixtureEvents(t)[2]
	involved, ok := deployment["involvedObject"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "Deployment", involved["kind"])
	require.NoError(t, exp.consumeLogs(context.Background(), watchRecords(t, "ADDED", []map[string]any{deployment})))
	exp.flush(context.Background())

	payloads, _ := capture.snapshot()
	require.Len(t, payloads, 1)
	assert.Equal(t, "collector-pod", payloads[0].Events["kubernetes"][0].Host)
}

func TestAggregatorPostProcessing(t *testing.T) {
	exp, _ := testExporter(t, clusterTypeKubernetes, nil)
	exp.now = func() time.Time { return time.Unix(1234, 0) }
	exp.enqueue(telemetry.Event{Tags: []string{"b", "a", "b"}})

	require.Len(t, exp.pending, 1)
	assert.Equal(t, int64(1234), exp.pending[0].Ts)
	assert.Equal(t, goldenDefaultHost, exp.pending[0].Host)
	assert.Equal(t, []string{"a", "b"}, exp.pending[0].Tags)
}

func TestTransientFailureIsRetriedAtNextInterval(t *testing.T) {
	exp, capture := testExporter(t, clusterTypeKubernetes, func(cfg *Config) {
		cfg.MaxEventsPerRequest = 4
		cfg.MaxAttempts = 2
	})
	capture.status = []int{http.StatusOK, http.StatusServiceUnavailable, http.StatusBadGateway}
	consumeFixture(t, exp)
	exp.flush(context.Background())

	payloads, _ := capture.snapshot()
	require.Len(t, payloads, 1)
	assert.Len(t, exp.pending, 6, "the failed request and the one after it stay queued")

	exp.flush(context.Background())
	payloads, _ = capture.snapshot()
	require.Len(t, payloads, 3)
	assert.Empty(t, exp.pending)
	assert.Equal(t, "MysteryChange", payloads[1].Events["kubernetes"][0].EventType, "requeued events keep their order")
}

func TestPermanentFailureDropsOnlyThatRequest(t *testing.T) {
	exp, capture := testExporter(t, clusterTypeKubernetes, func(cfg *Config) { cfg.MaxEventsPerRequest = 4 })
	capture.status = []int{http.StatusBadRequest}
	consumeFixture(t, exp)
	exp.flush(context.Background())

	payloads, _ := capture.snapshot()
	require.Len(t, payloads, 2)
	assert.Equal(t, 3, capture.requests, "a rejected request is not retried")
	assert.Empty(t, exp.pending)
}

func TestBufferDropsOldestEvents(t *testing.T) {
	exp, capture := testExporter(t, clusterTypeKubernetes, func(cfg *Config) {
		cfg.MaxEventsPerRequest = 2
		cfg.MaxBufferedEvents = 4
	})
	consumeFixture(t, exp)
	require.Len(t, exp.pending, 4)
	assert.Equal(t, 6, exp.dropped)
	exp.flush(context.Background())

	payloads, _ := capture.snapshot()
	require.Len(t, payloads, 2)
	assert.Equal(t, "Created", payloads[0].Events["kubernetes"][0].EventType)
	assert.Equal(t, "OOMKilled", payloads[1].Events["kubernetes"][1].EventType)
	assert.Zero(t, exp.dropped)
}

func TestShutdownSendsBufferedEvents(t *testing.T) {
	exp, capture := testExporter(t, clusterTypeKubernetes, func(cfg *Config) { cfg.Interval = time.Hour })
	require.NoError(t, exp.start(context.Background(), nil))
	consumeFixture(t, exp)
	payloads, _ := capture.snapshot()
	require.Empty(t, payloads, "nothing is sent before the interval")

	require.NoError(t, exp.shutdown(context.Background()))
	payloads, _ = capture.snapshot()
	require.Len(t, payloads, 1, "shutdown sends what is buffered")
	assert.NotEmpty(t, payloads[0].Events[clusterTypeKubernetes])
	assert.Empty(t, exp.pending)
}

func TestStartAndShutdown(t *testing.T) {
	exp, capture := testExporter(t, clusterTypeKubernetes, func(cfg *Config) { cfg.Interval = 10 * time.Millisecond })
	require.NoError(t, exp.start(context.Background(), nil))
	consumeFixture(t, exp)
	require.Eventually(t, func() bool {
		payloads, _ := capture.snapshot()
		return len(payloads) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, exp.shutdown(context.Background()))
}

func TestEventsCarryClusterID(t *testing.T) {
	exp, capture := testExporter(t, clusterTypeKubernetes, func(cfg *Config) { cfg.Interval = time.Hour })
	exp.kubeClient = func() (kubernetes.Interface, error) {
		return fake.NewClientset(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: "4a7394ae-dc2e-4b36-aca9-47222b432701"}}), nil
	}
	require.NoError(t, exp.start(context.Background(), nil))
	consumeFixture(t, exp)
	require.NoError(t, exp.shutdown(context.Background()))

	payloads, _ := capture.snapshot()
	require.Len(t, payloads, 1)
	events := payloads[0].Events[clusterTypeKubernetes]
	require.NotEmpty(t, events)
	for _, event := range events {
		assert.Contains(t, event.Tags, "orch_cluster_id:4a7394ae-dc2e-4b36-aca9-47222b432701")
		assert.IsNonDecreasing(t, event.Tags)
	}
}

func TestEventsAreSentWithoutClusterIDWhenUnavailable(t *testing.T) {
	for name, tc := range map[string]struct {
		client kubernetes.Interface
		err    error
	}{
		"no client":    {err: assert.AnError},
		"no namespace": {client: fake.NewClientset()},
	} {
		t.Run(name, func(t *testing.T) {
			exp, _ := testExporter(t, clusterTypeKubernetes, func(cfg *Config) { cfg.Interval = time.Hour })
			exp.kubeClient = func() (kubernetes.Interface, error) { return tc.client, tc.err }
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			require.NoError(t, exp.start(ctx, nil))
			defer func() { require.NoError(t, exp.shutdown(context.Background())) }()
			assert.Empty(t, exp.clusterTag)
		})
	}
}

func TestAuthTypeNoneSkipsClusterID(t *testing.T) {
	exp, _ := testExporter(t, clusterTypeKubernetes, func(cfg *Config) { cfg.AuthType = authTypeNone })
	exp.kubeClient = func() (kubernetes.Interface, error) {
		t.Fatal("no Kubernetes client is created")
		return nil, assert.AnError
	}
	exp.resolveClusterTag(context.Background())
	assert.Empty(t, exp.clusterTag)
}
