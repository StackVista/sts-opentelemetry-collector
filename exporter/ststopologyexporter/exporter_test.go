package ststopologyexporter_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/ststopologyexporter"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/ststopologyexporter/internal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap/zaptest"
)

//nolint:dupl
func TestExporter_pushResourcesData(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		require.Equal(t, "APIKEY", req.Header[http.CanonicalHeaderKey("sts-api-key")][0])
		require.Equal(t, "ms", req.Header[http.CanonicalHeaderKey("sts-time-format")][0])

		var payload internal.IntakeTopology
		err := json.NewDecoder(req.Body).Decode(&payload)
		require.NoError(t, err)
		require.Equal(t, 1, len(payload.Topologies))

		require.Equal(t, internal.Instance{
			Type: "opentelemetry",
			URL:  "collector",
		}, payload.Topologies[0].Instance)

		require.Equal(t, 3, len(payload.Topologies[0].Components))
		for _, component := range payload.Topologies[0].Components {
			tags := component.Data.Tags
			_, ok := tags["sts_api_key"]
			require.False(t, ok)
		}
		require.Equal(t, 2, len(payload.Topologies[0].Relations))
		for _, relation := range payload.Topologies[0].Relations {
			tags := relation.Data.Tags
			_, ok := tags["sts_api_key"]
			require.False(t, ok)
		}
		res.WriteHeader(200)
	}))
	exporter := newTestExporter(t, testServer.URL)
	err := exporter.ConsumeMetrics(context.TODO(), simpleMetrics())
	require.NoError(t, err)
}

func TestExporter_skipVirtualNodes(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		require.Fail(t, "No request should be sent")
	}))
	exporter := newTestExporter(t, testServer.URL)
	err := exporter.ConsumeMetrics(context.TODO(), virtualNodeMetrics())
	require.NoError(t, err)
}

//nolint:dupl
func TestExporter_simpleTrace(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		require.Equal(t, "APIKEY", req.Header[http.CanonicalHeaderKey("sts-api-key")][0])
		require.Equal(t, "ms", req.Header[http.CanonicalHeaderKey("sts-time-format")][0])

		var payload internal.IntakeTopology
		err := json.NewDecoder(req.Body).Decode(&payload)
		require.NoError(t, err)
		require.Equal(t, 1, len(payload.Topologies))

		require.Equal(t, internal.Instance{
			Type: "opentelemetry",
			URL:  "collector",
		}, payload.Topologies[0].Instance)

		require.Equal(t, 3, len(payload.Topologies[0].Components))
		for _, component := range payload.Topologies[0].Components {
			tags := component.Data.Tags
			_, ok := tags["sts_api_key"]
			require.False(t, ok)
		}
		// just service <-> instance
		require.Equal(t, 1, len(payload.Topologies[0].Relations))
		for _, relation := range payload.Topologies[0].Relations {
			tags := relation.Data.Tags
			_, ok := tags["sts_api_key"]
			require.False(t, ok)
		}
		res.WriteHeader(200)
	}))
	exporter := newTestExporter(t, testServer.URL)
	err := exporter.ConsumeTraces(context.TODO(), simpleTrace())
	require.NoError(t, err)

}

func TestExporter_sendCollection_httpErrorDoesNotPanic(t *testing.T) {
	// Create exporter with an endpoint that cannot be reached, forcing a non-nil error on httpClient.Do invocations.
	exporter := newTestExporter(t, "http://127.0.0.1:0") // invalid port

	// Use simpleMetrics to force a send.
	err := exporter.ConsumeMetrics(context.Background(), simpleMetrics())

	// We expect no panic and no error returned from ConsumeMetrics.
	require.NoError(t, err)
}

// simpleMetrics there will be added two ResourceMetrics and each of them have count data point
func simpleMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "demo 1")
	rm.Resource().Attributes().PutStr("service.namespace", "demo")
	rm.Resource().Attributes().PutStr("sts_api_key", "APIKEY")
	rm.Resource().Attributes().PutStr("Resource Attributes 1", "value1")
	rm.SetSchemaUrl("Resource SchemaUrl 1")

	rm = metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "demo 2")
	rm.Resource().Attributes().PutStr("service.namespace", "demo")
	rm.Resource().Attributes().PutStr("Resource Attributes 2", "value2")

	rm = metrics.ResourceMetrics().AppendEmpty()
	sc := rm.ScopeMetrics().AppendEmpty()
	sc.Scope().SetName("traces_service_graph")
	ms := sc.Metrics().AppendEmpty()
	ms.SetName("traces_service_graph_request_total")
	ms.SetEmptySum().SetIsMonotonic(true)
	ma := ms.Sum().DataPoints().AppendEmpty().Attributes()
	ma.PutStr("client_sts_api_key", "APIKEY")
	ma.PutStr("client", "client")
	ma.PutStr("client_service.namespace", "clientns")
	ma.PutStr("server", "server")
	ma.PutStr("server_service.namespace", "serverns")
	ma.PutStr("connection_type", "")
	return metrics
}

func virtualNodeMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sc := rm.ScopeMetrics().AppendEmpty()
	sc.Scope().SetName("traces_service_graph")
	ms := sc.Metrics().AppendEmpty()
	ms.SetName("traces_service_graph_request_total")
	ms.SetEmptySum().SetIsMonotonic(true)
	ma := ms.Sum().DataPoints().AppendEmpty().Attributes()
	ma.PutStr("client_sts_api_key", "APIKEY")
	ma.PutStr("client", "client")
	ma.PutStr("client_service.namespace", "clientns")
	ma.PutStr("server", "server")
	ma.PutStr("server_service.namespace", "serverns")
	ma.PutStr("connection_type", "virtual_node")
	return metrics
}

func simpleTrace() ptrace.Traces {
	traces := ptrace.NewTraces()
	rt := traces.ResourceSpans().AppendEmpty()
	rt.Resource().Attributes().PutStr("service.name", "demo 1")
	rt.Resource().Attributes().PutStr("sts_api_key", "APIKEY")
	return traces
}

func newTestExporter(t *testing.T, url string) *ststopologyexporter.TopologyExporter {
	exporter, err := ststopologyexporter.NewTopologyExporter(zaptest.NewLogger(t), &ststopologyexporter.Config{
		TimeoutSettings: exporterhelper.TimeoutConfig{
			Timeout: 15 * time.Millisecond,
		},
		QueueSettings: exporterhelper.NewDefaultQueueConfig(),
		Endpoint:      url,
	})
	require.NoError(t, err)
	return exporter
}
