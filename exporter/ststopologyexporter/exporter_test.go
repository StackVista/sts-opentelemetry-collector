package ststopologyexporter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/ststopologyexporter/internal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap/zaptest"
)

func TestExporter_pushResourcesData(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		require.Equal(t, "APIKEY", req.Header[http.CanonicalHeaderKey("sts-api-key")][0])

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
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		require.Fail(t, "No request should be sent")
	}))
	exporter := newTestExporter(t, testServer.URL)
	err := exporter.ConsumeMetrics(context.TODO(), virtualNodeMetrics())
	require.NoError(t, err)
}

func TestExporter_createBroker(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		var payload internal.IntakeTopology
		err := json.NewDecoder(req.Body).Decode(&payload)
		require.NoError(t, err)
		require.Equal(t, 1, len(payload.Topologies))
		require.Equal(t, 3, len(payload.Topologies[0].Components))
		require.Equal(t, "broker", payload.Topologies[0].Components[1].Type.Name)
		require.Equal(t, "broker-instance", payload.Topologies[0].Components[2].Type.Name)
		res.WriteHeader(200)
	}))
	exporter := newTestExporter(t, testServer.URL)
	err := exporter.ConsumeMetrics(context.TODO(), brokerMetrics())
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

// simpleMetrics there will be added two ResourceMetrics and each of them have count data point
func brokerMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "broker 1")
	rm.Resource().Attributes().PutStr("service.namespace", "demo")
	rm.Resource().Attributes().PutStr("sts_api_key", "APIKEY")
	rm.Resource().Attributes().PutStr("Resource Attributes 1", "value1")
	rm.SetSchemaUrl("Resource SchemaUrl 1")

	sc := rm.ScopeMetrics().AppendEmpty()
	sc.Scope().SetName("something")
	ms := sc.Metrics().AppendEmpty()
	ms.SetName("kafka_server_metric")
	ms.SetEmptySum().SetIsMonotonic(true)
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

func newTestExporter(t *testing.T, url string) *topologyExporter {
	exporter, err := newTopologyExporter(zaptest.NewLogger(t), &Config{
		TimeoutSettings: exporterhelper.TimeoutSettings{
			Timeout: 15 * time.Millisecond,
		},
		QueueSettings: exporterhelper.NewDefaultQueueSettings(),
		Endpoint:      url,
	})
	require.NoError(t, err)
	return exporter
}
