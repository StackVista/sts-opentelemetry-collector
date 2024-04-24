package ststopologyexporter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap/zaptest"
)

func TestExporter_pushResourcesData(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		require.Equal(t, "APIKEY", req.Header[http.CanonicalHeaderKey("sts-api-key")][0])

		var payload IntakeTopology
		err := json.NewDecoder(req.Body).Decode(&payload)
		require.NoError(t, err)
		require.Equal(t, 1, len(payload.Topologies))
		require.Equal(t, 2, len(payload.Topologies[0].Components))
		res.WriteHeader(200)
	}))
	exporter := newTestExporter(t, testServer.URL)
	err := exporter.ConsumeMetrics(context.TODO(), simpleMetrics())
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
