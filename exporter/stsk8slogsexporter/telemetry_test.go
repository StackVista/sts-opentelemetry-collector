package stsk8slogsexporter //nolint:testpackage // Checks validation counts at the real exporter boundary.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestValidationMetricsSurviveRetries(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "acknowledged", true: "rejected"}[reject], func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				call := calls.Add(1)
				body, err := io.ReadAll(req.Body)
				if err != nil {
					t.Error(err)
					return
				}
				decoded := decodeRequest(t, body)
				streams := field(decoded, "streams").List()
				if streams.Len() != 1 || field(streams.Get(0).Message(), "entries").List().Len() != 1 {
					t.Error("invalid sibling reached the wire")
				}
				switch {
				case reject:
					w.WriteHeader(http.StatusForbidden)
				case call == 1:
					w.WriteHeader(http.StatusServiceUnavailable)
				default:
					w.WriteHeader(http.StatusNoContent)
				}
			}))
			defer server.Close()
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			defer func() { _ = provider.Shutdown(context.Background()) }()
			settings := exportertest.NewNopSettings(component.MustNewType("stsk8slogs"))
			settings.MeterProvider = provider
			cfg := factoryConfig(t, server.URL+"/stsAgent/logs/k8s")
			cfg.BackOffConfig.InitialInterval = time.Millisecond
			cfg.BackOffConfig.MaxInterval = time.Millisecond
			exp, err := NewFactory().CreateLogs(context.Background(), settings, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := exp.Start(context.Background(), componenttest.NewNopHost()); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = exp.Shutdown(context.Background()) }()
			logs := testLogs()
			records := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
			firstRecord(logs).CopyTo(records.AppendEmpty())
			records.At(1).Body().SetInt(42)
			err = exp.ConsumeLogs(context.Background(), logs)
			if (err != nil) != reject {
				t.Fatalf("unexpected export result: %v", err)
			}
			if logs.LogRecordCount() != 2 || records.At(1).Body().Int() != 42 || exp.Capabilities().MutatesData {
				t.Fatal("exporter mutated its caller's records")
			}
			wantCalls := int32(2)
			wantMetric := "otelcol_exporter_sent_log_records"
			if reject {
				wantCalls = 1
				wantMetric = "otelcol_exporter_send_failed_log_records"
			}
			if calls.Load() != wantCalls {
				t.Fatalf("HTTP attempts = %d, want %d", calls.Load(), wantCalls)
			}
			var data metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &data); err != nil {
				t.Fatal(err)
			}
			if got := metricSum(t, data, "otelcol_stsk8slogs_invalid_log_records", "body"); got != 1 {
				t.Fatalf("invalid records counted %d times", got)
			}
			if got := metricSum(t, data, wantMetric, ""); got != 1 {
				t.Fatalf("helper counted %d records, want the one valid record", got)
			}
		})
	}
}

func metricSum(t *testing.T, data metricdata.ResourceMetrics, name, reason string) int64 {
	t.Helper()
	var total int64
	found := false
	for _, scope := range data.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			sum, ok := metric.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("unexpected aggregation for %s", name)
			}
			for _, point := range sum.DataPoints {
				if reason != "" {
					value, exists := point.Attributes.Value(attribute.Key("reason"))
					if !exists || value.AsString() != reason {
						t.Fatalf("unexpected reason attributes: %v", point.Attributes)
					}
				}
				total += point.Value
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("metric %s was not recorded", name)
	}
	return total
}
