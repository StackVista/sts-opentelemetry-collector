package naming

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// receiverCase is one scrape: exposition served by a test server, the metric relabel rules
// applied to it, and the exact set of names that must come out the other side.
//
// This answers whether a dotted name can be carried through a scrape, which decides whether
// the compatibility renames can live in metric_relabel_configs or have to be done with OTTL
// after the receiver. The expectations are exact rather than a subset, so a name that is
// quietly rewritten fails the test instead of passing unnoticed.
type receiverCase struct {
	id         string
	exposition string
	relabels   []map[string]any
	expect     []string
}

func receiverCases() []receiverCase {
	return []receiverCase{
		{
			id:         "underscored_exposition",
			exposition: "kus_cpu_usage_total 1\n",
			expect:     []string{"kus_cpu_usage_total"},
		},
		{
			// A dotted name is not valid exposition syntax under legacy name validation, so
			// it is rejected before any relabel runs.
			id:         "dotted_exposition",
			exposition: "kubernetes.cpu.usage.total 1\n",
			expect:     []string{},
		},
		{
			// A relabel rule can write a dotted name even though exposition cannot, and the
			// receiver passes it through to OTLP unchanged.
			id:         "relabel_to_dotted",
			exposition: "kus_cpu_usage_total 1\n",
			relabels: []map[string]any{{
				"source_labels": []any{"__name__"},
				"regex":         "kus_cpu_usage_total",
				"target_label":  "__name__",
				"replacement":   "kubernetes.cpu.usage.total",
			}},
			expect: []string{"kubernetes.cpu.usage.total"},
		},
		{
			id:         "relabel_to_underscored",
			exposition: "kus_cpu_usage_total 1\n",
			relabels: []map[string]any{{
				"source_labels": []any{"__name__"},
				"regex":         "kus_cpu_usage_total",
				"target_label":  "__name__",
				"replacement":   "kubernetes_cpu_usage_total",
			}},
			expect: []string{"kubernetes_cpu_usage_total"},
		},
	}
}

func TestPrometheusReceiverNaming(t *testing.T) {
	rows := [][]string{{"case", "exposition_name", "relabel_target", "scraped_name"}}

	for _, tc := range receiverCases() {
		t.Run(tc.id, func(t *testing.T) {
			got := scrapeOnce(t, tc)
			require.Equal(t, tc.expect, got,
				"%s: scraped names differ from the expected set", tc.id)

			if len(got) == 0 {
				rows = append(rows, []string{tc.id, expositionName(tc.exposition), relabelReplacement(tc.relabels), ""})
			}
			for _, name := range got {
				rows = append(rows, []string{tc.id, expositionName(tc.exposition), relabelReplacement(tc.relabels), name})
			}
		})
	}

	compareGolden(t, filepath.Join("testdata", "receiver.csv"), rows)
}

// isReceiverGenerated reports whether a metric was produced by the receiver itself rather
// than scraped from the target. Those series are bookkeeping and say nothing about naming
// translation, so they are excluded rather than asserted on.
func isReceiverGenerated(name string) bool {
	switch {
	case name == "up", name == "target_info", name == "scrape_series_added":
		return true
	case strings.HasPrefix(name, "scrape_"):
		return true
	case strings.HasPrefix(name, "otel_scope_"):
		return true
	default:
		return false
	}
}

func scrapeOnce(t *testing.T, tc receiverCase) []string {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprint(w, tc.exposition)
	}))
	defer server.Close()

	factory := prometheusreceiver.NewFactory()
	cfg := factory.CreateDefaultConfig().(*prometheusreceiver.Config)

	scrapeConfig := map[string]any{
		"job_name":        "naming",
		"scrape_interval": "1s",
		"static_configs": []any{
			map[string]any{"targets": []any{strings.TrimPrefix(server.URL, "http://")}},
		},
	}
	if len(tc.relabels) > 0 {
		scrapeConfig["metric_relabel_configs"] = tc.relabels
	}

	raw := map[string]any{
		"config": map[string]any{"scrape_configs": []any{scrapeConfig}},
	}
	require.NoError(t, confmap.NewFromStringMap(raw).Unmarshal(cfg))

	sink := new(consumertest.MetricsSink)
	ctx := t.Context()
	recv, err := factory.CreateMetrics(ctx, receivertest.NewNopSettings(factory.Type()), cfg, sink)
	require.NoError(t, err)

	require.NoError(t, recv.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() {
		require.NoError(t, recv.Shutdown(context.Background()))
	})

	// One scrape is enough to answer the naming question, and waiting for the first delivery
	// keeps the test independent of the scrape interval.
	require.Eventually(t, func() bool {
		return sink.DataPointCount() > 0
	}, 30*time.Second, 250*time.Millisecond, "%s: no metrics were scraped", tc.id)

	seen := map[string]bool{}
	for _, md := range sink.AllMetrics() {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					name := ms.At(k).Name()
					if isReceiverGenerated(name) {
						continue
					}
					seen[name] = true
				}
			}
		}
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func expositionName(exposition string) string {
	fields := strings.Fields(exposition)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func relabelReplacement(relabels []map[string]any) string {
	for _, r := range relabels {
		if v, ok := r["replacement"].(string); ok {
			return v
		}
	}
	return ""
}
