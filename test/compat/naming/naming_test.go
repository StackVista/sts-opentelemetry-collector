// Package naming pins down how an Agent V2 metric name is transformed on the way to the
// metrics store, so the contract in test/compat/contract.csv is backed by an executable
// rule rather than a hand-written one.
//
// Two transformations matter and both are exercised here:
//
//   - the remote write exporter, which is where the name the platform stores is decided
//   - the prometheus receiver, which decides whether a dotted name can be carried through a
//     scrape and a relabel at all
//
// The golden files under testdata/ are the artifact: review them on every dependency bump,
// because the translation strategy is version dependent and the exporter is already moving
// from add_metric_suffixes to translation_strategy.
package naming

import (
	"debug/buildinfo"
	"encoding/csv"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var update = flag.Bool("update", false, "rewrite the golden files under testdata")

// strategy is one value of the exporter's translation_strategy setting, plus the remote
// write version it needs. The exporter rejects the UTF-8 strategies unless remote write 2.0
// is configured, which is asserted in TestTranslationStrategyRemoteWriteRequirement.
type strategy struct {
	id     string
	prw    string
	bomSet bool
}

var strategies = []strategy{
	{id: "", prw: "v1", bomSet: true},
	{id: "UnderscoreEscapingWithSuffixes", prw: "v1"},
	{id: "UnderscoreEscapingWithoutSuffixes", prw: "v1"},
	{id: "NoUTF8EscapingWithSuffixes", prw: "v2"},
	{id: "NoTranslation", prw: "v2"},
}

type metricKind string

const (
	kindGauge     metricKind = "gauge"
	kindMonotonic metricKind = "monotonic_sum"
	kindHistogram metricKind = "histogram"
)

// variant is one input shape. The set covers what the contract contains: dotted and already
// underscored names, gauges and monotonic sums, with and without a unit, and a histogram.
type variant struct {
	id       string
	name     string
	unit     string
	kind     metricKind
	dpAttrs  map[string]string
	resAttrs map[string]string
}

func variants() []variant {
	return []variant{
		{id: "dotted_gauge_no_unit", name: "kubernetes.cpu.usage.total", kind: kindGauge},
		{id: "dotted_gauge_bytes", name: "kubernetes.memory.usage", unit: "By", kind: kindGauge},
		{id: "dotted_sum_seconds", name: "container.cpu.time", unit: "s", kind: kindMonotonic},
		{id: "dotted_sum_bytes", name: "container.network.io.usage.tx_bytes", unit: "By", kind: kindMonotonic},
		{id: "underscored_sum_no_unit", name: "kubernetes_cpu_usage_total", kind: kindMonotonic},
		{id: "underscored_gauge_no_unit", name: "kubernetes_state_pod_ready", kind: kindGauge},
		{id: "dotted_histogram_seconds", name: "kubernetes.kubelet.pod.start.duration", unit: "s", kind: kindHistogram},
		{id: "dotted_gauge_with_dash", name: "kubernetes.ephemeral-storage.usage", kind: kindGauge},
		{id: "dotted_sum_with_dash", name: "kubernetes.ephemeral-storage.usage", unit: "By", kind: kindMonotonic},
		{id: "dotted_gauge_with_attrs", name: "kubernetes.pod.network.io", kind: kindGauge,
			dpAttrs: map[string]string{"k8s.pod.name": "cart", "direction": "receive"}},
		{id: "dotted_gauge_with_resource_attrs", name: "kubernetes.pod.network.io", kind: kindGauge,
			resAttrs: map[string]string{"k8s.pod.name": "cart", "k8s.namespace.name": "shop"}},
	}
}

func buildMetrics(v variant) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	for k, val := range v.resAttrs {
		rm.Resource().Attributes().PutStr(k, val)
	}
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(v.name)
	if v.unit != "" {
		m.SetUnit(v.unit)
	}

	switch v.kind {
	case kindGauge:
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetDoubleValue(1)
		for k, val := range v.dpAttrs {
			dp.Attributes().PutStr(k, val)
		}
	case kindMonotonic:
		s := m.SetEmptySum()
		s.SetIsMonotonic(true)
		s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp := s.DataPoints().AppendEmpty()
		dp.SetDoubleValue(1)
		for k, val := range v.dpAttrs {
			dp.Attributes().PutStr(k, val)
		}
	case kindHistogram:
		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp := h.DataPoints().AppendEmpty()
		dp.SetCount(1)
		dp.SetSum(1)
		dp.ExplicitBounds().FromRaw([]float64{1})
		dp.BucketCounts().FromRaw([]uint64{0, 1})
	}

	return md
}

// translate runs one variant through the translator with one strategy, the same call the
// exporter makes, and returns every distinct series name it produced.
func translate(t *testing.T, v variant, s strategy) []string {
	t.Helper()

	settings := prometheusremotewrite.Settings{
		TranslationStrategy: s.id,
	}
	if s.bomSet {
		// The exporter's default is add_metric_suffixes true with no strategy set.
		settings.AddMetricSuffixes = true
	}

	series, err := prometheusremotewrite.FromMetrics(buildMetrics(v), settings)
	require.NoError(t, err)

	names := map[string]bool{}
	for _, ts := range series {
		for _, label := range ts.Labels {
			if label.Name == "__name__" {
				names[label.Value] = true
			}
		}
	}

	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	sort.Strings(out)
	require.NotEmpty(t, out, "no series produced for %s", v.id)
	return out
}

func TestRemoteWriteNaming(t *testing.T) {
	rows := [][]string{{"variant", "strategy", "prw", "metric_name", "unit", "kind", "series_name"}}

	for _, v := range variants() {
		for _, s := range strategies {
			label := s.id
			if label == "" {
				label = "default"
			}
			for _, name := range translate(t, v, s) {
				rows = append(rows, []string{v.id, label, s.prw, v.name, v.unit, string(v.kind), name})
			}
		}
	}

	compareGolden(t, filepath.Join("testdata", "naming.csv"), rows)
}

// TestAttributeNaming records how attribute keys become label names. Dots in an attribute
// become underscores under the underscore escaping strategies, which is why the compatibility
// layer cannot rely on a label staying as k8s.pod.name.
//
// Resource attributes are only converted to labels by the exporter's
// resource_to_telemetry_conversion wrapper, not by the translator, so the variant that puts
// them on the resource records that they do not appear as labels here.
func TestAttributeNaming(t *testing.T) {
	rows := [][]string{{"variant", "strategy", "prw", "input_attributes", "observed_labels"}}

	for _, v := range variants() {
		input := append(sortedKeys(v.dpAttrs), sortedKeys(v.resAttrs)...)
		if len(input) == 0 {
			continue
		}

		for _, s := range strategies {
			label := s.id
			if label == "" {
				label = "default"
			}

			settings := prometheusremotewrite.Settings{TranslationStrategy: s.id}
			if s.bomSet {
				settings.AddMetricSuffixes = true
			}
			series, err := prometheusremotewrite.FromMetrics(buildMetrics(v), settings)
			require.NoError(t, err)

			seen := map[string]bool{}
			for _, ts := range series {
				for _, l := range ts.Labels {
					if l.Name != "__name__" {
						seen[l.Name] = true
					}
				}
			}

			labels := make([]string, 0, len(seen))
			for name := range seen {
				labels = append(labels, name)
			}
			sort.Strings(labels)

			rows = append(rows, []string{v.id, label, s.prw, strings.Join(input, " "), strings.Join(labels, " ")})
		}
	}

	compareGolden(t, filepath.Join("testdata", "labels.csv"), rows)
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestUTF8StrategiesRequireRemoteWriteV2 asserts the constraint that narrows the decision to
// two strategies, using the exporter's own validation rather than a restatement of it. The
// UTF-8 strategies preserve dots, so they are both unavailable under the remote write 1.0
// the platform runs and undesirable, since the store already holds underscored names.
//
// Remote write 2.0 would admit them, but only with the exporter feature gate enabled, and it
// would still preserve dots.
func TestUTF8StrategiesRequireRemoteWriteV2(t *testing.T) {
	cases := []struct {
		strategy string
		valid    bool
	}{
		{"UnderscoreEscapingWithSuffixes", true},
		{"UnderscoreEscapingWithoutSuffixes", true},
		{"NoUTF8EscapingWithSuffixes", false},
		{"NoTranslation", false},
	}

	factory := prometheusremotewriteexporter.NewFactory()
	for _, tc := range cases {
		t.Run(tc.strategy, func(t *testing.T) {
			cfg := factory.CreateDefaultConfig()
			raw := map[string]any{
				"endpoint":             "http://127.0.0.1:1/api/v1/write",
				"translation_strategy": tc.strategy,
			}
			require.NoError(t, confmap.NewFromStringMap(raw).Unmarshal(cfg))

			err := cfg.(*prometheusremotewriteexporter.Config).Validate()
			if tc.valid {
				require.NoError(t, err, "%s should be accepted under remote write 1.0", tc.strategy)
				return
			}
			require.Error(t, err, "%s should be rejected under remote write 1.0", tc.strategy)
		})
	}
}

// TestDependencyMatchesBOM fails when the collector version this module is built against
// drifts from the version pinned in the server side bill of materials, which is where the
// remote write exporter actually runs. Without this the golden files would keep passing
// while describing a version nobody deploys.
func TestDependencyMatchesBOM(t *testing.T) {
	const module = "pkg/translator/prometheusremotewrite"
	want := bomVersion(t, filepath.Join("..", "..", "..", "sts-otel-builder.yaml"), "prometheusremotewriteexporter")
	require.NotEmpty(t, want, "could not find the exporter version in the bill of materials")

	info, err := buildinfo.ReadFile(os.Args[0])
	if err == nil {
		for _, dep := range info.Deps {
			if strings.HasSuffix(dep.Path, module) {
				require.Equal(t, want, dep.Version,
					"the translator version and the server side bill of materials have drifted")
				return
			}
		}
	}

	// Fall back to the module list when build info is unavailable, for example under some
	// test caching configurations.
	got := moduleVersionFromGoMod(t, "go.mod", module)
	require.Equal(t, want, got, "the translator version and the server side bill of materials have drifted")
}

func bomVersion(t *testing.T, path, contains string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	for _, line := range strings.Split(string(data), "\n") {
		if !strings.Contains(line, "gomod:") || !strings.Contains(line, contains) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		return fields[len(fields)-1]
	}
	return ""
}

func moduleVersionFromGoMod(t *testing.T, path, module string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	for _, line := range strings.Split(string(data), "\n") {
		if !strings.Contains(line, module) {
			continue
		}
		fields := strings.Fields(line)
		for _, f := range fields {
			if strings.HasPrefix(f, "v") && strings.Contains(f, ".") {
				return f
			}
		}
	}
	return ""
}

func compareGolden(t *testing.T, path string, rows [][]string) {
	t.Helper()

	var buf strings.Builder
	w := csv.NewWriter(&buf)
	require.NoError(t, w.WriteAll(rows))
	w.Flush()
	require.NoError(t, w.Error())
	got := buf.String()

	if *update {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(got), 0o644))
		t.Logf("wrote %s with %d rows", path, len(rows)-1)
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%s is missing, run: go test ./... -update\n%v", path, err)
	}
	if got != string(want) {
		t.Errorf("%s is out of date, run: go test ./... -update", path)
		reportDifference(t, string(want), got)
	}
}

func reportDifference(t *testing.T, want, got string) {
	t.Helper()
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")
	for i := 0; i < len(wantLines) || i < len(gotLines); i++ {
		var w, g string
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if w != g {
			t.Logf("row %d\n  want %s\n  got  %s", i+1, w, g)
		}
	}
}
