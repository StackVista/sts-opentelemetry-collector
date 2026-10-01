//nolint:goconst // Keep graph mutations beside their expected validation results.
package logsagent_test

import (
	"strings"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

func TestDeliveryGraphSelection(t *testing.T) {
	for _, effective := range []bool{false, true} {
		validate := logsagent.ValidatePipelineConfig
		if effective {
			validate = logsagent.ValidateEffectivePipelineConfig
		}
		for _, discovery := range []bool{false, true} {
			t.Run(map[bool]string{false: "authored", true: "effective"}[effective]+"/direct/"+
				map[bool]string{false: "fixed", true: "discovery"}[discovery], func(t *testing.T) {
				values := fixtureMap(t, directPipelineFixture)
				fixtureSet(t, values, "extensions::stslogsagent/logs::discovery_enabled", discovery)
				_, err := validate(confmap.NewFromStringMap(values))
				if discovery {
					require.ErrorContains(t, err, "direct logs delivery")
				} else {
					require.NoError(t, err)
				}
			})
			for _, exporter := range []string{"stsk8slogs/promtail", "otlp_http/otel_native"} {
				if !discovery && exporter != "stsk8slogs/promtail" {
					continue
				}
				for _, guard := range []struct {
					name  string
					value any
				}{
					{"null", nil},
					{"empty", map[string]any{}},
					{"configured", map[string]any{"controller_extension": "stslogsagent/logs"}},
					{"malformed", false},
				} {
					t.Run(map[bool]string{false: "authored", true: "effective"}[effective]+"/routed/"+
						map[bool]string{false: "fixed", true: "discovery"}[discovery]+"/"+exporter+"/"+guard.name, func(t *testing.T) {
						values := fixtureMap(t, strings.NewReplacer(
							"filelog/", "file_log/", "k8sattributes", "k8s_attributes",
						).Replace(pipelineFixture))
						fixtureSet(t, values, "extensions::stslogsagent/logs::discovery_enabled", discovery)
						if !discovery {
							fixtureDelete(t, values, "connectors::stslogsroute/logs::native_pipeline")
							fixtureDelete(t, values, "service::pipelines::logs/otel_native")
							fixtureDelete(t, values, "exporters::otlp_http/otel_native")
						}
						fixtureSet(t, values, "exporters::"+exporter+"::delivery", guard.value)
						_, err := validate(confmap.NewFromStringMap(values))
						if guard.value != nil {
							require.ErrorContains(t, err, "routed terminal exporters must not configure delivery")
						} else {
							require.NoError(t, err)
						}
					})
				}
			}
		}
	}
}
