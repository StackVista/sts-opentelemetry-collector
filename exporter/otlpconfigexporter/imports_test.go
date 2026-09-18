package otlpconfigexporter_test

import (
	"testing"

	grpcfactory "github.com/stackvista/sts-opentelemetry-collector/exporter/otlpconfigexporter/grpc"
	httpfactory "github.com/stackvista/sts-opentelemetry-collector/exporter/otlpconfigexporter/http"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/xexporter"
)

func TestBuilderImportFactories(t *testing.T) {
	for _, tc := range []struct {
		factory exporter.Factory
		kind    string
		alias   string
	}{
		{httpfactory.NewFactory(), "otlp_http", "otlphttp"},
		{grpcfactory.NewFactory(), "otlp_grpc", "otlp"},
	} {
		if tc.factory.Type().String() != tc.kind {
			t.Fatal("builder import changed the upstream type")
		}
		if _, ok := tc.factory.(xexporter.Factory); !ok {
			t.Fatal("builder import lost profiles support")
		}
		alias, ok := tc.factory.(interface {
			DeprecatedAlias() component.Type
			SetDeprecatedAlias(component.Type)
		})
		if !ok || alias.DeprecatedAlias().String() != tc.alias {
			t.Fatal("builder import lost the deprecated alias interface")
		}
	}
}
