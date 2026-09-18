// Package http exposes the OTLP HTTP factory with corrected config serialization.
package http

import (
	"github.com/stackvista/sts-opentelemetry-collector/exporter/otlpconfigexporter"
	"go.opentelemetry.io/collector/exporter"
)

func NewFactory() exporter.Factory { return otlpconfigexporter.NewHTTPFactory() }
