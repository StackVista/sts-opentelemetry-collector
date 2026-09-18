// Package grpc exposes the OTLP gRPC factory with corrected config serialization.
package grpc

import (
	"github.com/stackvista/sts-opentelemetry-collector/exporter/otlpconfigexporter"
	"go.opentelemetry.io/collector/exporter"
)

func NewFactory() exporter.Factory { return otlpconfigexporter.NewGRPCFactory() }
