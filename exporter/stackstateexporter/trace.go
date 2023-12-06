// Copyright StackState B.V.
// SPDX-License-Identifier: Apache-2.0
package stackstateexporter

import (
	"context"
	"fmt"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/internal/convert"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/internal/stackstate"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type traceExporter struct {
	logger *zap.Logger
	client stackstate.StackStateClient
	cfg    *Config
}

func newTraceExporter(ctx context.Context, logger *zap.Logger, cfg component.Config) (*traceExporter, error) {
	if _, ok := cfg.(*Config); !ok {
		return nil, fmt.Errorf("invalid config passed to stackstateexporter: %T", cfg)
	}
	c := cfg.(*Config)

	logger.Info("Configuring StackState exporter", zap.String("endpoint", c.API.Endpoint))

	client := stackstate.NewStackStateClient(c.API.Endpoint, c.API.APIKey)

	return &traceExporter{logger: logger, client: client}, nil
}

func (t *traceExporter) RequestFromTraces(ctx context.Context, td ptrace.Traces) (exporterhelper.Request, error) {
	logger := t.logger

	req := EmptyRequest(logger, t.client, t.cfg)
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		apiTrace, err := convert.ConvertTrace(ctx, rs, logger)
		if err != nil {
			logger.Warn("Failed to convert ResourceSpans to APITrace", zap.Error(err))
			return nil, err
		}
		req.AppendTrace(apiTrace)
	}

	return req, nil
}
