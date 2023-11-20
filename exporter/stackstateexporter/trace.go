// Copyright StackState B.V.
// SPDX-License-Identifier: Apache-2.0
package stackstateexporter

import (
	"context"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/internal/convert"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

var _ exporterhelper.TracesConverter = (*traceExporter)(nil)

type traceExporter struct {
	params exporter.CreateSettings
	logger *zap.Logger
}

func newTraceExporter(logger *zap.Logger) *traceExporter {
	return &traceExporter{logger: logger}
}

func (t *traceExporter) RequestFromTraces(ctx context.Context, td ptrace.Traces) (exporterhelper.Request, error) {
	logger := t.logger

	req := EmptyRequest(logger)
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		apiTrace, err := convert.ConvertTrace(ctx, rs, logger)
		if err != nil {
			logger.Warn("Failed to convert ResourceSpans to APITrace", zap.Error(err))
			return nil, err
		}
		req.APITraces = append(req.APITraces, apiTrace...)
	}

	return req, nil
}
