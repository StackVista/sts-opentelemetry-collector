// Copyright StackState B.V.
// SPDX-License-Identifier: Apache-2.0
package stackstateexporter

import (
	"context"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/internal/convert"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/internal/logger"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

var _ exporterhelper.TracesConverter = (*traceExporter)(nil)

type traceExporter struct {
	params exporter.CreateSettings
}

func newTraceExporter() *traceExporter {
	return &traceExporter{}
}

func (t *traceExporter) RequestFromTraces(ctx context.Context, td ptrace.Traces) (exporterhelper.Request, error) {
	ctx = logger.ZapToCtx(ctx, t.params.Logger)

	req := EmptyRequest()
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		apiTrace, err := convert.ConvertTrace(ctx, rs)
		if err != nil {
			t.params.Logger.Warn("Failed to convert ResourceSpans to APITrace", zap.Error(err))
			return nil, err
		}
		req.APITraces = append(req.APITraces, apiTrace...)
	}

	return req, nil
}
