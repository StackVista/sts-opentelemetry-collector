// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package stsusageprocessor // import "github.com/stackvista/sts-opentelemetry-collector/processor/stsusageprocessor"

import (
	"context"
	"go.opentelemetry.io/otel/metric"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type stsUsageProcessor struct {
	logger      *zap.Logger
	sizer       ptrace.Sizer
	tracesBytes metric.Int64Counter
}

func (rp *stsUsageProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	// Report usage
	bytes := int64(rp.sizer.TracesSize(td))
	rp.tracesBytes.Add(ctx, bytes)

	return td, nil
}

func (rp *stsUsageProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	// For now noop
	return md, nil
}

func (rp *stsUsageProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	// For now noop
	return ld, nil
}
