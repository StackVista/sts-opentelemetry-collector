package stackstateprocessor

import (
	"context"
	"fmt"

	"github.com/stackvista/sts-opentelemetry-collector/common/identifier"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
)

const (
	ClusterName = "io.stackstate.clustername"
)

var _ processorhelper.ProcessTracesFunc = (*stackstateprocessor)(nil).ProcessTraces

var _ processorhelper.ProcessLogsFunc = (*stackstateprocessor)(nil).ProcessLogs
var _ processorhelper.ProcessMetricsFunc = (*stackstateprocessor)(nil).ProcessMetrics

// var _ consumer.Traces = (*stackstateprocessor)(nil) // compile-time type check
// var _ processor.Traces = (*stackstateprocessor)(nil) // compile-time type check

type stackstateprocessor struct {
	logger      *zap.Logger
	identifier  identifier.Identifier
	clusterName string
}

func newStackstateprocessor(ctx context.Context, logger *zap.Logger, cfg component.Config) (*stackstateprocessor, error) {
	if _, ok := cfg.(*Config); !ok {
		return nil, fmt.Errorf("invalid config passed to stackstateprocessor: %T", cfg)
	}
	c := cfg.(*Config)

	logger.Info("Configuring StackState processor", zap.String("cluster_name", c.ClusterName))

	id, err := identifier.NewIdentifier(logger, c.ClusterName)
	if err != nil {
		return nil, err
	}

	return &stackstateprocessor{
		logger:      logger,
		identifier:  id,
		clusterName: c.ClusterName,
	}, nil
}

func (ssp *stackstateprocessor) ProcessMetrics(ctx context.Context, metrics pmetric.Metrics) (pmetric.Metrics, error) {
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		rm := metrics.ResourceMetrics().At(i)
		ssp.addStackStateAttributes(ctx, rm.Resource())
	}

	return metrics, nil
}

func (ssp *stackstateprocessor) ProcessTraces(ctx context.Context, traces ptrace.Traces) (ptrace.Traces, error) {
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rs := traces.ResourceSpans().At(i)
		ssp.addStackStateAttributes(ctx, rs.Resource())
	}

	return traces, nil
}

func (ssp *stackstateprocessor) ProcessLogs(ctx context.Context, logs plog.Logs) (plog.Logs, error) {
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rl := logs.ResourceLogs().At(i)
		ssp.addStackStateAttributes(ctx, rl.Resource())
	}

	return logs, nil
}

func (ssp *stackstateprocessor) addStackStateAttributes(_ context.Context, res pcommon.Resource) {
	attrMap := res.Attributes()
	attrMap.PutStr(ClusterName, ssp.clusterName)
}
