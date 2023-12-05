package stackstateprocessor

import (
	"context"
	"fmt"

	"github.com/stackvista/sts-opentelemetry-collector/common/convert"
	"github.com/stackvista/sts-opentelemetry-collector/processor/stackstateprocessor/internal/identifier"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// var _ consumer.Traces = (*stackstateprocessor)(nil)  // compile-time type check
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

func (ssp *stackstateprocessor) ProcessTraces(ctx context.Context, traces ptrace.Traces) (ptrace.Traces, error) {
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rs := traces.ResourceSpans().At(i)
		resourceAttrs := convert.ConvertCommonMap(rs.Resource().Attributes(), map[string]string{})

		// Sanity check the resource attributes to see whether the k8sattributesprocessor has been run
		if _, ok := resourceAttrs[identifier.K8sNamespaceName]; !ok {
			ssp.logger.Debug("Skip processing ResourceSpans without k8s.namespace.name")
			continue
		}

		// Find the identifiers for the resource
		identifiers := ssp.identifier.Identify(ctx, resourceAttrs)
		for k, v := range identifiers {
			rs.Resource().Attributes().PutStr(k, v)
		}
	}

	return traces, nil
}
