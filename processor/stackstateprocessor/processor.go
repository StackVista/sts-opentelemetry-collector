package stackstateprocessor

import (
	"context"
	"fmt"

	"github.com/stackvista/sts-opentelemetry-collector/processor/stackstateprocessor/internal/convert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// var _ consumer.Traces = (*stackstateprocessor)(nil)  // compile-time type check
// var _ processor.Traces = (*stackstateprocessor)(nil) // compile-time type check

const (
	StsServiceName        = "sts.service.name"
	StSServiceURN         = "sts.service.URN"
	StsServiceInstanceURN = "sts.service.instanceURN"
	K8sPodName            = "k8s.pod.name"
	K8sNamespaceName      = "k8s.namespace.name"
)

type stackstateprocessor struct {
	logger      *zap.Logger
	clusterName string
}

func newStackstateprocessor(ctx context.Context, logger *zap.Logger, cfg component.Config) (*stackstateprocessor, error) {
	if c, ok := cfg.(*Config); ok {
		logger.Info("Configured StackState processor", zap.String("cluster_name", c.ClusterName))
		return &stackstateprocessor{
			logger:      logger,
			clusterName: c.ClusterName,
		}, nil
	}

	return nil, fmt.Errorf("invalid config passed to stackstateprocessor: %T", cfg)
}

func (ssp *stackstateprocessor) ProcessTraces(ctx context.Context, traces ptrace.Traces) (ptrace.Traces, error) {
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rs := traces.ResourceSpans().At(i)
		resourceAttrs := convert.ConvertCommonMap(rs.Resource().Attributes(), map[string]string{})
		if _, ok := resourceAttrs[K8sNamespaceName]; !ok {
			ssp.logger.Debug("Skip processing ResourceSpans without k8s.namespace.name")
			continue
		}
		urn := fmt.Sprintf("urn:kubernetes:/%s:%s/pod/%s", ssp.clusterName, resourceAttrs[K8sNamespaceName], resourceAttrs[K8sPodName])
		rs.Resource().Attributes().PutStr("sts.service.URN", urn)
	}

	return traces, nil
}

// func (ssp *stackstateprocessor) Capabilities() consumer.Capabilities {
// 	return consumer.Capabilities{MutatesData: true}
// }

// func (ssp *stackstateprocessor) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
// 	for i := 0; i < traces.ResourceSpans().Len(); i++ {
// 		rs := traces.ResourceSpans().At(i)
// 		resourceAttrs := convert.ConvertCommonMap(rs.Resource().Attributes(), map[string]string{})
// 		if _, ok := resourceAttrs[K8sNamespaceName]; !ok {
// 			ssp.logger.Debug("Skip processing ResourceSpans without k8s.namespace.name")
// 			continue
// 		}
// 		urn := fmt.Sprintf("urn:kubernetes:/%s:%s/pod/%s", ssp.clusterName, resourceAttrs[K8sNamespaceName], resourceAttrs[K8sPodName])
// 		rs.Resource().Attributes().PutStr("sts.service.URN", urn)
// 	}

// 	return ssp.next.ConsumeTraces(ctx, traces)
// }

// func (ssp *stackstateprocessor) Start(context.Context, component.Host) error {
// 	return nil
// }

// func (ssp *stackstateprocessor) Shutdown(context.Context) error {
// 	return nil
// }
