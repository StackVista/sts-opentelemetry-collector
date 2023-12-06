package stackstateexporter

import (
	"context"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/internal/stackstate"
	ststracepb "github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/proto/sts/trace"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/zap"
)

var _ exporterhelper.Request = (*StackStateRequest)(nil)

type StackStateRequest struct {
	APITraces []*ststracepb.APITrace
	logger    *zap.Logger
	client    stackstate.StackStateClient
	Hostname  string
	Env       string
}

func EmptyRequest(logger *zap.Logger, client stackstate.StackStateClient, cfg *Config) *StackStateRequest {
	return &StackStateRequest{
		APITraces: []*ststracepb.APITrace{},
		logger:    logger,
		client:    client,
		Hostname:  cfg.Hostname,
		Env:       cfg.Env,
	}
}

func (r *StackStateRequest) AppendTrace(trace []*ststracepb.APITrace) {
	r.APITraces = append(r.APITraces, trace...)
}

func (r *StackStateRequest) Export(ctx context.Context) error {
	payload := &ststracepb.TracePayload{
		HostName: r.Hostname,
		Env:      r.Env,
		Traces:   r.APITraces,
	}

	r.logger.Info("Sending trace to StackState", zap.Any("client", r.client), zap.Int("traces", len(r.APITraces)))
	return r.client.SendTrace(ctx, payload)
}

func (r *StackStateRequest) ItemsCount() int {
	return len(r.APITraces)
}
