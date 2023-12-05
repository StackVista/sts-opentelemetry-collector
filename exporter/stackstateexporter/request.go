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
}

func EmptyRequest(logger *zap.Logger, client stackstate.StackStateClient) *StackStateRequest {
	return &StackStateRequest{
		APITraces: []*ststracepb.APITrace{},
		logger:    logger,
		client:    client,
	}
}

func (r *StackStateRequest) AppendTrace(trace []*ststracepb.APITrace) {
	r.APITraces = append(r.APITraces, trace...)
}

func (r *StackStateRequest) Export(ctx context.Context) error {
	// for _, apiTrace := range r.APITraces {
	// 	js, err := json.Marshal(apiTrace)
	// 	if err != nil {
	// 		r.logger.Error("Failed to marshal APITrace", zap.Error(err))
	// 		return err
	// 	}

	// }
	r.logger.Info("Sending trace to StackState", zap.Any("client", r.client), zap.Int("traces", len(r.APITraces)))
	return r.client.SendTrace(ctx, r.APITraces)
}

func (r *StackStateRequest) ItemsCount() int {
	return len(r.APITraces)
}
