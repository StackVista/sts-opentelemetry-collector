package stackstateexporter

import (
	"context"
	"encoding/json"

	ststracepb "github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/proto/sts/trace"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/zap"
)

var _ exporterhelper.Request = (*StackStateRequest)(nil)

type StackStateRequest struct {
	APITraces []*ststracepb.APITrace
	logger    *zap.Logger
}

func EmptyRequest(logger *zap.Logger) *StackStateRequest {
	return &StackStateRequest{
		APITraces: []*ststracepb.APITrace{},
		logger:    logger,
	}
}

func (r *StackStateRequest) Export(ctx context.Context) error {
	for _, apiTrace := range r.APITraces {
		js, err := json.Marshal(apiTrace)
		if err != nil {
			r.logger.Error("Failed to marshal APITrace", zap.Error(err))
			return err
		}

		r.logger.Info("Sending trace to StackState", zap.String("trace", string(js)))
	}
	return nil
}

func (r *StackStateRequest) ItemsCount() int {
	return len(r.APITraces)
}
