package stackstateexporter

import (
	"context"
	"encoding/json"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/internal/logger"
	ststracepb "github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/proto/sts/trace"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/zap"
)

var _ exporterhelper.Request = (*StackStateRequest)(nil)

type StackStateRequest struct {
	APITraces []*ststracepb.APITrace
}

func EmptyRequest() *StackStateRequest {
	return &StackStateRequest{
		APITraces: []*ststracepb.APITrace{},
	}
}

func (r *StackStateRequest) Export(ctx context.Context) error {
	logger := logger.ZapLogger(ctx, "stackstate-request")

	for _, apiTrace := range r.APITraces {
		js, err := json.Marshal(apiTrace)
		if err != nil {
			logger.Error("Failed to marshal APITrace", zap.Error(err))
			return err
		}

		logger.Info("Sending trace to StackState", zap.String("trace", string(js)))
	}
	return nil
}

func (r *StackStateRequest) ItemsCount() int {
	return len(r.APITraces)
}
