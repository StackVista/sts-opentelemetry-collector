package stackstate

import (
	"context"
	"fmt"

	"github.com/go-resty/resty/v2"
	ststracepb "github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/proto/sts/trace"
	"go.opentelemetry.io/collector/config/configopaque"
	"google.golang.org/protobuf/proto"
)

type StackStateClient interface {
	// SendTrace sends a trace to StackState
	SendTrace(ctx context.Context, tracePayload *ststracepb.TracePayload) error
}

type stackStateClient struct {
	Endpoint string
	APIKey   configopaque.String
	client   *resty.Client
}

func NewStackStateClient(endpoint string, apiKey configopaque.String) StackStateClient {
	client := resty.New()
	client = client.SetHeader("Content-Type", "application/json").SetHeader("sts-api-key", string(apiKey)).SetBaseURL(endpoint)

	return &stackStateClient{
		Endpoint: endpoint,
		APIKey:   apiKey,
		client:   client,
	}
}

func (c *stackStateClient) SendTrace(ctx context.Context, tracePayload *ststracepb.TracePayload) error {
	bytes, err := proto.Marshal(tracePayload)
	if err != nil {
		return err
	}

	r, err := c.client.R().SetContext(ctx).SetBody(bytes).Post("/api/v0.2/traces")
	if err != nil {
		return err
	}

	if r.IsError() {
		return fmt.Errorf("failed to send traces to StackState: %s", r.String())
	}

	return nil
}

func (c *stackStateClient) String() string {
	return fmt.Sprintf("StackStateClient{endpoint=%s}", c.Endpoint)
}
