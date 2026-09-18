package stslogsrouteconnector

import (
	"context"
	"errors"
	"strings"

	"go.opentelemetry.io/collector/consumer/consumererror"
)

const (
	outcomeAcknowledged = "acknowledged"
	outcomeDeadline     = "deadline_expired"
	outcomeRetry        = "retry_exhausted"
	outcomePermanent    = "permanent_rejection"
	outcomeTerminal     = "terminal_export_error"
	outcomeCanceled     = "canceled"
)

func classifyOutcome(ctx context.Context, err error) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return outcomeDeadline
	}
	if err == nil {
		return outcomeAcknowledged
	}
	if consumererror.IsPermanent(err) {
		return outcomePermanent
	}
	// exporterhelper v0.153.0 exposes these terminal reasons only as error prefixes.
	switch {
	case strings.HasPrefix(err.Error(), "request will be cancelled before next retry: "):
		return outcomeDeadline
	case strings.HasPrefix(err.Error(), "no more retries left: "):
		return outcomeRetry
	case errors.Is(ctx.Err(), context.Canceled):
		return outcomeCanceled
	default:
		return outcomeTerminal
	}
}
