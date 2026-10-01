package stslogsagentextension

import (
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/openapiclient/features"
)

type discoveryOptions struct {
	query              features.QueryOptions
	poll               features.PollOptions
	stableObservations int
	restartCooldown    time.Duration
}

func defaultDiscoveryOptions() discoveryOptions {
	return discoveryOptions{
		query: features.QueryOptions{
			Timeout: 20 * time.Second, AttemptTimeout: 5 * time.Second, MaxAttempts: 3,
			InitialBackoff: 500 * time.Millisecond, MaxBackoff: 2 * time.Second,
			BooleanCapabilities: []string{"otel-logs"},
		},
		poll:               features.PollOptions{Interval: time.Minute, Jitter: 0.2},
		stableObservations: 3,
		restartCooldown:    10 * time.Minute,
	}
}
