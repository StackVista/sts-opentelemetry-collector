// Package sender defines the subset of the Datadog agent sender that the KSM
// mapping layer calls.
package sender

import "github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/ddcompat/servicecheck"

// Sender receives the series produced by one mapping run. Implementations
// must not retain tags after returning; callers may reuse the slice.
type Sender interface {
	Gauge(metric string, value float64, hostname string, tags []string)
	ServiceCheck(checkName string, status servicecheck.ServiceCheckStatus, hostname string, tags []string, message string)
}
