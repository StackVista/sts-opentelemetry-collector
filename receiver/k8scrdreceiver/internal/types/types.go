// Package types holds the shared enums that describe receiver state and outcomes.
// They live here (rather than in the metrics package that records them) so that
// domain code doesn't depend on the observability layer for its own concepts.
package types

// ChangeType is the kind of resource change emitted to the platform.
type ChangeType string

const (
	ChangeAdded    ChangeType = "added"
	ChangeModified ChangeType = "modified"
	ChangeDeleted  ChangeType = "deleted"
	ChangeUnknown  ChangeType = "unknown"
)

// CycleMode distinguishes a snapshot cycle (full re-emit for TTL freshness) from
// an increment cycle (delta only).
type CycleMode string

const (
	ModeSnapshot  CycleMode = "snapshot"
	ModeIncrement CycleMode = "increment"
)

// ResourceKind identifies what's in the cache.
type ResourceKind string

const (
	KindCRD ResourceKind = "crd"
	KindCR  ResourceKind = "cr"
)

// BroadcastOutcome is the result of a peer broadcast (one ApplyDelta call).
type BroadcastOutcome string

const (
	BroadcastSuccess BroadcastOutcome = "success"
	BroadcastFailed  BroadcastOutcome = "failed"
)

// BroadcastFailureReason categorises why a broadcast did not satisfy its ACK threshold.
type BroadcastFailureReason string

const (
	BroadcastFailureNone       BroadcastFailureReason = ""
	BroadcastFailureDNSLookup  BroadcastFailureReason = "dns_lookup"
	BroadcastFailureGzip       BroadcastFailureReason = "gzip"
	BroadcastFailureAckTimeout BroadcastFailureReason = "ack_timeout"
	BroadcastFailureNoAcks     BroadcastFailureReason = "no_acks"
)

// PushOutcome is the result of a single push attempt to one peer.
type PushOutcome string

const (
	PushSuccess PushOutcome = "success"
	PushFailed  PushOutcome = "failed"
)

// PushFailureReason categorises why a single push attempt failed. Empty on success.
type PushFailureReason string

const (
	PushFailureNone          PushFailureReason = ""
	PushFailureTimeout       PushFailureReason = "timeout"
	PushFailureConnection    PushFailureReason = "connection_error"
	PushFailureHTTPStatus    PushFailureReason = "http_error"
	PushFailureRequestFailed PushFailureReason = "request_failed"
)

// BootstrapOutcome categorises how a Bootstrap call ended.
type BootstrapOutcome string

const (
	BootstrapApplied     BootstrapOutcome = "applied"
	BootstrapLeaderEmpty BootstrapOutcome = "leader_empty"
	BootstrapTimedOut    BootstrapOutcome = "timed_out"
)
