package metrics

// This file holds the enums used as metric attribute values. They live in the
// metrics package because they describe values that are recorded as labels —
// callers that don't record metrics generally don't need them.

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

// BootstrapSource reports which kind of peer the snapshot was pulled from.
// Operationally important: a high rate of "secondary" bootstraps indicates
// frequent leader churn (e.g., lease flapping or cold replacements winning the
// election race), while "leader" is the steady-state expected source.
type BootstrapSource string

const (
	BootstrapSourceNone      BootstrapSource = "none" // outcome was not "applied"
	BootstrapSourceLeader    BootstrapSource = "leader"
	BootstrapSourceSecondary BootstrapSource = "secondary"
)

// CRInformerOutcome categorises the result of a startCRInformer call. Used by the
// reconciler metric to surface whether periodic recovery is doing useful work.
type CRInformerOutcome string

const (
	CRInformerStarted   CRInformerOutcome = "started"
	CRInformerExists    CRInformerOutcome = "exists"
	CRInformerForbidden CRInformerOutcome = "forbidden"
	CRInformerFailed    CRInformerOutcome = "failed"
)
