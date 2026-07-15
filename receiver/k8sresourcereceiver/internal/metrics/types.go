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
	KindCRD    ResourceKind = "crd"
	KindObject ResourceKind = "object"
)

// PayloadSource identifies which payload budget a Kubernetes object belongs to.
type PayloadSource string

const (
	PayloadSourceCR     PayloadSource = "cr"
	PayloadSourceObject PayloadSource = "object"
)

// PayloadOutcome identifies whether a payload was forwarded or dropped by budget filtering.
type PayloadOutcome string

const (
	PayloadForwarded PayloadOutcome = "forwarded"
	PayloadDropped   PayloadOutcome = "dropped"
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

// InformerKind distinguishes the two informer flavours that share the reconcile
// metric: CRD-discovered CR informers and statically-configured object informers.
type InformerKind string

const (
	InformerKindCR     InformerKind = "cr"
	InformerKindStatic InformerKind = "static"
)

// InformerOutcome categorises the result of a (re-)attempt to start an informer.
// Used by the reconciler metric to surface whether periodic recovery is doing
// useful work.
type InformerOutcome string

const (
	InformerStarted   InformerOutcome = "started"
	InformerExists    InformerOutcome = "exists"
	InformerForbidden InformerOutcome = "forbidden"
	InformerFailed    InformerOutcome = "failed"
)

// EnrichmentSyncOutcome is the result of waiting for a resource attribute
// enrichment informer's cache to populate after startup.
type EnrichmentSyncOutcome string

const (
	EnrichmentSyncSynced   EnrichmentSyncOutcome = "synced"
	EnrichmentSyncTimedOut EnrichmentSyncOutcome = "timed_out"
)

// EnrichmentValueEvent describes a lifecycle transition for one enrichment key's value.
type EnrichmentValueEvent string

const (
	// EnrichmentValueSet is recorded when a new or changed value is resolved.
	EnrichmentValueSet EnrichmentValueEvent = "set"
	// EnrichmentValueCleared is recorded when the value is removed — the source
	// object was deleted, or the named env var is absent from the container spec.
	EnrichmentValueCleared EnrichmentValueEvent = "cleared"
	// EnrichmentValueUnsupported is recorded when the env entry uses valueFrom
	// (a dynamic reference) rather than a static literal — we cannot read it.
	EnrichmentValueUnsupported EnrichmentValueEvent = "unsupported"
)
