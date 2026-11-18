# Kubernetes resource receiver

> [!NOTE]
> The Go module of this component is internal to SUSE Observability: its API and configuration can change in any release. Configure the collector as described in the [SUSE Observability documentation](https://documentation.suse.com/cloudnative/suse-observability/latest/en/setup/otel/overview.html).

## Administrator collection denies

`denied_objects` blocks an exact plural resource name and API group:

```yaml
denied_objects:
  - name: certificates
    group: cert-manager.io
  - name: virtualmachines
    group: kubevirt.io
```

The deny covers every namespace and API version of that type. It is checked
before automatic CR discovery or resolved static watches make API requests.
An explicit `objects` entry with the same name and group fails configuration
validation. CRD definitions remain visible; their denied instances are not
collected. This does not change Kubernetes read permissions.
Cached denied objects are removed without exporting their bodies, including in
deletion events. Previously exported data follows downstream retention.

## CollectionDeclaration declarations

The receiver uses one runtime for YAML baseline collection and declarations.
When the declaration API is available, the active leader reads cluster-scoped
`observability.suse.com/v1alpha1` CollectionDeclarations directly. Their spec selects
CRD-backed custom resources through each type's served storage version, using
namespace names and Kubernetes label and field selectors. Declaration version pinning is
not supported; existing YAML version selections remain.

Baseline selections and declarations contribute a union. Identical watches are
shared and objects are deduplicated per API representation before payload budgets.
`max_declaration_watches` limits additional distinct watches beyond the YAML
baseline. The provisional default is 256; zero uses that default and negative
values are invalid. A key includes API version, resource, namespace, label and
field selectors. Exact shared watches count once; baseline and control watches
do not consume this budget. Declarations are considered by name, then selection
index and resource name. Adding an earlier declaration can displace a later one.
Every namespace for a resolved type must fit together, otherwise that type
reports `Blocked / WatchBudgetExceeded`. Other permitted wildcard types keep
collecting. Freeing capacity automatically retries blocked selections.
Superseded declaration watches use spare capacity during version handover;
at capacity, their last observations are retained without keeping their watches.
This limits registered watches, not cache bytes or the number of objects.

Editing a declaration's resource or filters withdraws its previous contribution,
including when the replacement is pending or blocked. Representation changes
for an unchanged selection retain previous observations until replacements synchronize.
Unavailable APIs and missing resource-read permissions are retried. Declarations
do not change RBAC or render ConfigMaps or collector rollouts.

The leader publishes generation-bound feedback through the status subresource
using `UpdateStatus` with the current resourceVersion. Conflict retries read the
declaration again and check its UID, generation and deletion state. Status-only updates do
not trigger discovery. The current per-selection summary is provisional; wildcard
children are tracked internally even when their details are not on the wire.

Declaration objects and their status are also reported as custom resources,
subject to the same administrator denies and CR payload budget. Disabling their
export does not disable the control reads needed to process selections.

Blocked watches for unchanged selections retain their last successful objects until observation recovers.
Permission failures delay further API requests for that watch by 30 minutes.
Other failures follow client-go retry behavior. Removing a watch cancels its wait.
There is no retention timeout because an unavailable API cannot prove deletion.
Discovery failures retain known APIs and contributions; an authoritative CRD
deletion withdraws its resources. On takeover, uncertain objects from the peer
cache remain in the reporting view until their watches synchronize. After an
explicit edit, local previous-scope metadata prevents peer data
from restoring the withdrawn contribution. That metadata is not in the peer
protocol, so a fresh runtime retains uncertain peer objects conservatively.
Retention is applied before payload budgets. A pending custom declaration does not stop
the healthy standard Kubernetes baseline from being reported.

Informer recovery remains visible in `informer_reconciles_total`. The declaration
path records watch transitions and periodic reconciliation, with bounded
`kind` (`cr`, `static`, `control`) and `outcome` labels. Shared watches are counted
once. Unresolved APIs and administrator denies also appear in the counter.
Watch failures and recovery log once per health transition.
Unresolved YAML object selections also log once per resolution transition.
Required YAML watches must synchronize before a snapshot can be complete.
Health and status checks run independently of object snapshot assembly, which
reads informer stores at the reporting interval.

## Pod container summary attributes

Core Pod object logs include three integer attributes when
`status.containerStatuses` is present and valid:

| Attribute | Value |
|---|---|
| `k8s.pod.container_count` | Number of regular container statuses |
| `k8s.pod.ready_container_count` | Number whose `ready` is true |
| `k8s.pod.restart_count` | Sum of regular container restart counts |

Init and ephemeral container statuses are excluded. An empty status list emits
zeros; an absent or malformed list omits the summary attributes. Missing ready
and restart fields count as false and zero. Negative or noninteger restart
counts and nonboolean ready values make the summary invalid, without dropping
the object event. The raw object body is unchanged. These attributes let
StackPack presentations reproduce legacy readiness and restart values without
platform expression extensions.

## Snapshot boundaries

With `emit_snapshot_boundaries: true`, the receiver brackets each snapshot with
`KubernetesSnapshotBoundary` records carrying `k8s.snapshot.boundary`
(`start`/`end`) and a shared `k8s.snapshot.id`. The end record's
`k8s.snapshot.complete` is true only when every configured static watch had a
synced informer before the snapshot was read. A `reset` boundary is emitted when
collection stops, for example on leadership loss.

Consumers that treat absence as deletion, such as the `stsk8stopology`
exporter, use these records to act only on complete snapshots. Existing
mappings select other event names and ignore them.

## Compact object cache

With `compact_cache: true`, CR and static objects are held in the informer and
peer caches as their sanitized JSON plus the metadata used for keys and change
detection, which retains far less memory than decoded objects. Objects are
decoded only to emit: changed objects in increments, every object in a
snapshot one at a time, and the cached object on deletion. Emitted records are
the same as with the option off, at the cost of decoding CPU on each snapshot.
An object that fails to decode is logged and skipped for that emission. CRDs
are not compacted. Peer sync uses the same wire format in both modes, so
replicas with different settings interoperate.

## Peer-sync protocol

A short reference for the in-process peer sync that keeps `k8sresourcereceiver`
replicas warm. The goal is to avoid a cold informer LIST during failover: when
the leader dies, a secondary should be able to take over with a cache that is
no more than a few seconds stale.

Source files: `peer_sync_cache_store.go`, `peer_store.go`, `crd_collector.go`,
`receiver.go`.

## Components

- **peerSyncCacheStore** — owns the synchronised `resourceCache`, runs on every
  replica, exposes two HTTP endpoints, and broadcasts deltas when leader.
- **resourceCollector** — runs only on the leader. Each cycle it reads informer
  state, asks the peer store for a delta, and emits + applies + broadcasts.
- **Leader election** (`k8sleaderelector` extension) — flips the leader bit on
  the peer store via `SetLeader(bool)` and starts/stops the collector.

## HTTP endpoints

Both served by every replica on `:4319` (default).

| Path               | Method | Purpose                                        |
|--------------------|--------|------------------------------------------------|
| `/sync/snapshot`   | GET    | Bootstrap pull — full cache + meta             |
| `/sync/increments` | POST   | Per-cycle delta push from leader to peers      |

Responses carry a `Source` field (`leader` / `secondary`) and `LastAppliedAt`
so callers can rank candidates.

## Algorithm

### Replica start (every replica, leader or not)

1. Start the peer store HTTP server.
2. `Bootstrap(ctx)` — pull a snapshot from peers:
   - Resolve the headless service DNS, skip own POD_IP, GET each peer.
   - Prefer non-empty leader (return immediately).
   - Empty leader → "cluster is cold", start fresh.
   - Otherwise track the freshest non-empty secondary as a fallback.
   - Retry with exponential backoff up to `bootstrapMaxDuration` (30s).
3. Deltas that arrive on `/sync/increments` during step 2 are buffered
   (cap `deltaBufferMaxSize`). On bootstrap completion the buffer is drained,
   skipping any delta with `AppliedAt` before the snapshot's `LastSnapshotTime`.
4. Mark `ready = true`. Subsequent deltas apply directly.

### Leader cycle (`resourceCollector.runIncrement`, every `IncrementInterval`)

1. Read current informer state.
2. `peerStore.ComputeChanges(currentCRDs, currentObjects)` diffs cache vs informer:
   - in informer, not in cache → `ADDED`
   - both, different `ResourceVersion` → `MODIFIED`
   - in cache, not in informer → `DELETED`
3. Decide snapshot vs increment:
   - If cache empty or `time.Since(LastSnapshotTime) >= SnapshotInterval` →
     emit a full snapshot (all current resources as `ADDED`, plus deletes from
     the diff). Apply via `ApplyDelta` with `LastSnapshotTime = now`.
   - Otherwise → emit the diff as an increment.
4. `ApplyDelta` updates the local cache and broadcasts the delta to all peers
   (concurrent POSTs, retried with backoff, capped at `broadcastAckTimeout`).
   Best-effort: a broadcast that times out is recorded as failed but does not
   block the cycle.

### Secondary receive (`handleIncrement`)

1. If not `ready`, buffer the delta and return.
2. Otherwise apply to the local cache; update `lastSnapshotTime` if the delta
   carries a non-zero one.

## Crash-safety: asymmetric apply ordering

Within `resourceCollector.emitIncrement` adds and deletes are handled in different
orders to bias the failure mode toward duplicate emits rather than missed ones
(duplicates are idempotent on the platform side).

| Change type | Order                     | Rationale                                                                                     |
|-------------|---------------------------|-----------------------------------------------------------------------------------------------|
| ADD / MOD   | apply+broadcast → emit    | Crash between apply and emit ⇒ next leader's cache matches informer ⇒ no duplicate.           |
| DELETE      | emit → apply+broadcast    | Crash between emit and apply ⇒ next leader's cache still has it ⇒ DELETE re-emitted (dupe).   |

## Reconciliation guarantees

Cache drift between leader and secondary (from missed broadcasts, dropped
packets, etc.) is bounded by the leader's next cycle:

- The new leader's first `runIncrement` does a fresh informer LIST
  (`WaitForCacheSync` on Start) and runs `ComputeChanges` against its possibly
  stale cache.
- Anything missing from cache surfaces as an `ADDED` re-emit. Anything stale in
  cache that's gone in informer surfaces as a `DELETED` re-emit.
- Both paths are duplicates of what the previous leader sent (or tried to) —
  idempotent on the platform.

In other words: the protocol does not need a separate divergence-detection
mechanism (hash, checksum, etc.). `ComputeChanges` is the reconciliation
primitive, and it runs every `IncrementInterval`.

## Failure modes

| Scenario                            | Outcome                                                                          |
|-------------------------------------|----------------------------------------------------------------------------------|
| Leader graceful shutdown            | Lease expires → secondary takes over with warm cache, first cycle reconciles.    |
| Leader hard crash                   | Same as above, plus up to `IncrementInterval` of staleness on the secondary.     |
| Secondary crash                     | No effect on emission. Replacement bootstraps from the leader.                   |
| Broadcast partial failure           | Drift on offline secondary, healed when it (or another) becomes leader.          |
| Bootstrap exceeds `bootstrapMaxDuration` | Replica starts with empty cache. First cycle as leader emits a full snapshot. |
| All replicas restart simultaneously | Cold start. First leader does a full LIST + snapshot emit.                       |

## Observability

Watch these metrics for protocol health:

- `peer_broadcasts_total{outcome=success|failed}` — per-cycle broadcast result.
- `peer_push_attempts_total{outcome=...}` — per-peer push attempts (under broadcasts).
- `peer_push_duration_seconds`, `peer_push_bytes` — push latency and payload size.
- `bootstrap_total{outcome=applied|leader_empty|timed_out|canceled, source=...}` — replica startup.
- `cached_resources{kind=crd|object}` — cache occupancy on every replica.

The store also tracks consecutive broadcast failures internally and escalates
the log line from Debug to Warn at threshold 5 so operators notice peer sync
breaking even without metric scraping.

## Object sources

The receiver emits two flavours of object log:

- **CR-shape** (`event.name = KubernetesCustomResourceEvent`) — for objects
  whose type was discovered from a CRD on the cluster, or for `Config.Objects`
  entries that overlap a CRD-defined type (marked `CRDBacked`). Downstream
  log-based mappings keep the existing CR shape for these.
- **Object-shape** (`event.name = KubernetesObjectEvent`) — for plain
  `Config.Objects` entries that have no backing CRD (e.g. core `pods`,
  `deployments`). Neutral event name for resources without a CRD definition.

Each cached object carries an `ObjectSource` (`cr` / `static`) recorded at
discovery time; `eventNameForSource` maps that to the emitted event name on
both the snapshot and increment paths.

The collection plan resolves YAML objects, CR group discovery and declarations
together. Overlapping selections form a union and share identical watches.
CRD-backed objects use the CR log shape; resources without a backing CRD use
the Object shape.

## Secrets and ConfigMaps

Core Secrets and ConfigMaps are sanitized by an informer transform, so their
contents never reach the cache, the peer sync or any consumer
(`common/k8ssanitize`). A Secret's data is replaced by the cluster agent's hash
of it under `<data hash>`; a TLS Secret also gets its certificate's expiry
under `<certificate expiration>`. ConfigMap data is truncated to
`ConfigMapMaxDataSize`, shared evenly between the keys, and binary data is
replaced by its length and hash. Annotations that can carry the data are
redacted on both. Topology built from them matches the cluster agent's.
Sanitizing is idempotent, because client-go applies the transform twice when an
informer initialises from a streaming list. Use `DeniedObjects` to keep either
out entirely.

## Tunables (Config)

| Field                | Default | Notes                                              |
|----------------------|---------|----------------------------------------------------|
| `IncrementInterval`  | 10s     | Leader cycle period. Bounds reconciliation lag.    |
| `SnapshotInterval`   | 5m      | Forces periodic full re-emit for platform TTL.     |
| `PeerSyncPort`       | 4319    | HTTP server port on each replica.                  |
| `PeerSyncDNS`        | —       | Headless service FQDN. Empty ⇒ single-replica.    |
| `MaxCRTotalDataSizeBytes` | 10MiB   | Total CR payload budget per collection cycle.      |
| `MaxDeclarationWatches` | 256 | Maximum distinct declaration watches beyond the YAML baseline. Zero uses the default. |
| `MaxObjectTotalDataSizeBytes` | 10MiB | Total static object payload budget per collection cycle. Zero disables it. |
| `ConfigMapMaxDataSize` | 100KiB | ConfigMap data kept per object. Zero keeps all. |
| `CompactCache` | false | Cache objects as compact JSON. See [Compact object cache](#compact-object-cache). |

Payload budgets are applied after reading informer caches and before diffing
against the peer cache. CRDs do not count against the budget. CRs and static
objects have separate budgets; each bucket is filled smallest-payload-first and
then by stable object identity. This maximises represented resources while making
drops deterministic. Per-kind budgets or rotation could improve fairness later,
but would add configuration and churn in the first version.
