# Peer-sync protocol

A short reference for the in-process peer sync that keeps `k8scrdreceiver`
replicas warm. The goal is to avoid a cold informer LIST during failover: when
the leader dies, a secondary should be able to take over with a cache that is
no more than a few seconds stale.

Source files: `peer_sync_cache_store.go`, `peer_store.go`, `crd_collector.go`,
`receiver.go`.

## Components

- **peerSyncCacheStore** — owns the synchronised `resourceCache`, runs on every
  replica, exposes two HTTP endpoints, and broadcasts deltas when leader.
- **crdCollector** — runs only on the leader. Each cycle it reads informer
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

### Leader cycle (`crdCollector.runIncrement`, every `IncrementInterval`)

1. Read current informer state.
2. `peerStore.ComputeChanges(currentCRDs, currentCRs)` diffs cache vs informer:
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

Within `crdCollector.emitIncrement` adds and deletes are handled in different
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
- `bootstrap_total{outcome=applied|leader_empty|timed_out, source=...}` — replica startup.
- `cached_resources{kind=crd|cr}` — cache occupancy on every replica.

The store also tracks consecutive broadcast failures internally and escalates
the log line from Debug to Warn at threshold 5 so operators notice peer sync
breaking even without metric scraping.

## Tunables (Config)

| Field                | Default | Notes                                              |
|----------------------|---------|----------------------------------------------------|
| `IncrementInterval`  | 10s     | Leader cycle period. Bounds reconciliation lag.    |
| `SnapshotInterval`   | 5m      | Forces periodic full re-emit for platform TTL.     |
| `PeerSyncPort`       | 4319    | HTTP server port on each replica.                  |
| `PeerSyncDNS`        | —       | Headless service FQDN. Empty ⇒ single-replica.    |
