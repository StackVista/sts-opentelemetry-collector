# Cluster-agent-compatible Kubernetes topology exporter

`stsk8stopology` sends Kubernetes topology to the Receiver intake
(`/stsAgent/intake`) in the format the cluster-agent's `kubernetes_api_topology`
check produces. It lets a platform running the legacy Kubernetes StackPack
receive its existing component contract, including `data` and
`sourceProperties`, without the cluster-agent. It is a temporary compatibility
path; native OTel topology does not use it.

## How it works

The exporter consumes the log records of the `k8sresourcereceiver` Cluster
Observer and keeps the latest version of every object in memory. On each
`interval` it runs the cluster-agent collectors over that cache and sends the
result as one full topology snapshot, split into ordered requests when it
exceeds `max_elements_per_request`.

The topology sync deletes every element missing from a snapshot, so the
exporter only sends after it has seen a complete, bracketed observer snapshot:

- The observer must set `emit_snapshot_boundaries: true`.
- An end boundary with `k8s.snapshot.complete=false` (a static watch not synced
  or forbidden) pauses sending.
- A reset boundary, emitted when the observer stops collecting, for example
  after losing leadership, pauses sending until the next complete snapshot.
- Sending also pauses when no complete snapshot has arrived within
  `snapshot_max_age`.

If any request in a snapshot fails, the remaining requests are not sent and the
snapshot is retried at the next interval. A reset cancels a snapshot that is
still being sent; the reset and a delivery's read of the cache are serialized,
so no delivery can start from the state being reset. A panic in the collectors abandons the snapshot and stops the
exporter with a permanent error status rather than terminating the collector.

The topology sync accepts a snapshot from a producer it has not recently seen
immediately, and ignores data from producers that are no longer active. Each
collector replica therefore uses its pod hostname as `internal_hostname`, so a
new leader takes over at once and a former leader's late requests cannot
complete its successor's snapshot. A producer that returns after another has
taken over is only accepted once it is the sole recent producer, which bounds,
rather than prevents, the delay when rolling back to the cluster agent.

The sync has no fencing, so a former leader's first request that is processed
after its successor's start would take ownership back. Leader election stops
the former leader, cancelling its requests, before the lease can pass. The
successor then waits `handover_delay` after the observer becomes ready before
its first snapshot, which covers requests the Receiver accepted but had not yet
processed. A request delayed beyond that is only recovered once the successor
is the sole recent producer.

Pipelines feeding this exporter must not reorder records: do not add batching
or asynchronous processors, and the exporter has no sending queue.

The observer must watch every kind the collectors read, including Secrets and
ConfigMaps if those components are required. Its payload budgets drop large
objects, and dropped objects are deleted from the platform topology, so
disable or size them for this pipeline.

## Following the platform

With `discovery_enabled` (the default), the exporter reads the Receiver's
`/stsAgent/features` with its own endpoint and key. While the platform
advertises `otel-cluster-topology: true`, no legacy topology is sent;
Kubernetes-V2's auto-expiry then removes the legacy elements, and merged
components keep their native data. Absent, `false`, unreachable or failing
answers keep the current mode, which starts as sending: an older platform
without the capability always receives legacy topology.

The first valid answer, awaited before the first snapshot, decides the mode.
Later changes need three consecutive matching answers from the one-minute poll.
A confirmed disable cancels a snapshot that is still being sent. Resuming
sends a snapshot immediately. The
`otelcol_stsk8stopology_legacy_export_enabled` gauge reports the current mode.

## Configuration

| Key | Default | Description |
|---|---|---|
| `endpoint` | | Receiver intake URL ending in `/stsAgent/intake` |
| `api_key` | | Receiver API key |
| `cluster_name` | | Topology instance URL; must match the StackPack instance |
| `cluster_type` | `kubernetes` | `kubernetes` or `openshift` |
| `internal_hostname` | pod hostname | Producer identity; see below |
| `interval` | `90s` | Time between snapshots |
| `handover_delay` | `60s` | Wait after the observer becomes ready before the first snapshot |
| `snapshot_max_age` | `15m` | Pause sending after this long without a complete observer snapshot |
| `collect_timeout` | `10m` | Bound on one topology build |
| `max_elements_per_request` | `10000` | Components and relations per request |
| `max_attempts` | `3` | Attempts per request |
| `timeout` | `30s` | Per-request timeout |
| `resources.*` | all `true` | Same switches as the cluster-agent check |
| `csi_pv_mapper_enabled` | `false` | CSI persistent volume source mapping |
| `discovery_enabled` | `true` | Follow the platform's `otel-cluster-topology` capability |
| `proxy_url`, `tls.insecure_skip_verify` | | Receiver transport options |

## Vendored cluster-agent code

`internal/topologycollectors`, `internal/urn`, `internal/hostname`,
`internal/dns` and `internal/apiserver` are copied from
[stackstate-agent](https://github.com/StackVista/stackstate-agent)
`pkg/collector/corechecks/cluster` at `dd1cba0384` (Apache License 2.0) and are
excluded from linting. Changes from the original:

- Agent imports point at local packages; `internal/log` and `internal/util`
  replace the agent logging and path helpers.
- `hostname.GetHostname` takes the cluster name from the topology instance
  instead of the agent's global `cluster_name` setting; the agent sets both from
  the same value.
- `VolumeAttachment`s with an inline volume spec are skipped instead of
  dereferencing a missing `persistentVolumeName`.
- The relation cache uses a mutex; the original appended to it concurrently.
- `log.Warnf` no longer uses `%w`.
- Protobuf `ProtoMessage` wrappers were removed; `k8s.io/api` no longer
  provides them.

`testdata/golden.json` is the cluster-agent's output for `testdata/cluster.json`.
Regenerate it with `testdata/agentgolden/generate.sh <stackstate-agent checkout>`;
`TestTopologyMatchesClusterAgent` requires the exporter to reproduce it exactly.
