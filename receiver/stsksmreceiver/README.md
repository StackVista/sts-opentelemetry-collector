# Cluster-agent-compatible Kubernetes state metrics receiver

> [!NOTE]
> The Go module of this component is internal to SUSE Observability: its API and configuration can change in any release. Configure the collector as described in the [SUSE Observability documentation](https://documentation.suse.com/cloudnative/suse-observability/latest/en/setup/otel/overview.html).

`stsksm` produces the `kubernetes_state.*` series that the cluster agent's
`kubernetes_state_core` check sends, from the same kube-state-metrics (KSM)
library and the agent's own mapping code. It is a temporary compatibility path.

## Configuration

| Key | Default | Description |
|---|---|---|
| `cluster_name` | (required) | The cluster agent's cluster name; lowercased, as the agent does |
| `auth_type` | `serviceAccount` | `serviceAccount` or `kubeConfig` |
| `collection_interval` | `40s` | Interval between collections, the check interval |
| `resync_period` | `300s` | Informer resync period, in whole seconds |
| `collectors` | the cluster agent chart's list | kube-state-metrics resources to collect |
| `labels_as_tags`, `annotations_as_tags` | | Per resource kind, label or annotation → tag, as in the check's instance config |
| `k8s_leader_elector` | | ID of a `k8sleaderelector` extension; only the leader collects |

## How it works

At start, the receiver reads the kube-system namespace UID as the cluster ID,
retrying for up to 10 seconds. If it cannot, it warns and sends the series
without `orch_cluster_id`, as the cluster agent does. Without a leader elector,
collection starts with the receiver; with one, it starts when the replica
becomes leader and stops, with the informers, when it loses leadership.

`ksm.New` takes the check configuration (`KSMConfig`) and the values the agent
read from its own configuration and the cluster agent (`Options`: cluster name
and ID, global tags, global labels and annotations as tags, namespace tags).
`BuildStores` starts the KSM informers for the configured collectors.
`Collect` applies the agent's label joins, label mapping, owner tags,
transformers and aggregators to the current store contents and passes each
series to a sender. A failed collection is logged and retried at the next
interval.

`pmetricsender` turns the series into OTLP gauges:

- the metric name is the agent's (`kubernetes_state.<name>`);
- every Datadog tag becomes a data point attribute (`key:value` → `key`), as
  the Receiver stores it: tags are sorted and the last value of a repeated key
  wins, and a tag without a value becomes `label_<tag>: "true"`;
- `sts_host` carries the host the check assigns (`<node>-<cluster name>`), when
  it assigns one;
- `interval` is `"0"`, as the Receiver labels agent check series;
- the matching `k8sclusterreceiver` attribute (`k8s.namespace.name`,
  `k8s.pod.name`, `k8s.pod.uid`, `k8s.node.name`, `k8s.deployment.name`,
  `container.id`, …) is added alongside the Datadog tag;
- a series repeated with the same name, host and tags keeps its last value, as
  the agent's aggregator does;
- service checks are dropped, because the cluster agent's serializer does not
  forward them.

The receiver sends each collection as one resource with two attributes,
`k8s.cluster.name` and an empty `service.namespace`. The platform collector
inserts `service.namespace=default` only where the attribute is absent, and the
empty value is dropped when stored. The scope name is empty.

## Vendored cluster-agent code

`internal/ksm` is copied from
[stackstate-agent](https://github.com/StackVista/stackstate-agent) at
`13679eda6f` (branch `stackstate-7.78.2`):
`pkg/collector/corechecks/cluster/ksm` (`kubernetes_state*.go`, and from
`customresources/` the pod, node, job, deployment, replicaset, statefulset,
daemonset and controllerrevision factories, the rollout tracker and `utils.go`)
and `pkg/kubestatemetrics/store` and `builder`. `internal/ddcompat` holds
`comp/core/tagger/tags`, `comp/core/tagger/kubetags` and `pkg/util/kubernetes`
(`helpers.go`, `const.go`) from the same commit, and replacements for the agent
sender, service check status, logger and test mock sender. The agent's tests
for these files are copied with them. All of it is Apache License 2.0 and
excluded from linting.

The agent pins the kube-state-metrics fork
`github.com/L3n41c/kube-state-metrics/v2 v2.13.1-0.20250808193648-ead8278ad9fb`.
Its stores do not implement the client-go `cache.Store` interface from v0.36,
so this module uses upstream `k8s.io/kube-state-metrics/v2 v2.20.0`.

Changes from the original:

- `New`, `BuildStores` and `Collect` replace the check's `Configure` and `Run`.
  There is no leader election, no CRD discovery and no custom resource state
  configuration.
- `Options` replaces the agent configuration, cluster name and cluster ID
  lookups. An optional `NamespaceTagger` replaces the tagger; without one,
  metrics get no namespace tags. Resource kinds for labels-as-tags come from
  API discovery instead of the agent's global resource type cache.
- Factories take a `kubernetes.Interface` instead of the agent API client.
- Pod collection from the kubelet (workloadmeta) and the unassigned-pods mode
  are removed, with the `pod_collection_mode`, `custom_resource`,
  `collect_crd_metrics` and `skip_leader_election` options.
- The APIService, VerticalPodAutoscaler, CRD and custom resource factories, and
  those for API versions removed before Kubernetes 1.25, are not vendored.
- For kube-state-metrics v2.20, the store implements `Bookmark` and
  `LastStoreSyncResourceVersion`, and the store builders accept the object
  limit argument, which is ignored.
- To match the output of the agent's v2.13 fork, from a comparison of every
  `internal/store` generator for the configured collectors:
  - `internal/ksm/ksm213` holds the v2.13 generators for
    `kube_pod_status_reason` (one row per known reason, set from
    `status.reason` only), `kube_pod_tolerations` (empty fields omitted, empty
    tolerations skipped, no deduplication), `kube_job_status_failed` (no
    series while `failed` is set but no `Failed` condition is) and
    `kube_cronjob_spec_suspend` (no series when `suspend` is unset); the store
    builder uses them in place of the upstream ones;
  - an `endpoints_extended` factory, ported from v2.13's Endpoints store,
    restores `kube_endpoint_address_available` and
    `kube_endpoint_address_not_ready`. It reads `v1.Endpoints`, deprecated
    since Kubernetes 1.33, as the agent does;
  - the store removes `path_type` from `kube_ingress_path`,
    `external_traffic_policy` from `kube_service_info`, `reason` from
    `kube_deployment_status_condition`, `container` from both HPA target
    metric families and `ephemeral` from the pod PVC volume families, and
    drops the ephemeral-volume series v2.13 did not emit;
  - `endpoints` is added to KSM's default collector list, used when no
    collectors are configured.
- `slices.DeleteFunc` replaces `lo.Filter`, and the unused CRD and APIService
  condition helpers are removed.

Differences from v2.13 that are left in place:

- Families v2.13 did not have are dropped by the mapping, as the agent drops
  unknown families: pod resource claims, disruption reason, unscheduled time,
  init container start, exit code and termination time; node pod CIDRs; PV
  access modes; Job ready and suspended; Deployment owner and terminating
  replicas; ReplicaSet terminating replicas; HPA tolerances; CronJob invalid
  schedule; deletion timestamps.
- Changed families nothing consumes: `kube_pod_ips`, `kube_endpoint_address`,
  `kube_endpoint_ports`, and the new `kubeproxy_version` value of
  `kube_node_info` and `reclaim_policy` label of `kube_persistentvolume_info`,
  which no join or aggregator reads.
- `kube_cronjob_next_schedule_time` now uses `netresearch/go-cron`, which
  differs from `robfig/cron` at DST transitions; the agent maps it only to a
  service check, which is dropped.
- Quantities above 2^63/1000 base units (about 8 PiB) are reported correctly;
  v2.13 overflowed them.
- Where v2.13 panicked (unset `spec.replicas`, `minReplicas` or `suspend`, an
  unparseable schedule or rolling-update value, an HPA status metric without
  its source), v2.20 omits or defaults the value. The API server defaults or
  validates all of these.
- Label names generated from Kubernetes labels and annotations differ only
  when a `_conflictN` suffix would collide with another label.

`testdata/golden.json` holds the series the agent's `kubernetes_state_core`
check sends for `testdata/objects.json`, configured as the cluster agent chart
configures it: the check builds its stores and runs against a fake clientset,
and each gauge gets the check tags, sorted and de-duplicated tags and, per
context, its last value, as the agent's sender and aggregator do. Regenerate it
with `testdata/agentgolden/generate.sh <stackstate-agent checkout>`, which runs
the harness in a temporary worktree of the checkout's `HEAD` with the agent's Go
version. `TestSeriesMatchClusterAgent` requires the receiver to send the same
series, compared by name, host, tags and value.

- Both sides run in a `testing/synctest` bubble whose clock does not move, so
  ages, uptimes, job and rollout durations and the time since a CronJob's last
  schedule are compared exactly.
- A CronJob without `suspend` gets the API server's default before either side
  sees it: v2.13's `kube_cronjob_next_schedule_time` dereferences the field. The
  unset case is covered by the unit tests only.
- Storage classes and volume attachments produce no gauges on either side.
