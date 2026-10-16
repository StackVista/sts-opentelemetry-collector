# Cluster-agent-compatible Kubernetes state metrics

This module produces the `kubernetes_state.*` series that the cluster agent's
`kubernetes_state_core` check sends, from the same kube-state-metrics (KSM)
library and the agent's own mapping code. It does not yet register a collector
component.

## How it works

`ksm.New` takes the check configuration (`KSMConfig`) and the values the agent
read from its own configuration and the cluster agent (`Options`: cluster name
and ID, global tags, global labels and annotations as tags, namespace tags).
`BuildStores` starts the KSM informers for the configured collectors.
`Collect` applies the agent's label joins, label mapping, owner tags,
transformers and aggregators to the current store contents and passes each
series to a sender.

`pmetricsender` turns the series into OTLP gauges:

- the metric name is the agent's (`kubernetes_state.<name>`);
- every Datadog tag becomes a data point attribute (`key:value` → `key`);
  where a tag key repeats, the last value wins;
- `sts_host` carries the host the check assigns (`<node>-<cluster name>`), when
  it assigns one;
- `interval` is `"0"`, as the Receiver labels agent check series;
- the matching `k8sclusterreceiver` attribute (`k8s.namespace.name`,
  `k8s.pod.name`, `k8s.pod.uid`, `k8s.node.name`, `k8s.deployment.name`,
  `container.id`, …) is added alongside the Datadog tag;
- service checks are dropped, because the cluster agent's serializer does not
  forward them.

No resource or scope attributes are set.

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
- To match v2.13 output:
  - an `endpoints_extended` factory, ported from v2.13's Endpoints store,
    restores `kube_endpoint_address_available` and
    `kube_endpoint_address_not_ready`;
  - the store removes `path_type` from `kube_ingress_path`,
    `external_traffic_policy` from `kube_service_info` and `ephemeral` from
    the pod PVC volume families, and drops the ephemeral-volume series v2.13
    did not emit;
  - `endpoints` is added to KSM's default collector list, used when no
    collectors are configured.
- `slices.DeleteFunc` replaces `lo.Filter`, and the unused CRD and APIService
  condition helpers are removed.
