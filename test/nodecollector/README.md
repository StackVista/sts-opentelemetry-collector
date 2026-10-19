# Node collector prototype

Scratch prototype of the native OpenTelemetry pipeline that replaces the Agent V2 node-agent metric
collection. It exists to prove that the receivers and scrapers can produce the `k8s.*`, `container.*`
and `system.*` series on a real node. It is deliberately not product code: `#686` promotes the config
and the RBAC into the agent chart, and `#687` turns the config into a validate fixture.

## Contents

| File | Purpose |
| ---- | ------- |
| `config.yaml` | Collector config: `kubeletstats` (all four metric groups), four kubelet scrape jobs, `hostmetrics` with `root_path`, `k8sattributes`, OTLP exporter and auth, health, pprof and self-telemetry. |
| `rbac.yaml` | ServiceAccount, ClusterRole and ClusterRoleBinding for `nodes/stats`, `nodes/proxy`, `nodes/metrics`, `nodes/spec` and the `k8sattributes` reads. |
| `daemonset.yaml` | Raw DaemonSet with the hostfs mounts, downward API env and probes. |

Everything assumes the `default` namespace. The manifest refers to a ConfigMap
`sts-node-collector-prototype` and a Secret `sts-node-collector-prototype` with an `api-key` entry.

## Deploy

The agent image from `agent-otel-builder.yaml` is not published for branch builds. Build or load one
first and set `spec.template.spec.containers[0].image` in `daemonset.yaml` accordingly. The local
review image used during development was `sts-otel-agent:review-195-branch`.

```sh
kubectl -n default create secret generic sts-node-collector-prototype \
  --from-literal=api-key="$STS_API_KEY"

kubectl -n default create configmap sts-node-collector-prototype \
  --from-file=config.yaml

kubectl apply -f rbac.yaml
kubectl apply -f daemonset.yaml
```

Edit the `K8S_CLUSTER_NAME` and `PLATFORM_OTLP_ENDPOINT` values in `daemonset.yaml` before applying.
The cluster name is what binds node and container telemetry to the same StackGraph node component the
cluster agent already produces (see `#477`), and the endpoint must point at the platform's OTLP
listener. The kubelet endpoints use `KUBE_NODE_IP` (`status.hostIP`) rather than the node name, so
they also work where node-name DNS does not resolve.

### kind

The prototype was exercised on a kind cluster with the image loaded locally and the exporter
replaced by `debug`, since kind has no platform to send to:

```sh
kind create cluster --name node-collector
kind load docker-image <agent-image> --name node-collector

kubectl create configmap sts-node-collector-prototype --from-file=config.yaml
kubectl apply -f rbac.yaml
kubectl apply -f daemonset.yaml
```

Use `runAsUser: 0` and the `default` namespace as shipped, or update the ClusterRoleBinding subject
namespace to match. Two kind-specific results, neither a config bug:

- `/metrics/slis` on kind v1.37 exposes no `kubernetes_healthcheck*` series, so that family cannot be
  verified there. `/metrics`, `/metrics/cadvisor` and `/metrics/probes` do produce data.
- `system.filesystem.*` is absent because the kind node's root is `overlay`, which the scraper
  excludes. On a node with an `ext4`/`xfs` root it appears; `include_virtual_filesystems: true`
  would surface it in kind.

## Validate the config offline

The collector binary validates the config against the component schemas. Point `root_path` and the
service account paths at real directories so the checks pass outside a cluster:

```sh
mkdir -p /tmp/nc-validate/hostfs /tmp/nc-validate/sa
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout /tmp/nc-validate/sa/ca.key -out /tmp/nc-validate/sa/ca.crt \
  -days 1 -subj "/CN=test"
printf 'token\n' > /tmp/nc-validate/sa/token

docker run --rm --security-opt label=disable \
  -e KUBE_NODE_NAME=test-node -e KUBE_NODE_IP=192.0.2.1 -e K8S_CLUSTER_NAME=test-cluster -e POD_UID=test-uid \
  -e PLATFORM_OTLP_ENDPOINT=https://suse-observability:443 -e STS_API_KEY=test-token \
  -v "$PWD/test/nodecollector:/cfg:ro" \
  -v /tmp/nc-validate/hostfs:/hostfs:ro \
  -v /tmp/nc-validate/sa:/var/run/secrets/kubernetes.io/serviceaccount:ro \
  <agent-image> validate --config /cfg/config.yaml
```

## Acceptance

Apply the manifests, then confirm native series are visible in the metric explorer:

- `k8s.node.*`, `k8s.pod.*` and `container.*` (dotted) from `kubeletstats`, bound to the existing
  node, pod and container components.
- `container_*` from the cAdvisor scrape.
- `kubernetes_healthcheck*` from `/metrics/slis` and `prober_probe_*` from `/metrics/probes`.
- `kubelet_*` from `/metrics`.
- `system.cpu.*`, `system.memory.*`, `system.disk.*`, `system.filesystem.*`, `system.network.*`,
  `system.paging.*` and `system.processes.*` from `hostmetrics`.

## hostmetrics and the non-root question

The prototype ships as `runAsUser: 0` so the per-process scraper works. The chart decision in
`#686` is whether to keep that or run non-root with partial metrics. The table below was checked on
kind by running the same config as UID 0 and UID 10001 and diffing the unique series.

| Scraper | Non-root | What happens |
| ------- | -------- | ------------ |
| `cpu`, `load`, `memory`, `paging`, `network`, `system`, `disk` | works | identical unique series as root |
| `filesystem` | works with the hostfs mount | no per-process access; kind produced none because its root is `overlay` and is excluded |
| `processes` | works | counts `/hostfs/proc` entries; identical unique series as root |
| `process` | partial | emits CPU and memory for readable processes, but `readlink /hostfs/proc/<pid>/exe` and `open /hostfs/proc/<pid>/io` are denied for other users, so identity and disk I/O are incomplete |

The kubelet, cAdvisor and probe metrics come from the kubelet API with the service account token, so
they do not depend on the container user; their unique series were identical root and non-root.
`process` is not in the shipped config; it was added temporarily for the comparison.

## Teardown

```sh
kubectl delete -f daemonset.yaml
kubectl delete -f rbac.yaml
kubectl -n default delete configmap sts-node-collector-prototype
kubectl -n default delete secret sts-node-collector-prototype
```

## Not in scope

Agent V2 naming and labels (`#679`, `#680`, `#681`), pod-spec and pod-state metrics (`#682`), the
kubelet health service check (`#683`), chart integration (`#686`), the validate fixture and CI gate
(`#687`), documentation (`#688`) and the remaining `system.*` families (`#693`, `#695`, `#696`).
This prototype emits raw native names only.
