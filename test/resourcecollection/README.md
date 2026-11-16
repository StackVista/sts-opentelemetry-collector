# ResourceCollection real API checks

The runner creates and deletes its own local cluster. It never accepts an
existing kubeconfig or changes the default Kubernetes context. Python uses only
the standard library.

Admission checks apply the receiver's canonical CRD, exercise maximum inputs,
reject invalid selectors and strict unknown fields, and use actual ServiceAccount
tokens to verify declaration and status permissions.
Status admission persists 32 entries with 2048-character Unicode messages and
rejects excessive messages, excessive entries and invalid or duplicate indexes.

Runtime checks start a built agent binary on the host with a fixture
ServiceAccount kubeconfig. They capture actual OTLP JSON logs, check missing/late
APIs, namespace-only resource grants, union/removal, label changes, wildcard
denies, custom-only scope, field-selector behavior and automatic declaration
status export. A complete snapshot must contain each expected object once.
The runner also checks that the collector writes all 32 selection results for the
current generation while permitted resources continue collecting.

Build the candidate using `Dockerfile.agent`, then extract its binary. Run from
the collector repository:

```sh
python3 test/resourcecollection/run.py \
  --stage runtime --engine k3d \
  --node-image 'rancher/k3s@sha256:<qualified-node-digest>' \
  --kubernetes-version '<expected-minor>' \
  --crd receiver/k8sresourcereceiver/resourcecollection/resourcecollection-crd.yaml \
  --agent-binary /path/to/built-agent \
  --artifacts /tmp/resourcecollection-evidence
```

Use `--compact` for compact caching, or `--stage admission` without a candidate
binary. The artifact directory must be empty. Reports bind results to the CRD,
binary, harness hashes and actual API-server version. Temporary credentials are
removed; artifacts contain only synthetic fixture data and logs.

The runtime binary must match the host architecture. This path does not qualify
Helm installation/retention, rollout stability, two-Pod leader failover, image
architecture portability or backend topology.

SUSE BCI base/development catalogs were checked when selecting this harness.
They do not provide a runnable Kubernetes node or k3d helper. The specialized
Rancher K3s node and k3d tool images are the harness exception; the candidate
agent still uses the repository's BCI builder and runtime images. Pin the node
image by digest and verify downloaded cluster-tool binaries against their
published checksums.

## Helm lifecycle and two-Pod takeover

`chart.py` packages the supplied source chart, checks that its CRD matches the
canonical file, and installs its candidate agent image into a new owned k3d
cluster. Other agent workloads are disabled. A host relay accepts the chart's
normal OTLP protocol and records JSON; collector ConfigMaps are not patched for
capture. The report binds the chart archive, canonical CRD, image ID, binary and
harness hashes.

```sh
python3 test/resourcecollection/chart.py \
  --chart /path/to/helm-charts-internal/stable/suse-observability-agent \
  --crd receiver/k8sresourcereceiver/resourcecollection/resourcecollection-crd.yaml \
  --agent-binary /path/to/candidate-agent \
  --agent-image candidate-agent:local \
  --node-image "$RC_NODE_IMAGE" \
  --kubernetes-version "$RC_KUBERNETES_MINOR" \
  --artifacts /tmp/resourcecollection-chart-evidence
```

The node image must be pinned by digest. The local candidate image and binary
must come from the same build. The runner uses restricted chart RBAC plus an
administrator-created exact resource-read grant. It checks selector edits
without ConfigMap, Deployment or Pod changes, replicated peer caches, and leader
replacement while custom-resource reads are revoked. The new leader must retain
inherited objects, report blocked access, and recover after restoring reads.
During takeover, the owned node is temporarily cordoned so the populated follower
gets the lease before a replacement Pod can compete.

Lifecycle checks cover same-source upgrade, feature disable/re-enable, rollback
to the disabled profile, retained declarations after uninstall, and reinstall
with the same release name. Metrics are read through the API proxy because the
chart binds them to the Pod IP. Both peer snapshots must contain unique selected
objects.

The default `--profile native` does not emit snapshot boundaries.

## Topology compatibility

Use the same command with `--profile topology` and a separate empty artifact
directory. This enables the existing chart topology gates, compact caching and
complete snapshot boundaries. The cluster agent has zero replicas; its existing
resource switches configure the native baseline. Events and state metrics are
disabled for this profile.

A local intake sink receives the actual gzip JSON sent by `stsk8stopology`.
It checks producer identity, snapshot start/stop ordering, required lists,
component uniqueness and native fixture identifiers. Pending APIs and blocked
custom reads must preserve native topology. Native ConfigMap deletion must
remove that component while keeping the Secret and Pods. Secret values must
remain sanitized in both export paths.

After revoking native ConfigMap reads, the populated follower takes over.
Incomplete observer snapshots must suppress intake from that new producer for
longer than the exporter's default handover delay. Restoring reads must resume
complete observer and legacy snapshots without restarting that Pod. The
artifact directory includes raw intake and reconstructed completed snapshots.

This profile checks collector output and local intake acceptance. Older
chart/image rollback bundles, installation order between separate releases,
other architectures and platform persistence/presentation require separate
qualification.

## CI

The collector workflow runs the host-binary runtime checks on the two pinned
Kubernetes targets in its matrix, covering standard and compact caches. It
extracts the candidate binary from that run's agent image artifact, verifies
downloaded cluster tools against upstream checksums, and uploads synthetic
evidence on success or failure. The job is required by publication and ci-success.
Receiver race tests and canonical export reproducibility run in the unit-test job.

The separate compatibility gate requires a changed assessment and API regression
when the canonical CRD or wire types change. Its tests use real Git trees and
verify missing, unchanged, empty and deleted evidence fails. Run the same gate
against an uncommitted local candidate with:

```sh
python3 test/resourcecollection/check_compatibility.py \
  --base "$(git rev-parse HEAD)" --worktree
```

This job does not establish the product support floor. Helm lifecycle,
two-Pod takeover, other architectures and backend checks remain separate gates.
