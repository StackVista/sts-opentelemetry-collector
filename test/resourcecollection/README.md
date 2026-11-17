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
two-Pod takeover and backend acceptance are deferred to BEEST qualification.
They are outside this component test suite.
