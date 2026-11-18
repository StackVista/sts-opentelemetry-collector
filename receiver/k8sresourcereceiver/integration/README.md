# ResourceCollection Go/K3s integration tests

These tests run the receiver through its public factory with an in-process logs
sink and a real Kubernetes API. Testcontainers starts and removes an owned K3s
container. The tests never use an existing cluster or change the default context.

Admission tests install the canonical CRD, exercise maximum inputs and Unicode
status limits, reject invalid selectors and unknown fields, and check that rejected
updates preserve stored data. ServiceAccount tokens verify declaration and status
permissions, including namespace-only delegation and revoked status access.

Receiver tests cover missing and late APIs, namespace-only read grants, overlapping
selections, removal, label changes, wildcard denies, custom-resource-only scope,
field selectors, automatic declaration/status export and all 32 selection results.
They also verify watch-budget blocking, exact sharing at capacity, and automatic
recovery after another selection releases its watch.
A complete snapshot must contain each expected custom resource once.
Permission recovery uses an explicit selection edit after a read grant.
Unchanged-watch retry timing remains covered by the receiver's fake-clock tests.

Run from this directory with Docker available:

```sh
mkdir -p /tmp/resourcecollection-evidence
RC_NODE_IMAGE='rancher/k3s@sha256:<qualified-node-digest>' \
RC_KUBERNETES_MINOR='<expected-minor>' \
RC_COMPACT=false \
RC_ARTIFACTS=/tmp/resourcecollection-evidence \
GOWORK=off go test -race -tags=integration -count=1 -timeout=15m -v ./...
```

Use `RC_COMPACT=true` for compact caching. The workflow contains the qualified
node digests and API-server minors. `RC_ARTIFACTS` is optional; create a fresh directory
for each run. Evidence contains synthetic objects, receiver logs and cluster
events. Temporary kubeconfigs contain only the test ServiceAccount token and
are removed after the test.

CI runs this separate Go module on both pinned Kubernetes targets, with standard
and compact caches. The job remains required by publication and `ci-success`.
Normal agent image builds, configuration validation, receiver race tests and
canonical CRD export verification run in their existing jobs.

SUSE BCI catalogs do not provide a runnable Kubernetes node or the Testcontainers
resource reaper. Those specialized containers are the test exception.
The agent's builder and runtime still use BCI images.

Helm installation/retention, rollout, two-Pod takeover and backend topology
acceptance are deferred to BEEST. They have not been implemented there yet.
This suite does not establish the product's Kubernetes support floor.
Schema compatibility checks against a released CRD follow the first API release.
