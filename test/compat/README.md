# Agent V2 metrics contract

This directory answers one question: if the node-agent's metric collection moves to an
OpenTelemetry Collector, which series have to keep working, and what has to happen to each
one for that to be true.

It is the output of NA-1 in the node-agent migration plan, and the input to the diff harness
in NA-10. `contract.csv` is the artifact; everything else here exists to produce and refresh
it.

## contract.csv

One row per metric name the Agent V2 node-agent can emit.

| column | meaning |
| ------ | ------- |
| `metric` | dotted name as the agent submits it |
| `stored_name` | name as stored, dots and dashes become underscores |
| `check` | which check emits it: `kubelet`, `container`, `system` |
| `kind` | `gauge`, `rate`, `count`, `monotonic_count`, `service_check`, `unknown` |
| `unit` | unit of the Agent V2 series, so the harness can compare like with like |
| `otel_source` | receiver and metric that can carry the data, or `none` |
| `compat_action` | what the collector pipeline has to do, see below |
| `labels` | label keys actually present on the stored series, from a live dump |
| `consumers` | artifacts that reference the metric, from installed settings |
| `priority` | `P1` when something references it, `P3` when nothing does, `unknown` before a live dump |
| `origin` | `scanned` from agent source, or `supplemental` because the name is built at runtime |
| `notes` | anything a reviewer needs in order to disagree with the row |

`compat_action` values:

| value | meaning |
| ----- | ------- |
| `rename` | same value, different name |
| `rename+scale` | same quantity, different unit or factor, for example cores to nanocores |
| `rename+rate` | OTel ships a cumulative counter, Agent V2 ships a per-second rate |
| `split-attribute` | one OTel metric with an attribute becomes several Agent V2 names |
| `derive` | arithmetic over one or more OTel metrics |
| `no-source` | nothing in the chosen receivers produces it |
| `undecided` | needs a decision, usually one recorded in the migration document |

## Producing it

Two of the four inputs are offline and always work. Two need an instance.

```shell
# offline: inventory from an agent checkout, then the contract
./scripts/extract-agent-metrics.sh -a ~/projects/stackstate-agent
./scripts/build-contract.sh

# with an instance: real label sets and real consumers, then rebuild
STS_URL=https://your-instance STS_API_TOKEN=... ./scripts/dump-live-series.sh
./scripts/extract-stackpack-queries.sh
./scripts/build-contract.sh
```

`scripts/extract-agent-metrics.sh` scans a `stackstate-agent` checkout for metric name
literals in the kubelet, container and host checks. It resolves the three shapes the agent
uses: whole literals, concatenation onto a prefix constant such as `KubeletMetricsPrefix`,
and `fmt.Sprintf` against a format constant such as `diskMetric = "system.disk.%s"`.
`mapping/supplemental-metrics.csv` adds the names the agent assembles at runtime, which no
scanner can see: the container state reason fan-out, the per-resource requests and limits,
the probe and SLI names, the node filesystem prefixes and the system container prefixes.
`mapping/otel-sources.csv` is the hand-maintained part, one row per metric or glob saying
where the data can come from and what the pipeline has to do to it.

`scripts/build-contract.sh` joins all of that and prints a summary. Rerunning it is safe and
idempotent; `generated/` is not committed because the live half is instance-specific.

## What the current contract says

From the offline inputs alone, 317 metrics:

| compat action | count |
| ------------- | ----- |
| `no-source` | 105 |
| `rename` | 63 |
| `split-attribute` | 56 |
| `undecided` | 42 |
| `derive` | 33 |
| `rename+rate` | 14 |
| `rename+scale` | 4 |

By family: 167 `system.*`, 101 `kubernetes.*`, 49 `container.*`.

So about two thirds of the surface is mechanical, and the difficulty is concentrated in two
places: the 105 with no source, nearly all of them `system.net.*` protocol counters and the
`/proc` details listed in section 7.9 of the migration document, and the 42 undecided, nearly
all of them pod-spec derived and waiting on NA-8.

## Two things this cannot answer yet

**Label spelling.** The `labels` column is empty until someone runs `dump-live-series.sh`
against an instance that is still receiving Agent V2 data. That script prints a frequency
count of label keys seen on container-level series, which is what settles whether the stored
label is `kube_namespace` or `namespace`, and `pod_name` or `pod`. Both the node-agent and
the cluster-agent migrations depend on the answer, so it belongs on the epic, not just in
this file.

**What drop-in replacement means.** The `consumers` column decides whether a metric with no
consumer still has to be reproduced. Until it is populated, `priority` stays `unknown` and
the compatibility work has no defensible stopping point. This is open question 9 in the
migration document.

## Caveats worth knowing before trusting a row

`kind` is a heuristic. The extractor attributes a submission token to a name literal within
three lines, which is right for the providers in this tree but not guaranteed. Any row whose
`compat_action` involves a rate should have its `kind` confirmed against the source before
the harness treats a mismatch as a bug.

`otel_source` names the receiver that can carry the data, not a configuration that exists
yet. Several rows assume optional metrics are enabled, for example the kubeletstats system
container group and `container.uptime`.

The `container.*` family is included for completeness and is largely redundant with
`kubernetes.*`, since both read the same cgroup counters by different routes. If the answer
to open question 9 is that nothing queries it, 49 rows leave the contract at once.
