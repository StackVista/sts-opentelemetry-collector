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
| `compat_action` | the operations the collector pipeline has to apply, see below |
| `labels` | label keys actually present on the stored series, from a live dump |
| `consumers` | artifacts that reference the metric, by identifier or local file path |
| `priority` | `P1` when something references it, `P3` when consumer extraction completed and found nothing, `unknown` while that evidence is missing |
| `origin` | `scanned` from agent source, or `supplemental` because the name is built at runtime |
| `notes` | anything a reviewer needs in order to disagree with the row |

`compat_action` is a `+` joined list of operations, because several metrics need more than
one. The primitives:

| operation | meaning |
| --------- | ------- |
| `rename` | the name changes |
| `split` | one OTel metric with an attribute becomes several Agent V2 names |
| `scale` | a numeric factor or unit conversion, for example cores to nanocores |
| `rate` | OTel ships a cumulative counter, Agent V2 ships a per-second rate |
| `derive` | arithmetic over one or more OTel metrics |
| `no-source` | nothing in the chosen receivers produces it |
| `undecided` | needs a decision, usually one recorded in the migration document |

So `split+rate` means split by attribute and convert a cumulative counter to a rate, and
`rename+scale` means rename and apply a factor. Units in the `unit` column are the Agent V2
units as submitted, which are not always the natural OTel unit: the memory and swap checks
divide bytes by 1024^2, and the disk checks divide by 1024, so those series are MiB and KiB
rather than bytes.

## Producing it

Five inputs feed the builder: three offline that always work, and two that need an instance.

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
literals in the kubelet, container and host checks. It resolves the shapes the agent uses:
whole literals, concatenation onto a prefix constant such as `KubeletMetricsPrefix`,
`fmt.Sprintf` against a format constant such as `diskMetric = "system.disk.%s"`, and bare
suffixes handed to a submission helper or sitting in a metric name map. A bare suffix is only
prefixed when one of those contexts applies, so unrelated dotted strings in the tree do not
become metrics.

Two families append a suffix at submission time and cannot be seen as literals: the protocol
counters that also emit a `.count` series, and the histogram-derived kubelet durations that
emit `.sum` and `.count`. Both are expanded from the map holding the base names, so the
expansion follows the source when names are added or removed.

`mapping/supplemental-metrics.csv` adds the names the agent assembles at runtime, which no
scanner can see: the container state reason fan-out, the per-resource requests and limits,
the probe and SLI names, the node filesystem prefixes and the system container prefixes.
`mapping/otel-sources.csv` is the hand-maintained part, one row per metric or glob saying
where the data can come from and what the pipeline has to do to it.

`scripts/build-contract.sh` joins all of that and prints a summary. Rerunning it is safe and
idempotent; `generated/` is not committed because the live half is instance-specific.

The two instance-derived scripts differ in how their absence is treated. `dump-live-series.sh`
queries the public metrics API at `/api/metrics/series` by default, overridable with
`STS_METRICS_PATH`, and takes `LOOKBACK` as a duration such as `30m` or `2d`.
`extract-stackpack-queries.sh` reads setting bodies with `sts settings describe` rather than
`sts settings list`, which only returns a summary table, and it includes `MetricBinding`
because the Kubernetes metrics tab queries live there. It writes `generated/queries.status`
only when every setting was exported successfully, and the builder uses that to decide
whether an empty `consumers` value means "no consumers" or "not looked at yet".

## What the current contract says

From the offline inputs alone, 383 metrics:

| compat action | count |
| ------------- | ----- |
| `no-source` | 161 |
| `rename` | 66 |
| `undecided` | 42 |
| `derive` | 35 |
| `split` | 20 |
| `split+scale` | 16 |
| `split+rate` | 16 |
| `rename+rate` | 14 |
| `rename+scale` | 11 |
| `split+rate+scale` | 2 |

By family: 223 `system.*`, 111 `kubernetes.*`, 49 `container.*`.

The difficulty is concentrated in two places: the 161 with no source, nearly all of them
`system.net.*` protocol counters, their `.count` siblings, and the `/proc` details listed in
section 7.9 of the migration document, and the 42 undecided, nearly all of them pod-spec
derived and waiting on NA-8. Everything else is mechanical, and the most common mechanical
combination is a split by attribute followed by a rate conversion.

## Naming rules, settled

`naming/` runs the remote write translator and the prometheus receiver against a matrix of
input shapes and records the result in two golden files. That is NA-2, and it is a test rather
than a note because the translation strategy is version dependent and the exporter is already
moving from `add_metric_suffixes` to `translation_strategy`.

What it establishes:

| finding | consequence |
| ------- | ----------- |
| The default appends unit and type suffixes: `kubernetes.memory.usage` (`By`) becomes `kubernetes_memory_usage_bytes`, and `container.cpu.time` (`s`, monotonic sum) becomes `container_cpu_time_seconds_total` | The default cannot produce the Agent V2 names, so the agent would have to emit gauges with empty units purely to control naming |
| `translation_strategy: UnderscoreEscapingWithoutSuffixes` escapes dots and dashes to underscores and appends nothing | This reproduces the Agent V2 shape from the natural names and units, so the agent can carry honest units and types |
| An already underscored monotonic sum is passed through unchanged by every strategy | There is no double `_total` problem to work around |
| The two UTF-8 strategies need remote write 2.0, which the platform does not run, and they preserve dots | They are not available, and if they were they would break the contract, because the store holds underscores |
| A `metric_relabel_configs` rule can write a dotted `__name__` and it survives to OTLP | Renames can live in relabel rules rather than OTTL |
| A dotted name cannot arrive through exposition, it is dropped before relabeling | Scrape targets must keep exposing underscored names, which they do |
| Attribute keys are escaped the same way, so `k8s.pod.name` becomes the label `k8s_pod_name` | Label compatibility has to be done agent side, as section 3.1 of the migration document already says |

The decision that follows: the agent emits dotted names with their real units and types, and
the platform sets `translation_strategy: UnderscoreEscapingWithoutSuffixes` on the server side
`prometheusremotewrite/victoria-metrics` exporter. This is why `stored_name` in `contract.csv`
is a plain dots and dashes to underscores transform, with no suffix handling: that is the
behaviour the naming test pins down.

One thing this cannot check, because it is a different repository: that the deployed collector
actually sets that strategy. Close that with a helm unittest in the chart repository asserting
the rendered config contains it.

## Two things this cannot answer yet

**Label spelling.** The `labels` column is empty until someone runs `dump-live-series.sh`
against an instance that is still receiving Agent V2 data. That script prints a frequency
count of label keys seen on container-level series, which is what settles whether the stored
label is `kube_namespace` or `namespace`, and `pod_name` or `pod`. Both the node-agent and
the cluster-agent migrations depend on the answer, so it belongs on the epic, not just in
this file.

**What drop-in replacement means.** The `consumers` column decides whether a metric with no
consumer still has to be reproduced. It is only trustworthy once
`extract-stackpack-queries.sh` has run to completion against an instance, which is why the
builder reports `priority` as `unknown` rather than `P3` until then, and why seeing live
series is not treated as evidence of anything. This is open question 9 in the migration
document.

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
