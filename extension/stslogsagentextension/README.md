# Logs agent lifecycle extension

`stslogsagent` owns health probes, effective configuration validation and final
export-drain accounting. Its only setting is `health_endpoint` (default
`0.0.0.0:13133`). It makes no Receiver feature requests.

`/live` succeeds while the listener runs. `/ready` succeeds after configuration,
pipelines and the single delivery observer are initialized. Shutdown marks the agent
unready and fixes one absolute drain deadline. The exporter joins admitted calls;
the extension reports the final outcome after exporter shutdown. Failed exports,
late rejection and incomplete shutdown remain visible in counters and logs.

The graph requires one Filelog pipeline exporting directly to `stsk8slogs`.
The exporter's `delivery.controller_extension` points here. Enable
`--feature-gates=stanza.synchronousLogEmitter`. The shared
[logs-agent delivery contract](../../common/logsagent/README.md) documents the
validated pipeline invariants, admission limits, deadlines, retries and drain
semantics.

`common/logsagent` provides shared validation and accounting. Authored fixtures
require explicit bounds; runtime validation handles Collector serialization of
disabled queues and omitted false storage recreation. Neither validation nor
readiness proves Receiver storage or product acceptance.
