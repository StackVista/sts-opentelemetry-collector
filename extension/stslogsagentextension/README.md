# Logs agent lifecycle extension

`stslogsagent` owns fixed Promtail delivery, health probes, effective configuration
validation and final export-drain accounting. Its only setting is `health_endpoint`
(default `0.0.0.0:13133`). It makes no Receiver feature requests.

`/live` succeeds while the listener runs. `/ready` succeeds after configuration,
pipelines and the single delivery observer are initialized. Shutdown marks the agent
unready and fixes one absolute drain deadline. The exporter joins admitted calls;
the extension reports the final outcome after exporter shutdown. Failed exports,
late rejection and incomplete shutdown remain visible in counters and logs.

The graph requires one Filelog pipeline exporting directly to `stsk8slogs`.
The exporter's `delivery.controller_extension` points here. Enable
`--feature-gates=stanza.synchronousLogEmitter`. Queues and Filelog retries must be
disabled; exporter retries and attempts must be finite. Admission must allow at
least `max_concurrent_files + 2` calls, and `export_lifetime` must cover retry elapsed
time plus attempt timeout plus 20 seconds. Record/request bounds and file-storage
checkpoint preservation are validated independently of Collector defaults.

`common/logsagent` provides shared validation and accounting. Authored fixtures
require explicit bounds; runtime validation handles Collector serialization of
disabled queues and omitted false storage recreation. Neither `validate` alone nor
readiness proves Receiver storage or product acceptance.
