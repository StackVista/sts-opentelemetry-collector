# Logs route connector

`stslogsroute` sends logs to the one pipeline selected by the configured
`logsagent.Controller` extension at startup. Both destination pipelines belong
to the Collector lifecycle. The connector does not switch destinations or retry
exports.

| Configuration key | Default |
| --- | --- |
| `capability_extension` | `stslogscapability/logs` |
| `legacy_pipeline` | `logs/legacy` |
| `native_pipeline` | `logs/native` |
| `max_concurrent_calls` | `8` |
| `max_record_bytes` | `262144` |
| `max_request_bytes` | `1048576` |
| `export_lifetime` | `90s` |

Size limits use uncompressed OTLP protobuf sizes. Each record is sized as its
own request including resource, scope and schema metadata. An oversized request
or any oversized record permanently rejects the entire request before export.
Other record validation belongs to the legacy exporter, which can retain valid
siblings.

Resource and scope envelope sizes are computed once per group. Individual
record sizes include their protobuf field and enclosing length prefixes without
revisiting shared metadata.

Admission is nonblocking. Admitted calls retain caller context values but have
an independent export lifetime. During drain, new calls must fit the controller's
validated queue/retry bound before its absolute deadline. Existing call
deadlines stay unchanged. Cross-pipeline validation belongs to the controller.

Metrics use only bounded `mode`, `reason`, `outcome` and `draining` attributes:

- `stslogsroute.pre_export_rejected_requests` and
  `stslogsroute.pre_export_rejected_records`: size, admission, drain or lifecycle
rejection, separated by reason.
- `stslogsroute.oversized_records`: individual oversized records in requests
  that fit the request limit.
- `stslogsroute.export_requests`: acknowledged, permanent rejection, exhausted
  retry budget, expired export deadline or unclassified terminal error.
- `stslogsroute.outstanding_requests`: synchronous connector calls.

Acknowledgement counts requests, including successful partial responses; it
does not assert that every record was stored. A queue waiter may return before
its exporter worker finishes. The registered observer and connector shutdown
account for calls only; the controller finalizes draining after exporter
shutdown.

Every pre-export rejection observed during drain is included in the controller's
`DrainRejected` count. DEBUG messages `Logs export completed` and
`Logs export rejected` expose only outcome, mode, drain state and record count.
They contain no payload or downstream error text.

Run from this directory:

```sh
GOWORK=off GOCACHE=/tmp/otel-logs-go-cache go test -race -count=1 -timeout=180s ./...
```
