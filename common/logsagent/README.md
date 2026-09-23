# Logs agent delivery

`logsagent` provides bounded synchronous delivery for the Filelog agent. It
admits a complete export only when a call slot is available, applies record and
request OTLP protobuf-size limits before export, and accounts for its final
outcome. Resource and scope metadata contribute to each record's size. Any
request or record exceeding its bound permanently rejects the entire call;
saturation and drain-budget rejection remain retryable.

| Field | Meaning |
| --- | --- |
| `controller_extension` | Enabled `stslogsagent` extension that coordinates delivery. |
| `max_concurrent_calls` | Maximum admitted synchronous exports. |
| `max_record_bytes` | Maximum encoded OTLP size of one record and its resource/scope overhead. |
| `max_request_bytes` | Maximum encoded OTLP size of the complete export request. |
| `export_lifetime` | Maximum duration for one admitted export, including retries. |

Fixed Promtail configuration can export Filelog directly to `stsk8slogs`.
Capability-routed configuration keeps this delivery in `stslogsroute`, which
selects exactly one terminal exporter. A pipeline must use one delivery owner,
never a route plus exporter `delivery` wrapper. Both forms use checkpoint
storage without recreation, disable Filelog retry and exporter queues, and
permit only synchronous processors. `max_concurrent_calls` must cover Filelog
concurrency plus two additional calls. See the
[fixed fixture](../../test/validate/configs/promtail-logs-agent.yaml) and
[routed fixture](../../test/validate/configs/logs-agent.yaml).

Each admitted export receives a fresh deadline from `export_lifetime`, separate
from cancellation of its caller. Exporter-helper owns bounded export retries;
`export_lifetime` must cover its retry window, attempt timeout, and shutdown
allowance. The `stslogsagent` extension owns readiness, the absolute drain
deadline, and final drain accounting. During shutdown, existing calls retain
their deadline while new calls are rejected when there is insufficient time for
the retry bound. The exporter joins admitted calls before stopping
exporter-helper's retry sender. Detaching caller cancellation prevents receiver
shutdown from canceling an admitted export before its bounded completion.

Delivery records rejected inputs, oversized records, outstanding calls, and
completed outcomes. Drain completion is reported as completed, failed, or
incomplete. There is no durable payload queue: Filelog checkpoints track read
progress, not backend acknowledgement. Bounded retries can therefore drop an
export, and a failed response after a partial backend write can be retried and
produce duplicates. Readiness or successful configuration validation does not
prove Receiver storage or product acceptance.
