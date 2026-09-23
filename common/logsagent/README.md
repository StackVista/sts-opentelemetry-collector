# Logs agent delivery

Delivery controls admission and completion of synchronous Filelog exports.
Its purpose is to let admitted exports finish during receiver shutdown while
keeping their concurrency, input size and execution time bounded.

## Properties

- **Completion is synchronous.** An admitted call returns after the downstream
  exporter finishes, including its retries. Returning success does not merely
  mean that work was placed in a queue.
- **Admitted work is bounded.** At most `max_concurrent_calls` exports run
  concurrently. Saturation rejects a call immediately instead of building a
  waiting queue. Request and record size checks reject the entire call before
  export if either limit is exceeded. Record size includes resource and scope
  metadata. These limits bound admitted inputs, not total process memory.
- **Receiver cancellation does not abandon admitted work.** Delivery preserves
  context values but replaces the caller's cancellation and deadline with its
  own deadline. A receiver stopping during an export request therefore does not
  cancel that export before it can finish within its delivery budget.
- **Retries have one owner and a finite budget.** Exporter-helper owns retries;
  delivery does not retry the call again. The validated retry window, attempt
  timeout and shutdown allowance must fit within `export_lifetime`. The
  downstream exporter receives an absolute deadline and must honor it.
- **Shutdown cannot keep extending the drain.** The `stslogsagent` extension
  clears readiness and fixes one absolute drain deadline. Already admitted
  calls retain their deadlines. Late calls are admitted only when the remaining
  drain budget covers the retry bound, and their deadlines cannot exceed the
  drain deadline. Delivery shutdown then closes admission and waits for
  admitted calls before exporter-helper's retry sender is stopped.
- **The final result remains visible.** Every admitted call is accounted for
  until it returns; rejected calls are counted separately. The final drain
  report considers failures, rejections, outstanding calls and exporter status.
  Zero outstanding calls alone is insufficient to report a successful drain.

## Required pipeline and configuration

The fixed Promtail pipeline is:

```text
Filelog → synchronous processors → Delivery → exporter-helper → protocol sender
```

Capability-routed configuration inserts `stslogsroute` after the synchronous
processors. It owns the shared delivery and selects one Promtail or native OTLP
terminal exporter. Direct configuration gives delivery to `stsk8slogs`; a
pipeline must never use both delivery wrappers.

Filelog retries, exporter queues and asynchronous processors are disabled so
that this completion accounting covers the complete export. Checkpoint storage
must not silently recreate corrupt state. `max_concurrent_calls` must cover
Filelog concurrency plus two additional calls. The
[fixed fixture](../../test/validate/configs/promtail-logs-agent.yaml) and
[routed fixture](../../test/validate/configs/logs-agent.yaml) show the required
shape and explicit bounds.

| Field | Meaning |
| --- | --- |
| `controller_extension` | Enabled `stslogsagent` extension that coordinates delivery. |
| `max_concurrent_calls` | Maximum admitted synchronous exports. |
| `max_record_bytes` | Maximum encoded OTLP size of one record and its resource/scope overhead. |
| `max_request_bytes` | Maximum encoded OTLP size of the complete export request. |
| `export_lifetime` | Maximum duration for one admitted export, including retries. |

## Limits of these properties

`acknowledged` means that the synchronous exporter returned success within the
delivery deadline. It does not establish durable Receiver storage or prove that
every input record was accepted by the backend.

There is no durable payload queue or guarantee of loss-free delivery. Oversize
inputs, exhausted retries and admission or drain rejection can lose records.
Although saturation and drain rejection return retryable errors, the configured
Filelog receiver does not retry them.

There is no exactly-once guarantee. Retrying after a lost response or partial
backend write can duplicate records. Filelog checkpoints track read progress,
not backend acknowledgement, and replay can also duplicate records. A successful
drain does not establish checkpoint persistence.

[Delivery tests](delivery_test.go) exercise admission, cancellation, deadlines
and shutdown; [exporter-helper tests](delivery_helper_test.go) exercise the retry
outcomes observed by delivery.
