# Logs agent delivery

Delivery controls admission and completion of synchronous Filelog exports.
Its purpose is to let admitted exports finish during receiver shutdown while
keeping their concurrency, exported chunk size and execution time bounded.

## Properties

- **Completion is synchronous.** An admitted call returns after the downstream
  exporter finishes, including its retries. Returning success does not merely
  mean that work was placed in a queue.
- **Admitted work is bounded.** At most `max_concurrent_calls` exports run
  concurrently. Saturation rejects a call immediately instead of building a
  waiting queue. Oversized records are dropped individually; their healthy
  neighbours remain eligible for export. Larger batches are split into bounded
  chunks and sent sequentially. Record size includes resource and scope metadata.
  Splitting builds one chunk at a time and conservatively counts shared metadata
  per record. Fitting batches without oversized records keep the direct path.
  The original input remains in memory: these limits bound exported chunks,
  not admitted input size or total process memory. Filelog and container
  recombination limits remain necessary.
- **Receiver cancellation does not abandon admitted work.** Delivery preserves
  context values but replaces the caller's cancellation and deadline with its
  own deadline. A receiver stopping during an HTTP request therefore does not
  cancel that export before it can finish within its delivery budget.
- **Retries have one owner and a finite budget.** Exporter-helper owns retries;
  delivery does not retry the call again. Each chunk uses exporter-helper retries
  within the original call's single absolute deadline. A terminal chunk failure
  or expired deadline stops further sends; previously acknowledged chunks are
  never replayed by delivery. The validated retry window, attempt
  timeout and shutdown allowance must fit within `export_lifetime`. The
  downstream exporter receives an absolute deadline and must honor it.
- **Shutdown cannot keep extending the drain.** The `stslogsagent` extension
  clears readiness and fixes one absolute drain deadline. Already admitted
  calls retain their deadlines. Late calls are admitted only when the remaining
  drain budget covers the retry bound, and their deadlines cannot exceed the
  drain deadline. Delivery shutdown then closes admission and waits for
  admitted calls before exporter-helper's retry sender is stopped.
  A canceled exporter shutdown still joins calls under their existing deadlines
  and runs cleanup once; its cancellation and any cleanup error are returned.
- **The final result remains visible.** Every admitted call is accounted for
  until it returns; rejected calls are counted separately. The final drain
  report considers failures, rejections, outstanding calls and exporter status.
  Zero outstanding calls alone is insufficient to report a successful drain.
  `stslogsagent.export_records` distinguishes completed, failed, dropped and
  unsent records. Completed counts records in successful downstream calls;
  exporter validation can still filter records and reports its own drop metrics. A call with any oversized drops returns a permanent error after
  sending its valid records, unless an export error or deadline interrupts it.
  Dropped records therefore cannot make a partially successful call appear fully
  acknowledged.

## Required pipeline and configuration

The direct agent pipeline is:

```text
Filelog → synchronous processors → Delivery → exporter-helper → HTTP sender
```

Filelog retries, exporter queues and asynchronous processors are disabled so
that this completion accounting covers the complete export. Checkpoint storage
must not silently recreate corrupt state. `max_concurrent_calls` must cover
Filelog concurrency plus two additional calls. The validated
[agent fixture](../../test/validate/configs/logs-agent.yaml) shows the required
shape and explicit bounds.

| Field | Meaning |
| --- | --- |
| `controller_extension` | Enabled `stslogsagent` extension that coordinates delivery. |
| `max_concurrent_calls` | Maximum admitted synchronous exports. |
| `max_record_bytes` | Maximum encoded OTLP size of one record and its resource/scope overhead. |
| `max_request_bytes` | Maximum encoded OTLP size of each exported chunk. |
| `export_lifetime` | Maximum duration for one admitted export, including retries. |

## Limits of these properties

`acknowledged` means that the synchronous exporter returned success within the
delivery deadline. It does not establish durable Receiver storage or prove that
every input record was accepted by the backend.

There is no durable payload queue or guarantee of loss-free delivery. Oversize
records, exhausted retries and admission or drain rejection can lose records.
Although saturation and drain rejection return retryable errors, the configured
Filelog receiver does not retry them.

There is no exactly-once guarantee. Retrying after a lost response or partial
backend write can duplicate records. Filelog checkpoints track read progress,
not backend acknowledgement, and replay can also duplicate records. A successful
drain does not establish checkpoint persistence.

[Delivery tests](delivery_test.go) exercise admission, cancellation, deadlines
and shutdown; [exporter-helper tests](delivery_helper_test.go) exercise the retry
outcomes observed by delivery.
