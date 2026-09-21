# Logs-agent process tests

Standalone Go module for a built agent supplied through `OTEL_AGENT_BINARY`.
Each process uses stock Filelog/container parsing and file storage, a memory
limiter, synthetic pod identity from transform, and local feature/Loki/OTLP HTTP
and gRPC servers. Requests are decoded as Snappy/protobuf or OTLP protobuf. The legacy
descriptor mirrors the existing exporter's Receiver wire fixture.
Authored configurations are checked directly with the shared
`ValidatePipelineConfig`; this test module resolves `common` from `../../common`.

Run synchronously from this directory with Go 1.26.6:

```sh
GOWORK=off GOTOOLCHAIN=local \
  OTEL_AGENT_BINARY=/absolute/path/to/sts-opentelemetry-agent-collector \
  go test -race -count=1 -timeout=180s -v ./... -require-agent
```

The harness passes `--feature-gates=stanza.synchronousLogEmitter`. The child
environment contains only temporary HOME/TMPDIR, a fixed system PATH,
GOMAXPROCS, and a synthetic API key. Proxy tests add explicit synthetic proxy
settings; the child inherits no credentials or proxies.
Without the binary, omit `-require-agent` to compile and run fixture checks;
process cases explicitly skip. An invalid supplied binary fails.
Debug-level collector logging exposes the connector's bounded completion events;
all fixture records, credentials and endpoints are synthetic.

Both exporters explicitly disable queues. The connector admits C=8 calls,
with R=2s retries and T=200ms attempts. Filelog allows four concurrent files and
disables receiver retry. The 25s export lifetime exceeds `max(R+T)+20s = 22.2s`.
Both terminal pipelines have no processors or payload storage;
checkpoint recreation is disabled. Normal drain tests wait on observed events
and exits. CRI recombination serializes some calls before admission; the bound
does not promise a full retry budget for every buffered fragment during drain.

| Process coverage implemented | Evidence asserted |
| --- | --- |
| Both routes: success, transient recovery, outage, HTTP 401/403/404/413 | Decoded bodies/identity/timestamps, attempts, fixed destination, terminal result and exit |
| Native partial success; legacy invalid sibling | No whole-request retry after partial success, rejection reporting, valid sibling delivery |
| SIGTERM during retries, both routes, recovery/outage | Real signal, unready/live during drain, accepted bodies, bounded exit, finalized exporter shutdown |
| Both capability transitions during retries, recovery/outage | Old route through shutdown, persisted intent/message, replacement discovery, retained offsets with no old-record replay |
| Four files with transient failures | All 40 records delivered once after recovery |
| Fresh checkpoint directory | Exactly one replay of each available record |
| Feature fallback and authentication | Startup fallback/rejection; runtime readiness recovers with route fixed |
| Seeded pending restart intent/cooldown | Discovery overrides intent; cooldown timestamp survives and prevents another immediate restart |
| Corrupt controller/checkpoint files | Failed startup without collection or silent state replacement |
| Invalid lifetime, concurrency, queue, retry and synchronous gate | Real startup rejection; each terminal exporter's bounds are challenged |
| Omitted exporter timeout/retry settings | Insufficient effective lifetime rejected; sufficient lifetime permits startup, immediate-success export and shutdown |
| Sequential partial-CRI shutdown during outage | One absolute drain budget, completed retries and late rejection, exact final counters after exporter shutdown |
| Timer flush with a stopped/resumed child process | Expired export deadline, rejection of remaining fragments and final export shutdown |
| Record/request size limits | Rejection before export and corresponding counters |
| Capability observation reset, both directions | Current-mode, malformed, authentication, transient and unsupported queries interrupt the candidate sequence; authentication readiness recovers |
| Failed restart marker/message writes | Old route remains usable; cooldown expires before three fresh observations request one successful restart |
| Checkpoint-write failure, both routes | Upstream save error, continued delivery, completed export drain and replay from the last saved checkpoint |
| Lost responses, both routes | Stored records duplicated by one retry; a subsequent restart retains the checkpoint |
| Timestamped rename/copytruncate, both routes | Default fingerprint size; rotation while running or stopped; growth beyond the old offset; restart without missing or duplicate records |
| Repeated fingerprint after truncation | Default retains the old offset and reports at debug level; explicit `read_whole_file` reads the shorter file |
| Native gRPC success and partial success | Decoded records, authorization and rejection reporting without whole-request retry |
| Custom CA, missing CA and proxy variants | Trust/rejection for discovery and both exports; explicit HTTP proxy and gRPC HTTPS_PROXY/NO_PROXY behavior |
| Injected admission saturation, both routes | Eight synchronous calls hold completion; the ninth call is counted and rejected before export |
| Injected completion delay, both routes | Backend acceptance precedes deadline expiry; connector and final failed drain wait for the admitted call to return |
| Injected signal callback failure, both directions | Persisted intent/message, restored readiness, fixed route, cooldown suppression and exactly one real SIGTERM after three fresh observations |

The binary-independent checks reject omitted authored bound fields and
invalid exporter-bound variants through the shared validator, without
Collector default insertion or typed-config marshaling.

The ordinary suite uses a normal OCB build and runs the harness with the race
detector. The injected cases use a separate agent built with the race detector.
Component race tests run separately. The suite timeout is 180s.

Build and run the injected cases from the repository root:

```sh
go run ./test/logsagent/cmd/faultagent -output /tmp/otel-agent-faults
OTEL_AGENT_FAULT_BINARY=/tmp/otel-agent-faults \
  go test -race -count=1 -timeout=180s -run '^TestInjected' \
  ./test/logsagent -require-fault-agent
```

The helper generates the agent from its normal BOM and builds with Go source
overlays under a temporary directory. `-build-dir` can reuse an existing
OCB-generated directory. Exact-match patches fail if their source anchors change.
Repository files, the module cache and the published binary are not modified.
CI requires both the ordinary suite and a separate fault-injection job.

The overlays add file barriers controlled by the parent fixture. Admission
testing fans one parsed Filelog record into eight numbered concurrent calls to
the real connector, then submits the original as the excess call. This is forced
load, not evidence that ordinary Filelog exceeds its configured concurrency.
Completion testing pauses an admitted synchronous call after export returns and
before connector accounting finishes. It leaves the real context deadline,
receiver and Collector shutdown running. Final reporting fails the test binary
if any instrumented call has not finished. Signal testing substitutes the existing
restart callback and allows its real SIGTERM implementation after fault removal.
These hooks exist only in the separate test binary.

Negative startup cases require the intended error category.
Startup checks effective bounds after Collector default insertion; authored
fixtures separately require explicit fields through the strict validator.
Four positive process cases omit a timeout or retry budget and size the
lifetime for the resulting defaults, using immediate-success responses.

The realistic rotation cases use the upstream default fingerprint size and
changing CRI timestamps. They acknowledge the initial records before rotating,
so they do not test a writer racing the copy/truncate operation. A separate
edge fixture uses repeated timestamps and a 32-byte fingerprint to exercise
both truncation policies. The shared configuration keeps the upstream default.

Checkpoint-write faults use Linux `prlimit` on the temporary child process.
The gRPC NO_PROXY case needs a local non-loopback IPv4 interface because the
underlying proxy library already bypasses loopback.

The opt-in Linux comparison uses the same harness for a queued baseline and the
synchronous candidate:

```sh
OTEL_COMPARISON_DESIGN=synchronous OTEL_AGENT_BINARY=/path/to/candidate \
  GOMAXPROCS=4 go test -count=1 -v -run '^TestQueueComparison$' -timeout=180s
OTEL_COMPARISON_DESIGN=queued OTEL_AGENT_BINARY=/path/to/queued-baseline \
  GOMAXPROCS=4 go test -count=1 -v -run '^TestQueueComparison$' -timeout=180s
```

The queued option restores the baseline fixture's eight request slots and four
workers only in the test harness. Run designs sequentially, alternating order,
for at least three repetitions. `QUEUE_COMPARISON` reports numbered-record
delivery/loss/duplicates, throughput, request concurrency, child CPU/peak RSS,
undelivered records at approximately 500ms and final drain counters.
Inputs cover healthy CRI traffic, delayed CRI/Docker responses, recovery after
one second, retry exhaustion and records near the size limit. Docker saturation
uses six concurrent files; all other cases use four. Both designs retain the same
25s lifetime and child GOMAXPROCS=2. These synthetic loads are comparative
measurements, not an owner-approved capacity target.

Kubernetes grace, enrichment, permissions/SELinux, container
replacement and Promtail migration/rollback require
separate fixtures. Fresh-directory tests model lost state, not an actual Pod
replacement. This suite makes no live Receiver or product-acceptance claim.
