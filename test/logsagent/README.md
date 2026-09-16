# Logs-agent process tests

Standalone Go module for a built agent supplied through `OTEL_AGENT_BINARY`.
Each process uses stock Filelog/container parsing and file storage, a memory
limiter, synthetic pod identity from transform, and local feature/Loki/OTLP HTTP
servers. Requests are decoded as Snappy/protobuf or OTLP protobuf. The legacy
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
GOMAXPROCS, and a synthetic API key. It inherits no credentials or proxies.
Without the binary, omit `-require-agent` to compile and run fixture checks;
process cases explicitly skip. An invalid supplied binary fails.
Debug-level collector logging exposes the connector's bounded completion events;
all fixture records, credentials and endpoints are synthetic.

Both exporters explicitly use C=8 queue slots, W=4 workers, R=2s retries and
T=200ms attempts. Filelog allows four concurrent files and disables receiver
retry. The 25s export lifetime exceeds `ceil(C/W)*(R+T)+20s = 24.4s`.
Both terminal pipelines have no processors, queue batching or payload storage;
checkpoint recreation is disabled. Normal drain tests wait on observed events
and exits, with no lifetime-sized sleeps.

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
| Timer flush with a stopped/resumed child process | Expired export deadline, rejection of remaining fragments and final worker shutdown |
| Record/request size limits | Rejection before export and corresponding counters |

The binary-independent checks also reject 29 omitted authored bound fields and
12 invalid exporter-bound variants through the shared validator, without
Collector default insertion or typed-config marshaling.

Measured locally on 2026-09-16 with Go 1.26.6: the required-binary suite passed
all 122 leaf cases with no skips in 56.729s, including 44 binary-independent
fixture cases. The race detector covers the harness; the supplied agent is a
normal OCB build. Component race tests run separately. The suite timeout is 180s.

Negative startup cases require the intended error category.
Startup checks effective bounds after Collector default insertion; authored
fixtures separately require explicit fields through the strict validator.
Four positive process cases omit a timeout or retry budget and size the
lifetime for the resulting defaults, using immediate-success responses.

Remaining process gaps: forced admission saturation;
canceled queue waiters with workers still running; observation reset and
cooldown expiry; failed marker/message/signal callbacks; checkpoint-save
failure/replay; rotation/truncation and lost responses; native gRPC, TLS and
proxy variants. Kubernetes grace, enrichment, permissions/SELinux, container
replacement, Promtail migration/rollback and resource/throughput tests require
separate fixtures. Fresh-directory tests model lost state, not an actual Pod
replacement. This suite makes no live Receiver or product-acceptance claim.
