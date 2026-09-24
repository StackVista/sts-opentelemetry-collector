# Logs-agent process tests

The harness runs a supplied agent with Filelog/container parsing, file storage,
synthetic pod identity and a local Promtail-compatible backend. Requests are decoded
from Snappy/protobuf. Any feature request or native delivery fails the test.

```sh
OTEL_AGENT_BINARY=/absolute/path/to/sts-opentelemetry-agent-collector \
  go test -race -count=1 -timeout=180s -v ./test/logsagent/... -require-agent
```

The child uses `stanza.synchronousLogEmitter`, explicit synthetic credentials and
an isolated environment. Queues and Filelog retries are disabled. The fixture
admits eight calls for four files; its lifetime covers bounded synchronous retries,
attempt timeout and shutdown overhead. Negative startup cases exercise the actual
configuration lifecycle, beyond `collector validate`.

Coverage includes successful and failed exports, SIGTERM during retries,
concurrent files, corrupted checkpoints, checkpoint-write failure and replay,
rotation/copytruncate, CRI partial records, Docker parsing, payload limits,
late drain rejection, expired deadlines, custom CA trust and explicit proxy use.
Lost-response tests expose possible duplicates after backend acceptance.
Fresh checkpoint directories model state loss on Pod replacement.

The scheduling-stall case holds the second request for one timer-flushed CRI
partial at the backend response boundary, then uses SIGSTOP/SIGCONT to expire
the active retry. The response stays held until the child cancels it. This
synchronizes request accounting before the stall; a later backend observation
alone does not establish when the child sent a request. The test requires exactly
one expired call, rejection of every remaining partial, and no additional requests.

Build and run the separate race-enabled fault agent:

```sh
go run ./test/logsagent/cmd/faultagent -output /tmp/otel-agent-faults
OTEL_AGENT_FAULT_BINARY=/tmp/otel-agent-faults \
  go test -race -count=1 -timeout=180s -run '^TestInjected' \
  ./test/logsagent -require-fault-agent
```

Exact-match source overlays inject admission saturation and delayed completion.
The former forces concurrent calls beyond ordinary Filelog concurrency; the latter
holds completion after backend acceptance while real deadlines and shutdown run.
Final drain fails if an instrumented call remains active. The normal binary and
repository sources are unchanged. `-build-dir` can reuse an OCB-generated directory.

Without a supplied binary, process tests skip; the required flags turn missing
artifacts into failures. CI builds and requires each binary in its respective job.
Real Kubernetes enrichment, host permissions, rollout/rollback and Receiver
storage/API/UI acceptance remain separate checks.
