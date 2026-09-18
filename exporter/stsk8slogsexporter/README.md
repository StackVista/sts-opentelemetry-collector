# Legacy Kubernetes logs exporter

Encodes OTel pod logs for the Receiver's existing `/stsAgent/logs/k8s`
endpoint. The agent distribution registers this exporter as `stsk8slogs`.
The [configuration fixture](../../test/validate/configs/legacy-logs-exporter.yaml)
shows the endpoint, authentication and bounded queue/retry settings.

The configured cluster name must be nonempty UTF-8 and must not contain double
quotes, backslashes or non-printable characters. The Receiver's label parser
removes quotes without unescaping their contents. Unsupported names fail
encoder construction rather than changing the cluster identity.

Records are grouped by pod UUID, pod name, container and optional
`log.iostream`. Resources asserting a different cluster are rejected. Invalid
records are counted by reason while valid siblings are retained. The encoder
preserves application text and nanosecond timestamps and does not mutate input.
It emits protobuf with Snappy block compression.

The factory uses the shared Receiver client for URL validation, explicit proxy
configuration and TLS trust. `tls.ca_file` adds PEM certificates to system roots;
`tls.insecure_skip_verify` explicitly disables certificate verification.
`proxy_url` selects an HTTP(S) proxy; an empty value means a direct connection.
It sends `sts-api-key`
in a header, disables redirects, and returns safe errors for exporter-helper
to retry or reject. HTTP success does not reveal limiter omissions. A lost
response or a partial backend write followed by failure can produce duplicates
when retried.

Invalid records are removed before entering exporter-helper, and counted once
by `otelcol_stsk8slogs_invalid_log_records` with bounded `reason` attributes.
The upstream exporter sent/failed counters count valid records only. The memory
queue waits for export results and blocks when full. Persistent queues, queue
batching, disabled retries and unlimited retry durations are rejected.
The sender bounds each HTTP attempt; the caller owns the overall export lifetime.

The Receiver client is pinned to a Go pseudo-version of its reviewed commit.
No library tag or separate release is required.

The schema descriptor in `testdata/` mirrors `promtail.proto` in
`StackVista/stackstate/stackstate-receiver-project/stackstate-receiver/src/main/protobuf/`.
The text fixture is decoded using that schema independently of the encoder.
Tests also exercise exporter-helper retry handling against the sender.

Run `go test -race ./...` from this directory using the repository Go toolchain.
