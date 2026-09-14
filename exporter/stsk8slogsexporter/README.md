# Legacy Kubernetes log encoding and sending

Encodes OTel pod logs for the Receiver's existing `/stsAgent/logs/k8s`
endpoint. This module currently contains the pure encoder and single-attempt
sender; Collector factory registration and production queue/telemetry wiring
are still pending.

The configured cluster name must be nonempty UTF-8 and must not contain double
quotes, backslashes or non-printable characters. The Receiver's label parser
removes quotes without unescaping their contents. Unsupported names fail
encoder construction rather than changing the cluster identity.

Records are grouped by pod UUID, pod name, container and optional
`log.iostream`. Resources asserting a different cluster are rejected. Invalid
records are counted by reason while valid siblings are retained. The encoder
preserves application text and nanosecond timestamps and does not mutate input.
It emits protobuf with Snappy block compression.

The sender borrows an explicitly configured HTTP transport so discovery and
log export can use the same proxy and certificate trust. It sends `sts-api-key`
in a header, disables redirects, and returns safe errors for exporter-helper
to retry or reject. HTTP success does not reveal limiter omissions. A lost
response or a partial backend write followed by failure can produce duplicates
when retried.

The schema descriptor in `testdata/` mirrors `promtail.proto` in
`StackVista/stackstate/stackstate-receiver-project/stackstate-receiver/src/main/protobuf/`.
The text fixture is decoded using that schema independently of the encoder.
Tests also exercise exporter-helper retry handling against the sender.

Run `go test -race ./...` from this directory using the repository Go toolchain.
