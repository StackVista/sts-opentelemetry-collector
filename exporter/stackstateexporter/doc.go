//go:generate mdatagen metadata.yaml
//go:generate protoc --proto_path=proto --go_out=. --go_opt=module=github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter proto/trace_payload.proto proto/trace.proto proto/span.proto
package stackstateexporter
