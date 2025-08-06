# Local development

## Install dependencies
### Protobuf generator
```shell
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

## Generate 
To generate a `settings` model from OpenAPI definition, execute following command:
```shell
go generate ./generated/settings/model.go
```

To generate a `topo_stream` model from Protobuf schema execute following command:
```shell
go generate ./generated/topostream/model.go
```

## Build collector locally
```shell
docker build . -t sts-opentelemetry-collector:latest
```

## Run it locally 
Create a file (`dev-config.yaml`) with configuration for OpenTelemetry Collector.
```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318
processors:
  batch:

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
```

```shell
docker run --rm -p 4317:4317 -p 4318:4318  -v ./dev-config.yaml:/config.yaml --network="host" sts-opentelemetry-collector:latest  --config /config.yaml
```

## Generate traces
```shell
docker run --rm -it --name otel-generator --network host golang /bin/bash
```
Run inside `golang` container
```shell
go install github.com/open-telemetry/opentelemetry-collector-contrib/tracegen@latest
tracegen --otlp-endpoint localhost:4317 --otlp-insecure --traces 1
```