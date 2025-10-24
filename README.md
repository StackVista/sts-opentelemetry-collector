# Local development

## Install dependencies
### Protobuf generator
```shell
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

## Update spec versions and code generation
The project uses Protobuf and OpenAPI schema to generate go lang models. We have scripts to download the latest versions of the schemas and update the models.

### Protobuf
1. To update protobuf version, you have to past git hash to `connector/topologyconnector/topostream_version`
2. Run script `connector/topologyconnector/scripts/generate_protobuf_model.sh`

The script will download the latest version of Protobuf schema to `connector/topologyconnector/spec` and generate a `topo_stream` model.
Optionally you can run the following command to generate a `topo_stream` model from Protobuf schema.
```shell
go generate ./connector/topologyconnector/generated/topostream/model.go
```

**NOTE:** The generated files have headers with protoc version. Your version may be different than used by github actions. It is recommended to update the version manually.

### OpenAPI
1. To update OpenAPI version, you have to past git hash to `extension/settingsproviderextension/settings_version`
2. Run script `extension/settingsproviderextension/scripts/generate_openapi_model.sh`

The script will download the latest version of OpenAPI schema to `extension/settingsproviderextension/spec` and generate a `settings` model.
Optionally you can run the following command to generate a `settings` model from OpenAPI schema.
```shell
go generate ./extension/settingsproviderextension/generated/settings/model.go
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
      receivers: [ otlp ]
      processors: [ batch ]
      exporters: [ debug ]
    metrics:
      receivers: [ otlp ]
      processors: [ batch ]
      exporters: [ debug ]
    logs:
      receivers: [ otlp ]
      processors: [ batch ]
      exporters: [ debug ]
```
### Run with above config (and no extension config)
```shell
docker run --rm -p 4317:4317 -p 4318:4318  -v ./dev-config.yaml:/config.yaml --network="host" sts-opentelemetry-collector:latest  --config /config.yaml
```

To run with an extension enabled, see as an example the guide for the [STS Settings Provider](extension/settingsproviderextension/example/settings_provider_extension.md) extension.

## Generate traces
```shell
docker run --rm -it --name otel-generator --network host golang /bin/bash
```
Run inside `golang` container
```shell
go install github.com/open-telemetry/opentelemetry-collector-contrib/tracegen@latest
tracegen --otlp-endpoint localhost:4317 --otlp-insecure --traces 1
```

## Upgrade Go Modules

To upgrade the Go version and dependencies across all modules in the workspace, follow these steps:

1.  **Upgrade Go Version:**
    Run the `upgrade_go_version.sh` script with the desired new Go version as an argument.
    ```shell
    ./scripts/upgrade_go_version.sh <new_go_version>
    ```
    Replace `<new_go_version>` with the target Go version (e.g., `1.21`).

2.  **Upgrade Dependencies and Synchronize:**
    Run the `upgrade_dependencies.sh` script to update all module dependencies and synchronize the `go.work` file.
    ```shell
    ./scripts/upgrade_dependencies.sh
    ```

These scripts will iterate through all `go.mod` files, update them, and ensure consistency across the workspace.