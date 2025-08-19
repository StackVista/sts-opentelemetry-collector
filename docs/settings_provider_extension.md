# Settings provider extension
Provides the capability to retrieve internal SUSE Observability/Stackstate settings to components.

The settings provider can use either a file or Kafka topic as a source for the settings (based on the protocol defined in
[stackstate-openapi](https://gitlab.com/stackvista/platform/stackstate-openapi/-/blob/master/spec_settings/openapi.yaml?ref_type=heads)).

## Run locally

What the following setup achieves is the following:
- OTel Collector with Extension consuming internal settings message from Kafka

Follow the [install dependencies](../README.md#install-dependencies) section in the main README.

The commands below are assuming they'll be run from the root dir of the project.

### Run a Kafka broker locally (for the kafka-based settings provider)

The following Compose app will also take care of creating a Kafka topic called `sts-internal-settings`.

```shell
docker-compose -f docker/kafka/docker-compose.yaml up -d
```

To stop the Compose app when you're done:
```shell
docker-compose -f docker/kafka/docker-compose down
```

### Run the collector using config with extension enabled

Assuming the dependencies are generated and the OTel collector image built, add to your dev-config.yaml (see the main README), the following segments:
```yaml
extensions:
  sts_settings_provider:
   kafka:
     brokers: ["localhost:9092"]
     topic: "sts-internal-settings"
   # Or file-based
#   file:
#     path: "/settings.yaml"
#     update_interval: 30s

service:
  # for debug logs enable
#  telemetry:
#    logs:
#      level: debug
  extensions: [ sts_settings_provider ]
```

and run the OTEL collector:

**with Kafka-based settings provider**
```shell
docker run --rm \
  -p 4317:4317 -p 4318:4318 \
  -v ./dev-config.yaml:/config.yaml \
  --network="host" sts-opentelemetry-collector:latest --config /config.yaml
```

**with file-based settings provider**
```shell
docker run --rm \
  -p 4317:4317 -p 4318:4318 \
  -v ./dev-config.yaml:/config.yaml \
  -v ./extension/settingsproviderextension/provider/file/testdata/settings.yaml:/settings.yaml \
  --network="host" \
  sts-opentelemetry-collector:latest --config /config.yaml
```

You should see a log line like:

```shell
2025-08-16T12:56:33.724Z        info    extensions/extensions.go:37     Extension is starting...        {"kind": "extension", "name": "sts_settings_provider"}
```

### Send messages to the settings Kafka topic

There's a Go app in `extension/settingsproviderextension/cmd/publish-settings` with which you can send messages to the 
settings Kafka topic to (manually) test the flow (of setting snapshot consumption in the extension).

The app takes one parameter, i.e. the address of the Kafka broker. 

```shell
go run ./extension/settingsproviderextension/cmd/publish-snapshot/main.go localhost:9092
```

In the terminal tab where the OTel collector container is running, you should see in the logs that the settings messages
have been processed and added to the internal state of the Kafka settings provider. Along the lines of:
```shell
2025-08-16T12:57:15.264Z        info    kafka/kafka_settings_provider.go:253    Received snapshot start.        {"kind": "extension", "name": "sts_settings_provider", "snapshotId": "36b6a4e1-1a44-42da-ac72-bd17cefbd4ee", "settingType": "OtelComponentMapping"}
2025-08-16T12:57:15.264Z        info    kafka/kafka_settings_provider.go:298    Received snapshot stop. Processing complete snapshot.   {"kind": "extension", "name": "sts_settings_provider", "snapshotId": "36b6a4e1-1a44-42da-ac72-bd17cefbd4ee", "settingType": "OtelComponentMapping", "settingCount": 1}
```
