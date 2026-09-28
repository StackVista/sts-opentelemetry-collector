# End-to-end integration tests

Before running the e2e tests, ensure there's a recent build/image of the collector.

```shell
docker build . -t sts-opentelemetry-collector:latest
```

Or, you can invoke the tests with a variable that would build a new image before running the tests.

```shell
REBUILD_COLLECTOR_IMAGE=1 go test -v ./test/e2e/
```

## Packages

- The root package tests topology mapping (collector + Kafka).
- `logsclickhouse` tests OTel logs export to ClickHouse (collector + ClickHouse), using the same pipeline shape as the SUSE Observability chart.

`OTEL_COLLECTOR_IMAGE` and `CLICKHOUSE_IMAGE` override the images used. The default ClickHouse image should match `clickhouse.image.tag` in the suse-observability chart.

```shell
go test -v ./test/e2e/logsclickhouse/
```
