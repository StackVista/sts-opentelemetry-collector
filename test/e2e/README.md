# End-to-end integration tests

Before running the e2e tests, ensure there's a recent build/image of the collector.

```shell
docker build . -t sts-opentelemetry-collector:latest
```

Or, you can invoke the tests with a variable that would build a new image before running the tests.

```shell
REBUILD_COLLECTOR_IMAGE=1 go test -v ./test/e2e/
```