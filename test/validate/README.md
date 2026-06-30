# Agent collector image validation fixtures

These configs are used by CI (`.github/workflows/build.yaml`, the `validate` job) to verify
that the strict agent collector image (built from [`agent-otel-builder.yaml`](../../agent-otel-builder.yaml))
actually contains every component the agent collectors need. Each file is run through the
collector's built-in `validate` subcommand:

```shell
docker run --rm -v "$PWD/test/validate/configs:/cfg" <agent-image> \
  validate --config /cfg/scraper.yaml
```

A missing or misnamed component in the BOM makes `validate` fail, breaking the build.

## Files

- `configs/scraper.yaml` — de-templated copy of the Prometheus-scraper collector config from
  `helm-charts-internal/stable/suse-observability-agent/templates/otel/scraper/configmap.yaml`
  (mTLS branch enabled to cover the superset of components).
- `configs/k8s-resource.yaml` — de-templated copy of the k8s-resource collector config from
  `helm-charts-internal/stable/suse-observability-agent/templates/k8s-resource-collector-configmap.yaml`
  (leader election enabled; uses the `otlphttp` exporter).
- `configs/coverage.yaml` — synthetic config wiring the BOM components no agent config uses yet
  (`spanmetrics` connector, `debug` exporter) plus the base `otlp` receiver.

## Keeping these in sync

`scraper.yaml` and `k8s-resource.yaml` are copies of the Helm-rendered configs with templating,
`${env:...}` references and the dynamically-chosen exporter name replaced by literals. When the
chart's collector configs change in a way that adds or removes a component, update these copies
so CI keeps validating the real shape of the configs.
