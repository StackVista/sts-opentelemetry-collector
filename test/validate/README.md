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

- `configs/promtail-logs-exporter.yaml` — Promtail-compatible Kubernetes logs exporter registration
  and synchronous retry configuration, using a synthetic credential and loopback endpoint.
- `configs/promtail-logs-agent.yaml` — the foundation's fixed Promtail Filelog graph, without discovery.
- `configs/logs-agent.yaml` — Filelog, checkpoint storage, explicit discovery and both export paths.
- `configs/scraper.yaml` — de-templated copy of the Prometheus-scraper collector config from
  `helm-charts-internal/stable/suse-observability-agent/templates/otel/scraper/configmap.yaml`
  (mTLS branch enabled to cover the superset of components).
- `configs/k8s-resource.yaml` — de-templated copy of the k8s-resource collector config from
  `helm-charts-internal/stable/suse-observability-agent/templates/otel/k8sresourcecollector/configmap.yaml`
  (leader election, self-metrics, the experimental Kubernetes topology exporter and the experimental
  Kubernetes state metrics receiver enabled; uses the `otlphttp` exporter).
- `configs/telemetry-gateway.yaml` — de-templated copy of the telemetry-gateway collector config
  from `helm-charts-internal/stable/suse-observability-agent/templates/otel/telemetrygateway/configmap.yaml`.
  This is the fixture that exercises the `span_metrics` connector and the `debug` exporter.
- `configs/node-collector.yaml` — de-templated copy of the node collector config from
  `helm-charts-internal/stable/suse-observability-agent/templates/otel/nodecollector/configmap.yaml`.
  It covers kubeletstats, the kubelet/cAdvisor/probe/SLI Prometheus jobs, hostmetrics, k8sattributes,
  the Agent V2 transforms, cumulative-to-delta and delta-to-rate, the native-series filter, the
  health_check/pprof/bearertokenauth extensions and the self-telemetry Prometheus exporter. Helm
  values are resolved to their defaults; loopback endpoints, a synthetic token, host root path `/`,
  `HOSTNAME` for the node filter and `auth_type: none` for kubeletstats replace the runtime
  environment.

## Node collector pod-spec/state spike (#682)

The pod-spec/state spike chose the existing `stsksm` receiver, exposed through the chart's
`global.features.experimentalOtelKubernetesClusterAgent` path, instead of adding a kubelet
`/pods` receiver or a server-side recording rule. The receiver already provides the
kube-state-metrics data (requests, limits, restarts, waiting/terminated reasons, phase/ready/
scheduled); the chart test in `helm-charts-internal` exercises the rendered `stsksm` config and
its `metrics/kubernetes-state` pipeline.

## Keeping these in sync

These fixtures are copies of the Helm-rendered agent collector configs with templating,
`${env:...}` references and the dynamically-chosen exporter name replaced by literals. When a
chart collector config changes in a way that adds or removes a component, update the matching copy
so CI keeps validating the real shape of the configs.
