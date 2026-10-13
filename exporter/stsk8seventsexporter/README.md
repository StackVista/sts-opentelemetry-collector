# Cluster-agent-compatible Kubernetes events exporter

`stsk8sevents` sends Kubernetes events to the Receiver intake
(`/stsAgent/intake`) in the format the cluster-agent's `kubernetes_api_events`
check produces, so the Kubernetes StackPack receives its existing event
contract without the cluster-agent. It is a temporary compatibility path.

## How it works

The exporter consumes `k8sobjectsreceiver` watch-mode log records for core
`events` and `pods`, as identified by the `k8s.resource.name` attribute.

- Every added or modified Event is mapped one to one, as the cluster agent does.
- Every added or modified Pod yields one `OOMKilled` event for each container
  whose current state is terminated with reason `OOMKilled`, reproducing the
  cluster agent's out-of-memory events.
- `DELETED` watch events and other records are ignored.

After mapping, the exporter applies what the agent's sender and aggregator do:
events without a host get `default_host`, and tags are sorted and deduplicated.
Events are buffered and sent every `interval`, grouped under the source type
(`kubernetes` or `openshift`), in requests of at most `max_events_per_request`.

A request that fails with a retryable status (408, 429, 5xx) or a transport
error is attempted up to `max_attempts` times; if it still fails, it and the
requests after it are retried at the next interval. Rejected requests (other
4xx) are dropped. At most `max_buffered_events` are held; the oldest are dropped
first.

The receiver should use leader election (`k8s_leader_elector`) so a single
replica sends events.

```yaml
receivers:
  k8sobjects/kubernetes-events:
    k8s_leader_elector: k8s_leader_elector
    objects:
      - name: events
        mode: watch
        exclude_watch_type: [DELETED]
      - name: pods
        mode: watch
        exclude_watch_type: [DELETED]
```

## Differences from the cluster agent

- **Delivery starts from now.** The agent resumes from a cursor stored in a
  ConfigMap and lists all events and pods on its first run. The exporter sends
  only what it watches, so events emitted while no collector is leader are lost,
  and pods already in an `OOMKilled` state at startup produce no event until
  they change. Delivery guarantees are tracked in
  [stackstate#718](https://github.com/StackVista/stackstate/issues/718).
- **Envelope.** The agent's body carries an empty `apiKey`; the exporter's
  carries empty `topologies`, `health` and `metrics` lists, as the topology
  exporter does. The agent writes an empty `context.data`, which the exporter
  omits, as earlier StackState agent releases did.
- **Default host.** The agent fills an empty host with its own hostname; set
  `default_host` to keep that value.
- **Invalid `event_categories`** fail configuration validation; the agent
  warns and ignores them.

## Configuration

| Key | Default | Description |
|---|---|---|
| `endpoint` | | Receiver intake URL ending in `/stsAgent/intake` |
| `api_key` | | Receiver API key |
| `cluster_name` | | Must match the topology's cluster name |
| `cluster_type` | `kubernetes` | `kubernetes` or `openshift` |
| `internal_hostname` | pod hostname | Producer identity |
| `default_host` | `internal_hostname` | Host for events the agent would assign its own hostname |
| `event_categories` | | Category per event reason: `Alerts`, `Changes`, `Activities` or `Others` |
| `interval` | `15s` | Time between sends |
| `max_events_per_request` | `1000` | Events per request |
| `max_buffered_events` | `10000` | Events held while the intake is unavailable |
| `max_attempts` | `3` | Attempts per request within one send |
| `timeout` | `30s` | Per-request timeout |
| `proxy_url`, `tls.insecure_skip_verify` | | Receiver transport options |

## Vendored cluster-agent code

`internal/eventmapper` is copied from
[stackstate-agent](https://github.com/StackVista/stackstate-agent)
`pkg/collector/corechecks/cluster/kubeapi` (`kubernetes_event_mapper.go` and the
OOM synthesis in `kubernetes_events_pods.go`) at `13679eda6f`, and
`internal/urn` and `internal/hostname` from the topology exporter (Apache
License 2.0). They are excluded from linting. Changes from the original:

- The receiver client's `telemetry.Event` replaces the agent event type.
- The configured cluster type replaces OpenShift API detection.
- zap replaces the agent logger; unknown involved-object kinds log at debug.

`testdata/golden.json` holds the agent's intake bodies for
`testdata/objects.json`, produced by the agent's mapper, sender, aggregator and
serializer. Regenerate it with
`testdata/agentgolden/generate.sh <stackstate-agent checkout>`;
`TestEventsMatchClusterAgent` requires the exporter to send the same events.
