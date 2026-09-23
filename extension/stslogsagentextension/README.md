# Logs agent extension

`stslogsagent` defaults to fixed Promtail export with health probes and bounded
shutdown. This mode needs no discovery credentials, state files or native pipeline,
and makes no feature queries.

Set `discovery_enabled: true` and configure `native_pipeline` on the route to
enable capability selection. Discovery settings remain on this extension. It
selects one logs destination at startup using the shared
Receiver client's authenticated feature query. A valid `otel-logs: true`
selects OTELNativeMode (OTLP export). A valid response without that capability
or with it set to false selects PromtailMode (Promtail-compatible export).
HTTP 404, malformed responses and temporary failures remaining after the bounded
query budget also select PromtailMode at startup. Authentication, configuration
and unexpected rejection errors fail startup.

Only `otel-logs` requires a boolean value; unrelated capabilities do not affect
selection. Configure the final Receiver URL: redirects are rejected, including
same-host redirects. Receiver proxying uses the explicit `proxy_url` setting.

The selected destination remains fixed. Polling starts after pipeline readiness.
A stable change requests SIGTERM after writing restart intent and the termination
message. Failed writes or signaling reset the observation sequence and impose
the same cooldown. The marker records intent, not successful shutdown. Kubernetes
restarts the container inside the same Pod, preserving the emptyDir checkpoint and
cooldown files. The restarted process queries the Receiver independently. Pod
replacement loses emptyDir state, so checkpoints and cooldown start fresh.
Missing state starts fresh; malformed or unreadable state fails startup.

`/live` stays successful during draining. `/ready` requires initialized pipelines
and no authentication failure or pending shutdown. Valid discovery restores
readiness after an authentication failure without changing the route.

Before receivers start, `NotifyConfig` validates both exporters' effective
retry/timeout bounds, disabled queues and the synchronous pipeline graph. Collector defaulting and
marshaling remove authored-value provenance; configuration fixtures separately
require explicit bounds. Filelog also requires `stanza.synchronousLogEmitter`.

Shutdown sets one absolute drain deadline without canceling admitted exports.
Final reporting combines connector-call outcomes with the selected exporter's
stopped status. It reports export completion only; it does not verify Filelog
checkpoint persistence. Admitted synchronous calls must finish before successful
shutdown. The controller stores no credentials or log payloads in its state.

Restart callbacks run outside the controller lock and polling goroutine. Shutdown
joins polling and intent writes, but cannot join the callback because it may call
Shutdown itself. Shutdown before callback dispatch suppresses the signal; once
dispatched, a callback can finish concurrently with shutdown. A failed callback
resets observations without clearing shutdown or changing the selected route.

Go modes are named `PromtailMode` and `OTELNativeMode`. State, structured mode
fields and telemetry use `promtail` and `native`. Configuration uses
`promtail_pipeline` and `native_pipeline`; fixture IDs are `promtail`/`otel_native`.
