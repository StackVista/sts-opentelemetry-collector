# Logs capability extension

`stslogscapability` selects one logs destination at startup using the shared
Receiver client's authenticated feature query. A valid `otel-logs: true`
selects native export; absence, unsupported discovery and exhausted transient
discovery failures select legacy export. Authentication, configuration and
unexpected rejection errors fail startup.

The selected destination remains fixed. Polling starts after pipeline readiness.
A stable change requests SIGTERM after writing restart intent and the termination
message. Failed writes or signaling reset the observation sequence and impose
the same cooldown. The marker records intent, not successful shutdown; replacement
startup queries the Receiver independently and retains the cooldown timestamp.
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
