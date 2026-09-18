# OTLP configuration adapter

The HTTP and gRPC factories delegate to the upstream OTLP exporters. They wrap
configuration marshaling so queue and batch units remain visible to extensions
that inspect the effective Collector configuration.

At the pinned upstream version, optional queue serialization produces `{}` for
all sizer values because `SizerType.MarshalText` has a pointer receiver. The logs
controller must distinguish requests from items and bytes to validate its queue
and shutdown bounds. The adapter serializes the actual typed unit as a string.

Runtime configuration, exporter creation, transport, authentication, retries and
queue processing remain upstream. Tests cover both transports, each sizer, all
supported signals, disabled queues, credential masking and preservation of other
configuration fields. Remove the adapter when upstream preserves these units
through effective-config marshaling.
