# STS Kafka Exporter

## What 
The STS Kafka Exporter allows explicit control over the message key, giving us safety and determinism when exporting OpenTelemetry topology information (components and relations).

## Why
This exporter was created because the existing [OpenTelemetry Kafka Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/kafkaexporter)
does not provide a configurable Kafka message key. For example, when exporting logs, the default exporter (as of 09/2025) computes the key as a hash of all resource attributes attached to the log.
Using a hash of all resource attributes, as in the default exporter can lead to unpredictable keys if a resource attribute is added or removed — potentially breaking message partitioning and order. 

However, if [otel-collector issue#12795](https://github.com/open-telemetry/opentelemetry-collector/issues/12795), [otel-collector-contrib issue#39199](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/39199) and [otel-collector-contrib issue#40149](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/40149) are actioned, it would provide this functionality — after which this custom Kafka exporter would 
become redundant and using the upstream Kafka exporter would be preferred.

Interestingly, this capability was nearly made available in [PR#31315](https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/31315#discussion_r1536742172), 
but was overruled in a later version of the PR. In any case, the code owners have earmarked `partition_traces_by_id`, `partition_metrics_by_resource_attributes`, and `partition_logs_by_resource_attributes`
for deprecation [here](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/38484#issuecomment-2756741755) in favour of a more flexible partition context using OTTL (transform language).

Since there's no clear timeline on the above manifesting, we have a custom Kafka exporter. 
