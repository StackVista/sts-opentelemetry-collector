# STS Kafka Exporter

Example configuration:
```yaml
exporters:
  sts_kafka_exporter:
    brokers: [ "localhost:9092" ]
    topic: "sts-otel-topology" 
    read_timeout: 2s
    produce_timeout: 5s
    required_acks: "none"   # allowed: "none", "leader", "all"

    # The configuration below is for the exporterhelper (which wraps the custom kafka exporter):
    
    # Configuration for internal queueing batches before sending to the kafka exporter
    sending_queue:
      enabled: true
      num_consumers: 1
      queue_size: 5000
      
    # Configuration for retrying batches in case of export failure
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 300s

    # The timeout applies to individual attempts to send data to the backend (kafka exporter). Zero means no timeout.
    timeout: 30s

service:
  pipelines:
    logs:
      receivers: [ otlp ]
      processors: [ batch ]
      exporters: [ sts_kafka_exporter ]
```
