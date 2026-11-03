CREATE TABLE IF NOT EXISTS `otel`.`otel_logs_v1` UUID '41580483-2caf-333f-95de-2c82966bd374'
(
    SourceTimestamp DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    ObservedTimestamp DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    Timestamp DateTime64(9, 'UTC') MATERIALIZED if(SourceTimestamp != toDateTime64(0, 9), SourceTimestamp, ObservedTimestamp) CODEC(Delta, ZSTD(1)),
    TimestampSource Enum8('missing' = 0, 'source' = 1, 'observed' = 2) MATERIALIZED multiIf(SourceTimestamp != toDateTime64(0, 9), 1, ObservedTimestamp != toDateTime64(0, 9), 2, 0),
    IngestedAt DateTime64(9, 'UTC') DEFAULT now64(9) CODEC(Delta, ZSTD(1)),
    ResourceRef UUID CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeStrings Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeInts Map(LowCardinality(String), Int64) CODEC(ZSTD(1)),
    ScopeDoubles Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    ScopeBools Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),
    ScopeOther Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    TraceId FixedString(16) CODEC(ZSTD(1)),
    SpanId FixedString(8) CODEC(ZSTD(1)),
    TraceFlags UInt8 CODEC(ZSTD(1)),
    SeverityText LowCardinality(String) CODEC(ZSTD(1)),
    SeverityNumber UInt8 CODEC(ZSTD(1)),
    EventName LowCardinality(String) CODEC(ZSTD(1)),
    Body String CODEC(ZSTD(1)),
    BodyOther String CODEC(ZSTD(1)),
    Strings Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    Ints Map(LowCardinality(String), Int64) CODEC(ZSTD(1)),
    Doubles Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    Bools Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),
    Other Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    AuthScope Array(LowCardinality(String)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_strings_key mapKeys(Strings) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_strings_value mapValues(Strings) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_ints_key mapKeys(Ints) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_ints_value mapValues(Ints) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_severity_number SeverityNumber TYPE minmax GRANULARITY 1,
    INDEX idx_body lowerUTF8(Body) TYPE ngrambf_v1(3, 65536, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(IngestedAt)
ORDER BY (toStartOfFifteenMinutes(Timestamp), ResourceRef, Timestamp)
TTL toDateTime(IngestedAt) + toIntervalDay(7)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS `otel`.`otel_logs_resources_v1` UUID '2067701b-8063-3b0e-a69e-4b3c0831b779'
(
    `Timestamp` DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    `ResourceRef` UUID CODEC(ZSTD(1)),
    `Strings` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `Ints` Map(LowCardinality(String), Int64) CODEC(ZSTD(1)),
    `Doubles` Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    `Bools` Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),
    `Other` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `AuthScope` Array(LowCardinality(String)),
    INDEX idx_res_attr_key mapKeys(`Strings`) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(`Strings`) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_int_key mapKeys(`Ints`) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_int_value mapValues(`Ints`) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(`Timestamp`)
ORDER BY (`ResourceRef`, arraySort(arrayDistinct(`AuthScope`)))
TTL toDateTime(`Timestamp`) + toIntervalDay(9)
SETTINGS index_granularity = 512;

CREATE TABLE IF NOT EXISTS `otel`.`otel_logs_trace_lookup_v1` UUID 'c7d152d0-8a3b-39a2-8cf4-30edd63f449a'
(
    TraceId FixedString(16),
    Bucket DateTime,
    ResourceRef UUID,
    IngestedAt DateTime64(9, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toDate(IngestedAt)
ORDER BY (TraceId, Bucket, ResourceRef)
TTL toDateTime(IngestedAt) + toIntervalDay(7)
SETTINGS index_granularity = 256, ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS `otel`.`otel_logs_trace_lookup_v1_mv` UUID '2908a5f3-8b80-3c24-8988-a9212757702f' TO `otel`.`otel_logs_trace_lookup_v1` AS
SELECT TraceId, Bucket, ResourceRef, max(IngestedAt) AS IngestedAt
FROM
(
    SELECT TraceId, toStartOfFifteenMinutes(Timestamp) AS Bucket, ResourceRef, IngestedAt, toDate(IngestedAt) AS IngestDay
    FROM `otel`.`otel_logs_v1`
    WHERE TraceId != toFixedString('', 16) AND TimestampSource != 'missing'
)
GROUP BY TraceId, Bucket, ResourceRef, IngestDay;

CREATE TABLE IF NOT EXISTS `otel`.`otel_logs_field_values_v1` UUID '7635a9e7-470a-31de-9bc1-3a67282ef3dd'
(
    FieldType LowCardinality(String),
    FieldName String,
    Value String,
    AuthScope Array(LowCardinality(String)),
    Timestamp DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1))
)
ENGINE = ReplacingMergeTree(Timestamp)
ORDER BY (FieldType, FieldName, Value, arraySort(arrayDistinct(AuthScope)))
TTL toDateTime(Timestamp) + toIntervalDay(7)
SETTINGS index_granularity = 2048;

CREATE MATERIALIZED VIEW IF NOT EXISTS `otel`.`otel_logs_field_values_v1_resource_attr_mv` UUID '0c63a7b2-eaa8-3520-b72d-9fedb39bf16d' TO `otel`.`otel_logs_field_values_v1` AS
SELECT DISTINCT 'resource_attribute' AS FieldType, kv.1 AS FieldName, kv.2 AS Value, AuthScope, now64(9) AS Timestamp
FROM `otel`.`otel_logs_resources_v1`
ARRAY JOIN arrayConcat(
    CAST(Strings, 'Array(Tuple(String, String))'),
    arrayMap(k -> (k, ''), arrayConcat(mapKeys(Ints), mapKeys(Doubles), mapKeys(Bools), mapKeys(Other)))
) AS kv;

CREATE MATERIALIZED VIEW IF NOT EXISTS `otel`.`otel_logs_field_values_v1_log_attr_mv` UUID '99417a5e-f814-3cb3-afc7-efbd87895d67' TO `otel`.`otel_logs_field_values_v1` AS
SELECT DISTINCT 'log_attribute' AS FieldType, kv.1 AS FieldName, kv.2 AS Value, AuthScope, now64(9) AS Timestamp
FROM `otel`.`otel_logs_v1`
ARRAY JOIN arrayConcat(
    CAST(Strings, 'Array(Tuple(String, String))'),
    arrayMap(k -> (k, ''), arrayConcat(mapKeys(Ints), mapKeys(Doubles), mapKeys(Bools), mapKeys(Other)))
) AS kv;

CREATE MATERIALIZED VIEW IF NOT EXISTS `otel`.`otel_logs_field_values_v1_scope_attr_mv` UUID '20618c51-18f2-303b-8ad7-03003912a7d6' TO `otel`.`otel_logs_field_values_v1` AS
SELECT DISTINCT 'scope_attribute' AS FieldType, kv.1 AS FieldName, kv.2 AS Value, AuthScope, now64(9) AS Timestamp
FROM `otel`.`otel_logs_v1`
ARRAY JOIN arrayConcat(
    CAST(ScopeStrings, 'Array(Tuple(String, String))'),
    arrayMap(k -> (k, ''), arrayConcat(mapKeys(ScopeInts), mapKeys(ScopeDoubles), mapKeys(ScopeBools), mapKeys(ScopeOther)))
) AS kv;

CREATE MATERIALIZED VIEW IF NOT EXISTS `otel`.`otel_logs_field_values_v1_fields_mv` UUID '863f779c-02f7-363e-b221-2a37481cadba' TO `otel`.`otel_logs_field_values_v1` AS
SELECT DISTINCT 'field' AS FieldType, kv.1 AS FieldName, kv.2 AS Value, AuthScope, now64(9) AS Timestamp
FROM `otel`.`otel_logs_v1`
ARRAY JOIN [('service.name', ServiceName), ('scope.name', ScopeName), ('severity_text', toString(SeverityText)), ('event_name', toString(EventName))] AS kv
WHERE kv.2 != '';
