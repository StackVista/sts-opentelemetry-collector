package internal

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"time"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// ShardCount is the number of shards to use for the topology stream.
const ShardCount = 4

type MessageWithKey struct {
	Key     *topostreamv1.TopologyStreamMessageKey
	Message *topostreamv1.TopologyStreamMessage
}

func ConvertSpanToTopologyStreamMessage(
	ctx context.Context,
	logger *zap.Logger,
	eval ExpressionEvaluator,
	mapper *Mapper,
	trace ptrace.Traces,
	componentMappings []settings.OtelComponentMapping,
	relationMappings []settings.OtelRelationMapping,
	collectionTimestampMs int64,
	metricsRecorder metrics.ConnectorMetricsRecorder,
) []MessageWithKey {
	result := make([]MessageWithKey, 0)

	iterateSpans(trace, func(expressionEvalContext *ExpressionEvalContext, timestamp pcommon.Timestamp) {
		result = mapComponents(ctx, logger, eval, mapper, componentMappings, collectionTimestampMs,
			metricsRecorder, expressionEvalContext, timestamp, settings.TRACES, result,
		)
		result = mapRelation(ctx, logger, eval, mapper, relationMappings, collectionTimestampMs,
			metricsRecorder, expressionEvalContext, timestamp, settings.TRACES, result,
		)
	})

	// Record metrics
	metricsRecorder.IncInputsProcessed(ctx, int64(trace.SpanCount()), settings.TRACES)
	return result
}

func ConvertMetricsToTopologyStreamMessage(
	ctx context.Context,
	logger *zap.Logger,
	eval ExpressionEvaluator,
	mapper *Mapper,
	metricData pmetric.Metrics,
	componentMappings []settings.OtelComponentMapping,
	relationMappings []settings.OtelRelationMapping,
	collectionTimestampMs int64,
	metricsRecorder metrics.ConnectorMetricsRecorder,
) []MessageWithKey {
	result := make([]MessageWithKey, 0)
	metricsDatapointCount := int64(0)

	iterateMetrics(metricData, func(expressionEvalContext *ExpressionEvalContext, timestamp pcommon.Timestamp) {
		metricsDatapointCount++
		result = mapComponents(ctx, logger, eval, mapper, componentMappings, collectionTimestampMs,
			metricsRecorder, expressionEvalContext, timestamp, settings.METRICS, result,
		)
		result = mapRelation(ctx, logger, eval, mapper, relationMappings, collectionTimestampMs,
			metricsRecorder, expressionEvalContext, timestamp, settings.METRICS, result,
		)
	})

	// Record metrics
	metricsRecorder.IncInputsProcessed(ctx, metricsDatapointCount, settings.METRICS)

	return result
}

func mapComponents(
	ctx context.Context,
	logger *zap.Logger,
	eval ExpressionEvaluator,
	mapper *Mapper,
	componentMappings []settings.OtelComponentMapping,
	collectionTimestampMs int64,
	metricsRecorder metrics.ConnectorMetricsRecorder,
	expressionEvalContext *ExpressionEvalContext,
	timestamp pcommon.Timestamp,
	signal settings.OtelInputSignal,
	// new components and errors are appended to this slice:
	mappingResult []MessageWithKey,
) []MessageWithKey {
	var components, componentErrs int
	var componentMappingStart time.Time
	var componentMappingDuration time.Duration

	for _, componentMapping := range componentMappings {
		componentMappingStart = time.Now()
		currentComponentMapping := componentMapping

		component, errs := convertToComponent(eval, mapper, expressionEvalContext, &currentComponentMapping)
		if component != nil {
			mappingResult = append(mappingResult, *outputToMessageWithKey(
				component,
				componentMapping,
				timestamp,
				collectionTimestampMs,
				func() []*topostreamv1.TopologyStreamComponent {
					return []*topostreamv1.TopologyStreamComponent{component}
				},
				func() []*topostreamv1.TopologyStreamRelation {
					return nil
				}),
			)

			components++
		}
		if errs != nil {
			mappingResult = append(mappingResult,
				*errorsToMessageWithKey(&errs, componentMapping, timestamp, collectionTimestampMs),
			)
			componentErrs++
		}
		componentMappingDuration = time.Since(componentMappingStart)
		metricsRecorder.RecordMappingDuration(
			ctx,
			componentMappingDuration,
			signal,
			settings.SettingTypeOtelComponentMapping,
			componentMapping.Identifier,
		)
	}

	if components > 0 {
		metricsRecorder.IncTopologyProduced(
			ctx, int64(components),
			settings.SettingTypeOtelComponentMapping, signal,
		)
	}
	if componentErrs > 0 {
		metricsRecorder.IncMappingErrors(ctx, int64(componentErrs),
			settings.SettingTypeOtelComponentMapping, signal)
	}
	logger.Debug(
		"Converted metrics to topology stream messages",
		zap.Int("components", components),
		zap.Int("componentErrs", componentErrs),
	)
	return mappingResult
}

func mapRelation(
	ctx context.Context,
	logger *zap.Logger,
	eval ExpressionEvaluator,
	mapper *Mapper,
	relationMappings []settings.OtelRelationMapping,
	collectionTimestampMs int64,
	metricsRecorder metrics.ConnectorMetricsRecorder,
	expressionEvalContext *ExpressionEvalContext,
	timestamp pcommon.Timestamp,
	signal settings.OtelInputSignal,
	// new components and errors are appended to this slice:
	mappingResult []MessageWithKey,
) []MessageWithKey {
	var relations, relationErrs int
	var relationMappingStart time.Time
	var relationMappingDuration time.Duration

	for _, relationMapping := range relationMappings {
		relationMappingStart = time.Now()
		currentRelationMapping := relationMapping

		relation, errs := convertToRelation(eval, mapper, expressionEvalContext, &currentRelationMapping)
		if relation != nil {
			mappingResult = append(mappingResult, *outputToMessageWithKey(
				relation,
				relationMapping,
				timestamp,
				collectionTimestampMs,
				func() []*topostreamv1.TopologyStreamComponent {
					return nil
				},
				func() []*topostreamv1.TopologyStreamRelation {
					return []*topostreamv1.TopologyStreamRelation{relation}
				}),
			)
			relations++
		}
		if errs != nil {
			mappingResult = append(mappingResult,
				*errorsToMessageWithKey(&errs, relationMapping, timestamp, collectionTimestampMs),
			)
			relationErrs++
		}
		relationMappingDuration = time.Since(relationMappingStart)
		metricsRecorder.RecordMappingDuration(
			ctx,
			relationMappingDuration,
			signal,
			settings.SettingTypeOtelRelationMapping,
			relationMapping.Identifier,
		)
	}

	if relations > 0 {
		metricsRecorder.IncTopologyProduced(
			ctx, int64(relations),
			settings.SettingTypeOtelRelationMapping, signal,
		)
	}

	if relationErrs > 0 {
		metricsRecorder.IncMappingErrors(ctx, int64(relationErrs), settings.SettingTypeOtelRelationMapping, signal)
	}

	logger.Debug(
		"Converted metrics to topology stream messages",
		zap.Int("relations", relations),
		zap.Int("relationErrors", relationErrs),
	)

	return mappingResult
}

func ConvertMappingRemovalsToTopologyStreamMessage(
	ctx context.Context,
	logger *zap.Logger,
	componentMappings []settings.OtelComponentMapping,
	relationMappings []settings.OtelRelationMapping,
	metricsRecorder metrics.ConnectorMetricsRecorder,
) []MessageWithKey {
	result := make([]MessageWithKey, 0)
	var componentMappingsRemoved, relationMappingsRemoved int

	for _, componentMapping := range componentMappings {
		messages := removalToMessageWithKey(componentMapping)
		result = append(result, messages...)
		componentMappingsRemoved++
	}

	for _, relationMapping := range relationMappings {
		messages := removalToMessageWithKey(relationMapping)
		result = append(result, messages...)
		relationMappingsRemoved++
	}

	if componentMappingsRemoved > 0 {
		metricsRecorder.IncMappingsRemoved(ctx, int64(componentMappingsRemoved),
			settings.SettingTypeOtelComponentMapping)
	}
	if relationMappingsRemoved > 0 {
		metricsRecorder.IncMappingsRemoved(ctx, int64(relationMappingsRemoved),
			settings.SettingTypeOtelRelationMapping)
	}

	logger.Debug(
		"Converted mapping removals to topology stream messages",
		zap.Int("componentMappingsRemoved", componentMappingsRemoved),
		zap.Int("relationMappingsRemoved", relationMappingsRemoved),
	)

	return result
}

func convertToComponent(
	expressionEvaluator ExpressionEvaluator,
	mapper *Mapper,
	evalContext *ExpressionEvalContext,
	mapping *settings.OtelComponentMapping,
) (*topostreamv1.TopologyStreamComponent, []error) {
	evaluatedVars, errs := EvalVariables(expressionEvaluator, evalContext, mapping.Vars)
	if errs != nil {
		return nil, errs
	}

	evalContextWithVars := evalContext.CloneWithVariables(evaluatedVars)

	if filterByConditions(expressionEvaluator, evalContextWithVars, &mapping.Conditions) {
		component, err := mapper.MapComponent(mapping, expressionEvaluator, evalContextWithVars)
		if len(err) > 0 {
			return nil, err
		}
		return component, nil
	}
	return nil, nil
}

func convertToRelation(
	expressionEvaluator ExpressionEvaluator,
	mapper *Mapper,
	evalContext *ExpressionEvalContext,
	mapping *settings.OtelRelationMapping,
) (*topostreamv1.TopologyStreamRelation, []error) {
	evaluatedVars, errs := EvalVariables(expressionEvaluator, evalContext, mapping.Vars)
	if errs != nil {
		return nil, errs
	}
	evalContextWithVars := evalContext.CloneWithVariables(evaluatedVars)

	if filterByConditions(expressionEvaluator, evalContextWithVars, &mapping.Conditions) {
		relation, err := mapper.MapRelation(mapping, expressionEvaluator, evalContextWithVars)
		if len(err) > 0 {
			return nil, err
		}
		return relation, nil
	}
	return nil, nil
}

type mappingHandler func(expressionEvalContext *ExpressionEvalContext, timestamp pcommon.Timestamp)

func iterateSpans(trace ptrace.Traces, handler mappingHandler) {
	resourceSpans := trace.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		resourceAttributes := rs.Resource().Attributes().AsRaw()
		scopeSpans := rs.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			ss := scopeSpans.At(j)
			scopeAttributes := ss.Scope().Attributes().AsRaw()
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				spanAttributes := span.Attributes().AsRaw()
				handler(NewSpanEvalContext(spanAttributes, scopeAttributes, resourceAttributes), span.EndTimestamp())
			}
		}
	}
}

func iterateMetrics(metrics pmetric.Metrics, handler mappingHandler) {
	resourceMetrics := metrics.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		rs := resourceMetrics.At(i)
		resourceAttributes := rs.Resource().Attributes().AsRaw()
		scopeMetrics := rs.ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			sm := scopeMetrics.At(j)
			scopeAttributes := sm.Scope().Attributes().AsRaw()
			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)

				switch metric.Type() {
				case pmetric.MetricTypeEmpty:
					continue
				case pmetric.MetricTypeGauge:
					for l := metric.Gauge().DataPoints().Len() - 1; l >= 0; l-- {
						datapoint := metric.Gauge().DataPoints().At(l)
						handler(NewMetricEvalContext(datapoint.Attributes().AsRaw(), scopeAttributes, resourceAttributes),
							datapoint.Timestamp())
					}
				case pmetric.MetricTypeSum:
					for l := metric.Sum().DataPoints().Len() - 1; l >= 0; l-- {
						datapoint := metric.Sum().DataPoints().At(l)
						handler(NewMetricEvalContext(datapoint.Attributes().AsRaw(), scopeAttributes, resourceAttributes),
							datapoint.Timestamp())
					}
				case pmetric.MetricTypeHistogram:
					for l := metric.Histogram().DataPoints().Len() - 1; l >= 0; l-- {
						datapoint := metric.Histogram().DataPoints().At(l)
						handler(NewMetricEvalContext(datapoint.Attributes().AsRaw(), scopeAttributes, resourceAttributes),
							datapoint.Timestamp())
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := metric.ExponentialHistogram().DataPoints().Len() - 1; l >= 0; l-- {
						datapoint := metric.ExponentialHistogram().DataPoints().At(l)
						handler(NewMetricEvalContext(datapoint.Attributes().AsRaw(), scopeAttributes, resourceAttributes),
							datapoint.Timestamp())
					}
				case pmetric.MetricTypeSummary:
					for l := metric.Summary().DataPoints().Len() - 1; l >= 0; l-- {
						datapoint := metric.Summary().DataPoints().At(l)
						handler(NewMetricEvalContext(datapoint.Attributes().AsRaw(), scopeAttributes, resourceAttributes),
							datapoint.Timestamp())
					}
				}
			}
		}
	}
}

func errorsToMessageWithKey(
	errs *[]error,
	mapping settings.SettingExtension,
	timestamp pcommon.Timestamp,
	collectionTimestampMs int64,
) *MessageWithKey {
	streamErrors := make([]*topostreamv1.TopoStreamError, len(*errs))
	for i, err := range *errs {
		streamErrors[i] = &topostreamv1.TopoStreamError{
			Message: err.Error(),
		}
	}
	return &MessageWithKey{
		Key: &topostreamv1.TopologyStreamMessageKey{
			Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: mapping.GetIdentifier(),
			ShardId:    "unknown",
		},
		Message: &topostreamv1.TopologyStreamMessage{
			CollectionTimestamp: collectionTimestampMs,
			SubmittedTimestamp:  convertTimestampToInt64(timestamp),
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
					ExpiryIntervalMs: mapping.GetExpireAfterMs(),
					Errors:           streamErrors,
				},
			},
		},
	}
}

func outputToMessageWithKey(
	output topostreamv1.ComponentOrRelation,
	mapping settings.SettingExtension,
	timestamp pcommon.Timestamp,
	collectionTimestampMs int64,
	toComponents func() []*topostreamv1.TopologyStreamComponent,
	toRelations func() []*topostreamv1.TopologyStreamRelation,
) *MessageWithKey {
	submittedTimestamp := convertTimestampToInt64(timestamp)
	return &MessageWithKey{
		Key: &topostreamv1.TopologyStreamMessageKey{
			Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
			DataSource: mapping.GetIdentifier(),
			ShardId:    stableShardID(output.GetExternalId(), ShardCount),
		},
		Message: &topostreamv1.TopologyStreamMessage{
			CollectionTimestamp: collectionTimestampMs,
			SubmittedTimestamp:  submittedTimestamp,
			Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData{
				TopologyStreamRepeatElementsData: &topostreamv1.TopologyStreamRepeatElementsData{
					ExpiryIntervalMs: mapping.GetExpireAfterMs(),
					Components:       toComponents(),
					Relations:        toRelations(),
				},
			},
		},
	}
}

func removalToMessageWithKey(
	mapping settings.SettingExtension,
) []MessageWithKey {
	messages := make([]MessageWithKey, 0)
	now := time.Now().UnixMilli()
	for shard := 0; shard < ShardCount; shard++ {
		message := &MessageWithKey{
			Key: &topostreamv1.TopologyStreamMessageKey{
				Owner:      topostreamv1.TopologyStreamOwner_TOPOLOGY_STREAM_OWNER_OTEL,
				DataSource: mapping.GetIdentifier(),
				ShardId:    fmt.Sprintf("%d", shard),
			},
			Message: &topostreamv1.TopologyStreamMessage{
				CollectionTimestamp: now,
				SubmittedTimestamp:  now,
				Payload: &topostreamv1.TopologyStreamMessage_TopologyStreamRemove{
					TopologyStreamRemove: &topostreamv1.TopologyStreamRemove{
						RemovalCause: fmt.Sprintf("Setting with identifier '%s' was removed'", mapping.GetIdentifier()),
					},
				},
			},
		}
		messages = append(messages, *message)
	}
	return messages
}

func stableShardID(shardKey string, shardCount uint32) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(shardKey))
	return fmt.Sprintf("%d", h.Sum32()%shardCount)
}

func convertTimestampToInt64(input pcommon.Timestamp) int64 {
	actualU64 := uint64(input)
	if actualU64 > math.MaxInt64 {
		return 0
	}
	return int64(actualU64)
}
