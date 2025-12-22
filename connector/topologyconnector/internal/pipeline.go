package internal

import (
	"context"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func ConvertSpanToTopologyStreamMessage(
	ctx context.Context,
	logger *zap.Logger,
	eval ExpressionEvaluator,
	deduplicator Deduplicator,
	mapper *Mapper,
	traceData ptrace.Traces,
	componentMappings []settings.OtelComponentMapping,
	relationMappings []settings.OtelRelationMapping,
	collectionTimestampMs int64,
	metricsRecorder metrics.ConnectorMetricsRecorder,
) []MessageWithKey {
	traceTraverser := NewTracesTraverser(traceData)

	return convertSignalDataToTopologyStreamMessage(
		ctx,
		logger,
		traceTraverser,
		settings.TRACES,
		eval,
		deduplicator,
		mapper,
		componentMappings,
		relationMappings,
		collectionTimestampMs,
		metricsRecorder,
	)
}

func ConvertMetricsToTopologyStreamMessage(
	ctx context.Context,
	logger *zap.Logger,
	eval ExpressionEvaluator,
	deduplicator Deduplicator,
	mapper *Mapper,
	metricData pmetric.Metrics,
	componentMappings []settings.OtelComponentMapping,
	relationMappings []settings.OtelRelationMapping,
	collectionTimestampMs int64,
	metricsRecorder metrics.ConnectorMetricsRecorder,
) []MessageWithKey {
	metricsTraverser := NewMetricsTraverser(metricData)

	return convertSignalDataToTopologyStreamMessage(
		ctx,
		logger,
		metricsTraverser,
		settings.METRICS,
		eval,
		deduplicator,
		mapper,
		componentMappings,
		relationMappings,
		collectionTimestampMs,
		metricsRecorder,
	)
}

func convertSignalDataToTopologyStreamMessage(
	ctx context.Context,
	logger *zap.Logger,
	traverser SignalTraverser,
	signal settings.OtelInputSignal,
	eval ExpressionEvaluator,
	deduplicator Deduplicator,
	mapper *Mapper,
	componentMappings []settings.OtelComponentMapping,
	relationMappings []settings.OtelRelationMapping,
	collectionTimestampMs int64,
	metricsRecorder metrics.ConnectorMetricsRecorder,
) []MessageWithKey {
	result := make([]MessageWithKey, 0)

	baseCtx := BaseContext{
		Signal:              signal,
		Mapper:              mapper,
		Evaluator:           eval,
		Deduplicator:        deduplicator,
		CollectionTimestamp: collectionTimestampMs,
		MetricsRecorder:     metricsRecorder,
		Results:             &result,
	}

	for _, componentMapping := range componentMappings {
		mappingCtx := &MappingContext[settings.OtelComponentMapping]{
			BaseCtx: baseCtx,
			Mapping: componentMapping,
		}
		v := NewGenericMappingVisitor(mappingCtx)
		traverser.Traverse(ctx, v)
	}

	for _, relationMapping := range relationMappings {
		mappingCtx := &MappingContext[settings.OtelRelationMapping]{
			BaseCtx: baseCtx,
			Mapping: relationMapping,
		}
		v := NewGenericMappingVisitor(mappingCtx)
		traverser.Traverse(ctx, v)
	}

	logResultSummary(logger, result, signal)

	return result
}

func logResultSummary(
	logger *zap.Logger,
	results []MessageWithKey,
	signal settings.OtelInputSignal,
) {
	if !logger.Core().Enabled(zap.DebugLevel) {
		return
	}

	components := 0
	relations := 0
	mappingErrs := 0

	for _, result := range results {
		payload := result.Message.GetPayload()

		repeatData, ok := payload.(*topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData)
		if !ok || repeatData.TopologyStreamRepeatElementsData == nil {
			continue
		}

		data := repeatData.TopologyStreamRepeatElementsData

		components += len(data.Components)
		relations += len(data.Relations)
		mappingErrs += len(data.Errors)
	}

	logger.Debug(
		"Converted signal to topology stream messages",
		zap.String("signal", string(signal)),
		zap.Int("components", components),
		zap.Int("relations", relations),
		zap.Int("mappingErrs", mappingErrs),
	)
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
		messages := RemovalToMessageWithKey(componentMapping)
		result = append(result, messages...)
		componentMappingsRemoved++
	}

	for _, relationMapping := range relationMappings {
		messages := RemovalToMessageWithKey(relationMapping)
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
